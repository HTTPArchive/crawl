#!/usr/bin/env python3
# Copyright 2024 Google Inc.
import datetime
import fcntl
import greenstalk
import json
import logging
import os
import time
from google.cloud import compute_v1

PROJECT = 'httparchive'

class Healthcheck(object):
    def __init__(self):
        self.instances = {}
        self.last_update = None
        self.root_path = os.path.abspath(os.path.dirname(__file__))
        self.instances_file = os.path.join(self.root_path, 'data', 'instances.json')
        if os.path.exists(self.instances_file):
            with open(self.instances_file, 'rt', encoding='utf-8') as f:
                self.instances = json.load(f)

    def update_instances(self):
        """ Update the authoritative list of running instances """
        instance_client = compute_v1.InstancesClient()
        request = compute_v1.AggregatedListInstancesRequest()
        request.project = PROJECT
        request.max_results = 500
        agg_list = instance_client.aggregated_list(request=request)
        instances = {}
        for zone, response in agg_list:
            if response.instances:
                for instance in response.instances:
                    name = instance.name
                    if name.startswith('agents-'):
                        started = datetime.datetime.fromisoformat(instance.last_start_timestamp).timestamp()
                        instances[name] = {'ip': instance.network_interfaces[0].network_i_p,
                                            'zone': zone,
                                            'started': started}
                        logging.info('%s - %s (uptime: %d)', instance.name, instance.network_interfaces[0].network_i_p, int(time.time() - started))
        # Reconcile the list of instances
        for name in self.instances:
            if name not in instances:
                del self.instances[name]
        for name in instances:
            if name not in self.instances:
                self.instances[name] = {}
            self.instances[name]['ip'] = instances[name]['ip']
            self.instances[name]['zone'] = instances[name]['zone']
            self.instances[name]['started'] = instances[name]['started']

    def update_alive(self):
        logging.info("Updating the last-alive times...")
        beanstalk = greenstalk.Client(('127.0.0.1', 11300), encoding=None, watch='alive')
        # update the last-alive time for all of the instances
        now = time.time()
        try:
            while True:
                job = beanstalk.reserve(1)
                message = json.loads(job.body.decode())
                if 'n' in message and 't' in message:
                    name = message['n']
                    last_alive = message['t']
                    if name in self.instances:
                        self.instances[name]['alive'] = last_alive
                        logging.debug("%s - last alive %d seconds ago", name, now - last_alive)
                    else:
                        logging.debug("%s - Instance not found", name)
                beanstalk.delete(job)
        except greenstalk.TimedOutError:
            pass
        except Exception:
            logging.exception("Error checking alive tube")

    def terminate_instance(self, name):
        logging.debug('Terminating %s...', name)

    def prune_instances(self):
        """ Delete any instances that have been running for more than an hour with a last-alive > 30 minutes ago """
        now = time.time()
        for name in self.instances:
            instance = self.instances[name]
            uptime = now - instance['started']
            if uptime > 3600:
                elapsed = None
                if 'alive' in instance:
                    elapsed = now - instance['alive']
                if elapsed is None or elapsed > 1800:
                    self.terminate_instance(name)

    def run(self):
        self.update_instances()
        self.update_alive()
        with open(self.instances_file, 'wt', encoding='utf-8') as f:
            json.dump(self.instances, f)
        self.prune_instances()

# Make sure only one instance is running at a time
lock_handle = None
def run_once():
    """Use a non-blocking lock on the current code file to make sure multiple instance aren't running"""
    global lock_handle
    try:
        lock_handle = open(os.path.realpath(__file__) + '.lock','w')
        fcntl.flock(lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except:
        logging.critical('Already running')
        os._exit(0)

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s.%(msecs)03d - %(message)s", datefmt="%H:%M:%S")
    run_once()
    healthcheck = Healthcheck()
    try:
        healthcheck.run()
    except Exception:
        logging.exception('Unhandled exception')