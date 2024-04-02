#!/usr/bin/env python3
# Copyright 2024 Google Inc.
import fcntl
import json
import logging
import os
import requests
import time
from google.cloud import compute_v1

PROJECT = 'httparchive'
UPDATE_INTERVAL = 1800  # update the list of instances every 30 minutes

class Healthcheck(object):
    def __init__(self):
        self.instances = {}
        self.last_update = None

    def update_instances(self):
        """ Update the authoritative list of running instances """
        now = time.monotonic()
        if self.last_update is None or now - self.last_update > UPDATE_INTERVAL:
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
                            instances[name] = {'ip': instance.network_interfaces[0].network_i_p, 'zone': zone}
                            logging.info('%s - %s', instance.name, instance.network_interfaces[0].network_i_p)
            # Reconcile the list of instances
            for name in self.instances:
                if name not in instances:
                    del self.instances[name]
            for name in instances:
                if name not in self.instances:
                    self.instances[name] = {}
                self.instances[name]['ip'] = instances[name]['ip']
                self.instances[name]['zone'] = instances[name]['zone']
            self.last_update = now

    def check_instance(self, name):
        instance = self.instances[name]
        ip = instance['ip']
        ok = False
        try:
            request = requests.get("http://{}:8889/".format(ip), timeout=60)
            if request.status_code == 200:
                ok = True
        except:
            pass
        if ok:
            logging.info("%s - OK", name)
            instance['c'] = 0
        else:
            if 'c' not in instance:
                instance['c'] = 0
            instance['c'] += 1
            logging.info("%s - FAILED %d times", name, instance['c'])

    def check_instances(self):
        for name in self.instances:
            self.check_instance(name)


    def run(self):
        while True:
            start = time.monotonic()
            self.update_instances()
            self.check_instances()
            elapsed = time.monotonic() - start
            if elapsed < 300:
                time.sleep(300 - elapsed)

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
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d - %(message)s", datefmt="%H:%M:%S")
    run_once()
    healthcheck = Healthcheck()
    try:
        healthcheck.run()
    except Exception:
        logging.exception('Unhandled exception')
