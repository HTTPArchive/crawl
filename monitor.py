#!/usr/bin/env python3
# Copyright 2024 Google Inc.
import fcntl
import greenstalk
import json
import logging
import os
import time
from google.cloud import monitoring_v3

TUBES = {'crawl': 'Crawl tests work queue',
         'retry': 'Crawl tests retry queue',
         'failed': 'Crawl tests failed queue',
         'complete': 'Crawl tests completed queue'}

GROUPS = ['global', 'central-1', 'east-1', 'east-4', 'west-1', 'west-2', 'west-3', 'west-4']

class Monitor(object):
    """Main agent workflow"""
    def __init__(self):
        pass

    def run(self):
        """ Update the metrics every 30 seconds """
        while True:
            counts = {}
            for tube in TUBES:
                counts[tube] = 0
            try:
                beanstalk = greenstalk.Client(('127.0.0.1', 11300))
                for tube in beanstalk.tubes():
                    try:
                        stats = beanstalk.stats_tube(tube)
                        count = stats['current-jobs-ready'] + stats['current-jobs-reserved']
                        logging.info('%s: %d', tube, count)
                        if tube in TUBES:
                            counts[tube] = count
                    except Exception:
                        pass
            except Exception:
                logging.exception("Error collecting metrics")
            try:
                client = monitoring_v3.MetricServiceClient()
                project_name = "projects/httparchive"
                values = []
                for tube in TUBES:
                    for group in GROUPS:
                        series = monitoring_v3.TimeSeries()
                        series.metric.type = "custom.googleapis.com/crawl/{}/{}".format(group, tube)
                        series.resource.type = "global"
                        series.resource.labels["project_id"] = 'httparchive'
                        series.metric.labels["queue"] = tube
                        now = time.time()
                        seconds = int(now)
                        nanos = int((now - seconds) * 10**9)
                        interval = monitoring_v3.TimeInterval(
                            {"end_time": {"seconds": seconds, "nanos": nanos}}
                        )
                        point = monitoring_v3.Point({"interval": interval, "value": {"double_value": counts[tube]}})
                        series.points = [point]
                        values.append(series)
                client.create_time_series(name=project_name, time_series=values)
            except Exception:
                logging.exception("Error reporting metrics")
            time.sleep(30)

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
    monitor = Monitor()
    monitor.run()
    # Force a hard exit so unclean threads can't hang the process
    os._exit(0)
