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

class Monitor(object):
    """Main agent workflow"""
    def __init__(self):
        pass

    def run(self):
        """ Update the metrics every 30 seconds """
        while True:
            counts = {}
            try:
                beanstalk = greenstalk.Client(('127.0.0.1', 11300))
                for tube in beanstalk.tubes():
                    try:
                        stats = beanstalk.stats_tube(tube)
                        logging.debug(json.dumps(stats))
                        #if tube in TUBES:
                        #    counts[tube]
                    except Exception:
                        pass
            except Exception:
                logging.exception("Error collecting metrics")
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
        level=logging.DEBUG,
        format="%(asctime)s.%(msecs)03d - %(message)s", datefmt="%H:%M:%S")
    run_once()
    monitor = Monitor()
    monitor.run()
    # Force a hard exit so unclean threads can't hang the process
    os._exit(0)
