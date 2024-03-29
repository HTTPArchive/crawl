#!/usr/bin/env python3
# Copyright 2024 Google Inc.
import greenstalk
import logging
from google.cloud import monitoring_v3

TUBES = {'crawl': 'Crawl tests work queue',
         'retry': 'Crawl tests retry queue',
         'failed': 'Crawl tests failed queue',
         'complete': 'Crawl tests completed queue'}

def main():
    """ Drain all of the jobs from all of the queues """
    beanstalk = greenstalk.Client(('127.0.0.1', 11300))
    for tube in beanstalk.tubes():
        beanstalk.watch(tube)
        count = 0
        try:
            job = beanstalk.reserve(0)
            beanstalk.delete(job)
        except Exception:
            pass
        beanstalk.ignore(tube)
        logging.info('Drained {} jobs from {}', count, tube)

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d - %(message)s", datefmt="%H:%M:%S")
    main()
