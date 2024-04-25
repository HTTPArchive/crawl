#!/usr/bin/env python3
# Copyright 2024 Google Inc.
import greenstalk
import logging
import zlib

TUBES = {'crawl': 'Crawl tests work queue',
         'retry': 'Crawl tests retry queue',
         'failed': 'Crawl tests failed queue',
         'complete': 'Crawl tests completed queue',
         'alive': 'Health checks'}

def main():
    """ Drain all of the jobs from all of the queues """
    beanstalk = greenstalk.Client(('127.0.0.1', 11300), encoding=None)
    previous = None
    for tube in beanstalk.tubes():
        if tube in TUBES:
            beanstalk.watch(tube)
            if previous is not None:
                beanstalk.ignore(previous)
            previous = tube
            count = 0
            try:
                while True:
                    job = beanstalk.reserve(0)
                    if tube == 'failed':
                        logging.info("Failed job: %s", zlib.decompress(job.body).decode())
                    count += 1
                    if count % 10000 == 0:
                        logging.info("Drained %d jobs from %s so far...", count, tube)
                    beanstalk.delete(job)
            except greenstalk.TimedOutError:
                pass
            except Exception:
                logging.exception("Error draining tube %s", tube)
            logging.info('Drained %d jobs from %s', count, tube)

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d - %(message)s", datefmt="%H:%M:%S")
    main()
