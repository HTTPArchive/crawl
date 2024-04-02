#!/usr/bin/env python3
# Copyright 2024 Google Inc.
import greenstalk
import logging
import zlib

def main():
    """ Get the stats for the first job in the queue """
    beanstalk = greenstalk.Client(('127.0.0.1', 11300), encoding=None, watch='crawl')
    try:
        job = beanstalk.reserve(1)
        stats = beanstalk.stats_job(job)
        logging.info(stats)
        if stats and 'timeouts' in stats and stats['timeouts'] >= 2:
            logging.info(zlib.decompress(job.body).decode())
        beanstalk.release(job)
    except greenstalk.TimedOutError:
        pass
    except Exception:
        logging.exception("Error checking job")

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d - %(message)s", datefmt="%H:%M:%S")
    main()
