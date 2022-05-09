"""
Main entry point for managing the monthly crawl
"""
from concurrent import futures
from datetime import datetime
import fcntl
import logging
import multiprocessing
import os
import random
import string
import threading
import time
try:
    import ujson as json
except BaseException:
    import json

RETRY_COUNT = 2
TESTING = True

class Crawl(object):
    """Main agent workflow"""
    def __init__(self):
        self.must_exit = False
        self.now = datetime.now()
        self.root_path = os.path.abspath(os.path.dirname(__file__))
        self.data_path = os.path.join(self.root_path, 'data')
        self.crawls = {
            'Desktop': {
                'urls_file': 'urls/latest_crux_desktop.csv',
                'crawl_name': self.now.strftime('chrome-%b_1_%Y')
            },
            'Mobile': {
                'urls_file': 'urls/latest_crux_mobile.csv',
                'crawl_name': self.now.strftime('android-%b_1_%Y')
            }
        }
        for crawl_name in self.crawls:
            template_file = os.path.join(self.root_path, crawl_name + '.json')
            if os.path.exists(template_file):
                with open(template_file, 'rt') as f:
                    self.crawls[crawl_name]['job'] = json.load(f)
        self.current_crawl = self.now.strftime('%Y-%m')
        self.id_characters = string.digits + string.ascii_uppercase
        self.project = 'httparchive'
        self.bucket = 'httparchive'
        self.crawl_queue = 'crawl-queue'
        self.retry_queue = 'crawl-queue-retry'
        self.failed_queue = 'crawl-queue-failed'
        self.completed_queue = 'crawl-queue-completed'
        self.test_archive = 'results'
        self.har_archive = 'crawls'
        if TESTING:
            self.crawl_queue += '-test'
            self.retry_queue += '-test'
            self.failed_queue += '-test'
            self.completed_queue += '-test'
            self.har_archive += '-test'
        self.status = None
        self.previous_pending = None
        self.previous_time = None
        self.status_file = os.path.join(self.data_path, 'status.json')
        self.status_mutex = threading.Lock()
        self.load_status()
        self.crux_keys = []
        crux_file = os.path.join(self.root_path, 'crux_keys.json')
        if os.path.exists(crux_file):
            with open(crux_file, 'rt') as f:
                self.crux_keys = json.load(f)
        self.job_queue = multiprocessing.JoinableQueue(maxsize=1000)

    def run(self):
        """Main Crawl entrypoint"""
        self.job_thread = threading.Thread(target=self.submit_jobs)
        self.job_thread.start()
        self.start_crawl()
        if not self.status['done']:
            # Start subscriptions for the retry and failed queues
            from google.cloud import pubsub_v1
            retry_subscriber = pubsub_v1.SubscriberClient()
            retry_subscription = retry_subscriber.subscription_path(self.project, self.retry_queue)
            retry_flow_control = pubsub_v1.types.FlowControl(max_messages=100)
            retry_future = retry_subscriber.subscribe(retry_subscription, callback=self.retry_job, flow_control=retry_flow_control)
            failed_subscriber = pubsub_v1.SubscriberClient()
            failed_subscription = failed_subscriber.subscription_path(self.project, self.failed_queue)
            failed_flow_control = pubsub_v1.types.FlowControl(max_messages=100)
            failed_future = failed_subscriber.subscribe(failed_subscription, callback=self.retry_job, flow_control=failed_flow_control)

            # Pump jobs for 4 minutes
            time.sleep(240)

            # Cancel the subscriptions and drain any work
            retry_future.cancel()
            failed_future.cancel()
            retry_future.result(timeout=300)
            failed_future.result(timeout=300)

            # Check the status of the overall crawl
            self.status_mutex.acquire()
            self.check_done()
            self.status['tm'] = time.time()
            if self.previous_pending and self.previous_time and self.status is not None and 'pending' in self.status:
                elapsed = self.status['tm'] - self.previous_time
                count = self.previous_pending - self.status['pending']
                if elapsed > 0 and count > 0:
                    self.status['rate'] = int(round((count * 3600) / elapsed))
            self.save_status()
            if 'rate' in self.status:
                logging.info('Done - Test rate: %d tests per hour', self.status['rate'])
            else:
                logging.info('Done')
            self.status_mutex.release()
        self.must_exit = True
        self.job_thread.join(timeout=600)

    def retry_job(self, message):
        """Pubsub callback for jobs that may need to be retried"""
        try:
            job = json.loads(message.data.decode('utf-8'))
            if job is not None and 'metadata' in job and 'layout' in job['metadata']:
                crawl_name = job['metadata']['layout']
                if 'retry_count' not in job['metadata']:
                    job['metadata']['retry_count'] = 0
                job['metadata']['retry_count'] += 1
                if job['metadata']['retry_count'] <= RETRY_COUNT:
                    self.job_queue.put(job, block=True, timeout=300)
                else:
                    self.status_mutex.acquire()
                    if self.status is not None and 'crawls' in self.status and crawl_name in self.status['crawls']:
                        crawl = self.status['crawls'][crawl_name]
                        if 'failed_count' not in crawl:
                            crawl['failed_count'] = 0
                        crawl['failed_count'] += 1
                    self.status_mutex.release()
        except Exception:
            logging.exception('Error processing pubsub job')
        message.ack()

    def start_crawl(self):
        """Start a new crawl if necessary"""
        if self.status is None or 'crawl' not in self.status or self.status['crawl'] != self.current_crawl:
            try:
                # Delete the old log
                try:
                    os.unlink('crawl.log')
                except Exception:
                    pass
                self.update_url_lists()
                self.submit_initial_tests()
                self.save_status()
            except Exception:
                logging.exception('Error starting new crawl')

    def update_url_lists(self):
        """Download the lastes CrUX URL lists"""
        from google.cloud import storage
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket)
        for crawl_name in self.crawls:
            logging.info('Downloading %s url list...', crawl_name)
            crawl = self.crawls[crawl_name]
            blob = bucket.blob(crawl['urls_file'])
            blob.download_to_filename(os.path.join(self.data_path, crawl_name + '.csv'))

    def submit_jobs(self):
        """Background thread that takes jobs from the job queue and submits them to the pubsub pending jobs list"""
        from google.cloud import pubsub_v1
        batch_settings = pubsub_v1.types.BatchSettings(
            max_messages = 1000,                  # 1000 messages
            max_bytes = 1 * 1000 * 1000 * 100,    # 100MB
            max_latency = 0.1,                    # 100ms
        )
        publisher = pubsub_v1.PublisherClient(batch_settings)
        publisher_futures = []
        test_queue = publisher.topic_path(self.project, self.crawl_queue)
        pending_count = 0
        total_count = 0
        while not self.must_exit:
            try:
                job = self.job_queue.get(block=True, timeout=1)
                if job is not None:
                    job_str = json.dumps(job)
                    try:
                        publisher_future = publisher.publish(test_queue, job_str.encode())
                        publisher_futures.append(publisher_future)
                        pending_count += 1
                        total_count += 1
                        if pending_count >= 10000:
                            futures.wait(publisher_futures, return_when=futures.ALL_COMPLETED)
                            logging.info('Queued %d tests (%d in this batch)...', total_count, pending_count)
                            publisher_futures = []
                            pending_count = 0
                    except Exception:
                        logging.exception
            except Exception:
                pass
        if len(publisher_futures):
            futures.wait(publisher_futures, return_when=futures.ALL_COMPLETED)
            logging.info('Queued %d tests (%d in this batch)...', total_count, pending_count)
        if total_count:
            self.status_mutex.acquire()
            self.status['last'] = time.time()
            self.status_mutex.release()
        publisher.stop()

    def submit_initial_tests(self):
        """Interleave between the URL lists for the various crawls and post jobs to the submit thread"""
        import csv
        from google.cloud import pubsub_v1

        logging.info('Submitting URLs for testing...')
        url_lists = {}
        # Open the CSV files in parallel
        for crawl_name in self.crawls:
            url_lists[crawl_name] = {}
            crawl = url_lists[crawl_name]
            crawl['fp'] = open(os.path.join(self.data_path, crawl_name + '.csv'), 'rt', newline='')
            crawl['reader'] = csv.reader(crawl['fp'])
            crawl['count'] = 0
            crawl['done'] = False

        # Iterate over all of the crawls in parallel
        all_done = False
        index = 0
        publisher = pubsub_v1.PublisherClient()
        test_count = 0
        retry_queue = publisher.topic_path(self.project, self.retry_queue)
        completed_queue = publisher.topic_path(self.project, self.completed_queue)
        while not all_done:
            if TESTING and test_count > 10:
                break
            for crawl_name in url_lists:
                crawl = url_lists[crawl_name]
                try:
                    entry = next(crawl['reader'])
                    try:
                        if len(entry) >= 2:
                            url = entry[1]
                            if url.startswith('http'):
                                index += 1
                                rank = int(entry[0])
                                crawl['count'] += 1
                                test_id = self.generate_test_id(crawl_name, index)
                                job = {
                                    'Test ID': test_id,
                                    'url': url,
                                    'metadata': {
                                        'rank': rank,
                                        'page_id': index,
                                        'tested_url': url,
                                        'layout': crawl_name,
                                        'crawl_depth': 0,
                                        'link_depth': 0
                                    },
                                    'pubsub_retry_queue': retry_queue,
                                    'pubsub_completed_queue': completed_queue,
                                    'pubsub_completed_metrics': ['crawl_links'],
                                    'gcs_test_archive': {
                                        'bucket': self.bucket,
                                        'path': self.test_archive
                                    },
                                    'gcs_har_upload': {
                                        'bucket': self.bucket,
                                        'path': self.har_archive + '/' + self.crawls[crawl_name]['crawl_name']
                                    }
                                }
                                if self.crux_keys is not None and len(self.crux_keys):
                                    job['crux_api_key'] = random.choice(self.crux_keys)
                                if 'job' in self.crawls[crawl_name]:
                                    job.update(self.crawls[crawl_name]['job'])
                                self.job_queue.put(job, block=True, timeout=600)
                                test_count += 1
                    except Exception:
                        logging.exception('Error processing URL')
                except StopIteration:
                    crawl['done'] = True
                    all_done = True
                    for name in url_lists:
                        if not url_lists[name]['done']:
                            all_done = False

        self.status = {
            'crawl': self.current_crawl,
            'crawls': {},
            'done': False,
            'count': test_count,
            'last': time.time()
        }

        # Close the CSV files and initialize the status
        for crawl_name in url_lists:
            crawl = url_lists[crawl_name]
            self.status['crawls'][crawl_name] = {
                'url_count': crawl['count'],
                'failed_count': 0,
                'name': self.crawls[crawl_name]['crawl_name']
            }
            logging.info('%s URLs submitted: %d', crawl_name, crawl['count'])
            crawl['fp'].close()

    def check_done(self):
        """Check the pub/sub queue length to see the crawl progress"""
        try:
            from google.cloud import monitoring_v3

            client = monitoring_v3.MetricServiceClient()
            now = time.time()
            seconds = int(now)
            nanos = int((now - seconds) * 10 ** 9)
            interval = monitoring_v3.TimeInterval({
                    "end_time": {"seconds": seconds, "nanos": nanos},
                    "start_time": {"seconds": (seconds - 600), "nanos": nanos},
                })

            results = client.list_time_series(
                request={
                    "name": 'projects/{}'.format(self.project),
                    "filter": 'metric.type = "pubsub.googleapis.com/subscription/num_undelivered_messages" AND resource.labels.subscription_id = "{}"'.format(self.crawl_queue),
                    "interval": interval,
                    "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
                }
            )

            self.status['pending'] = -1
            timestamp = None
            for result in results:
                for point in result.points:
                    if timestamp is None or point.interval.end_time > timestamp:
                        self.status['pending'] = point.value.int64_value
                        timestamp = point.interval.end_time
    
            logging.info("%d tests pending", self.status['pending'])

            # Mark the crawl as done if we haven't moved or submitted tests
            # in the last hour and the crawl queue has been empty for an hour
            if self.status['pending'] > 0:
                self.status['last'] = now
                if 'first_empty' in self.status:
                    del self.status['first_empty']
            elif self.status['pending'] == 0:
                if 'first_empty' not in self.status:
                    self.status['first_empty'] = now
                elapsed_activity = now - self.status['last']
                elapsed_empty = now - self.status['first_empty']
                logging.info("%0.1fs since last activity and %0.1fs since the queue became empty.", elapsed_activity, elapsed_empty)
                if elapsed_activity > 3600 and elapsed_empty > 3600:
                    try:
                        from google.cloud import storage
                        client = storage.Client()
                        bucket = client.get_bucket(self.bucket)
                        for crawl_name in self.crawls:
                            gcs_path = self.har_archive + '/' + self.crawls[crawl_name]['crawl_name'] + '/done'
                            blob = bucket.blob(gcs_path)
                            blob.upload_from_string('')
                            logging.info('Uploaded done file to gs://%s/%s', self.bucket, gcs_path)
                        logging.info('Crawl complete')
                        self.status['done'] = True
                    except Exception:
                        logging.exception('Error marking crawls as done')
        except Exception:
            logging.exception('Error checking queue status')

    def load_status(self):
        """Load the status.json"""
        if os.path.exists(self.status_file):
            try:
                with open(self.status_file, 'rt') as f:
                    self.status = json.load(f)
            except Exception:
                logging.exception('Error loading status')
        if self.status is not None:
            if 'pending' in self.status:
                self.previous_pending = self.status['pending']
            if 'tm' in self.status:
                self.previous_time = self.status['tm']


    def save_status(self):
        """Save the crawls status file"""
        try:
            with open(self.status_file, 'wt') as f:
                json.dump(self.status, f, indent=4, sort_keys=True)
        except Exception:
            logging.exception('Error saving status')
        logging.info("Status: %s", json.dumps(self.status, sort_keys=True))

    def num_to_str(self, num):
        """encode a number as an alphanum sequence"""
        num_str = '0' if num == 0 else ''
        count = len(self.id_characters)
        while num > 0:
            index = int(num % count)
            num_str = self.id_characters[index] + num_str
            num //= count
        return num_str

    def generate_test_id(self, crawl_name, num):
        """Convert an index into a dated test ID, grouped in groups of 10,000"""
        prefix = ''
        if crawl_name == 'Desktop':
            prefix = 'Dx'
        elif crawl_name == 'Mobile':
            prefix = 'Mx'
        if TESTING:
            prefix = 'T' + prefix
        group = int(num // 10000)
        return self.now.strftime('%y%m%d') + '_' + prefix + self.num_to_str(group) + '_' + self.num_to_str(num)

# Make sure only one instance is running at a time
lock_handle = None
def run_once():
    """Use a non-blocking lock on the current code file to make sure multiple instance aren't running"""
    global lock_handle
    lock_handle = open(os.path.realpath(__file__),'r')
    try:
        fcntl.flock(lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except:
        logging.critical('Already running')
        os._exit(0)

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        filename='crawl.log',
        format="%(asctime)s.%(msecs)03d - %(message)s", datefmt="%H:%M:%S")
    run_once()
    crawl = Crawl()
    crawl.run()

    # Force a hard exit so unclean threads can't hang the process
    os._exit(0)
