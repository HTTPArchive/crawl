"""
Main entry point for managing the monthly crawl
"""
from concurrent import futures
from datetime import datetime
import fcntl
import logging
import os
import random
import string
import time
try:
    import ujson as json
except BaseException:
    import json

RETRY_COUNT = 2

class Crawl(object):
    """Main agent workflow"""
    def __init__(self):
        self.now = datetime.now()
        self.root_path = os.path.abspath(os.path.dirname(__file__))
        self.data_path = os.path.join(self.root_path, 'data')
        self.crawls = {
            'Desktop': {
                'urls_file': 'urls/latest_crux_desktop.csv',
                'crawl_name': self.now.strftime('chrome-%b_1%Y')
            },
            'Mobile': {
                'urls_file': 'urls/latest_crux_mobile.csv',
                'crawl_name': self.now.strftime('android-%b_1%Y')
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
        self.test_archive = 'results'
        self.har_archive = 'har_test'   #TODO (pmeenan): Change this to crawls after testing
        self.status = None
        self.status_file = os.path.join(self.data_path, 'status.json')
        self.load_status()
        with open(os.path.join(self.root_path, 'crux_keys.json'), 'rt') as f:
            self.crux_keys = json.load(f)

    def run(self):
        """Main Crawl entrypoint"""
        self.start_crawl()
        if not self.status['done']:
            self.retry_jobs()
            self.check_done()

    def start_crawl(self):
        """Start a new crawl if necessary"""
        if self.status is None or 'crawl' not in self.status or self.status['crawl'] != self.current_crawl:
            try:
                self.update_url_lists()
                self.submit_tests()
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

    def submit_tests(self):
        """Interleave between the URL lists for the various crawls"""
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
        batch_settings = pubsub_v1.types.BatchSettings(
            max_messages = 1000,                  # 1000 messages
            max_bytes = 1 * 1000 * 1000 * 100,    # 100MB
            max_latency = 0.1,                    # 100ms
        )
        publisher = pubsub_v1.PublisherClient(batch_settings)
        publisher_futures = []
        pending_count = 0
        test_count = 0
        test_queue = publisher.topic_path(self.project, self.crawl_queue)
        retry_queue = publisher.topic_path(self.project, self.retry_queue)
        while not all_done:
            #TODO (pmeenan): Remove this limit after testing
            if test_count >= 100:
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
                                        'layout': crawl_name
                                    },
                                    'crux_api_key': random.choice(self.crux_keys),
                                    'pubsub_retry_queue': retry_queue,
                                    'gcs_test_archive': {
                                        'bucket': self.bucket,
                                        'path': self.test_archive
                                    },
                                    'gcs_har_upload': {
                                        'bucket': self.bucket,
                                        'path': self.har_archive + '/' + self.crawls[crawl_name]['crawl_name']
                                    }
                                }
                                if 'job' in self.crawls[crawl_name]:
                                    job.update(self.crawls[crawl_name]['job'])
                                job_str = json.dumps(job)
                                try:
                                    publisher_future = publisher.publish(test_queue, job_str.encode())
                                    publisher_futures.append(publisher_future)
                                    pending_count += 1
                                    test_count += 1
                                except Exception:
                                    logging.exception('Exception publishing job')
                                if pending_count >= 10000:
                                    futures.wait(publisher_futures, return_when=futures.ALL_COMPLETED)
                                    logging.info('Queued %d tests (%d in this batch)...', index, pending_count)
                                    publisher_futures = []
                                    pending_count = 0
                    except Exception:
                        logging.exception('Error processing URL')
                except StopIteration:
                    crawl['done'] = True
                    all_done = True
                    for name in url_lists:
                        if not url_lists[name]['done']:
                            all_done = False

        # Wait for all of the pending jobs to submit
        publisher.stop()
        if pending_count > 0:
            futures.wait(publisher_futures, return_when=futures.ALL_COMPLETED)
            logging.info('Queued %d tests...', index)
            publisher_futures = []
            pending_count = 0

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

    def retry_jobs(self):
        """Retry any jobs in the retry or failed queue"""
        from google.cloud import pubsub_v1
        logging.info('Checking for jobs that need to be retried...')
        subscriber = pubsub_v1.SubscriberClient()            
        publisher = pubsub_v1.PublisherClient()
        test_queue = publisher.topic_path(self.project, self.crawl_queue)
        publisher_futures = []
        queues = [self.retry_queue, self.failed_queue]
        retry_count = 0
        failed_count = 0
        for queue_name in queues:
            done = False
            subscription = subscriber.subscription_path(self.project, queue_name)
            while not done:
                try:
                    response = subscriber.pull(request={
                        'subscription': subscription,
                        'max_messages': 1,
                        'return_immediately': True,
                        }, timeout=10)
                    if len(response.received_messages) == 1:
                        msg = response.received_messages[0]
                        response_text = msg.message.data.decode('utf-8')
                        job = json.loads(response_text)
                        if 'metadata' in job and 'layout' in job['metadata']:
                            crawl_name = job['metadata']['layout']
                            if 'retry_count' not in job['metadata']:
                                job['metadata']['retry_count'] = 0
                            job['metadata']['retry_count'] += 1
                            if job['metadata']['retry_count'] <= RETRY_COUNT:
                                # resubmit the job to the main work queue
                                job_str = json.dumps(job)
                                try:
                                    publisher_future = publisher.publish(test_queue, job_str.encode())
                                    publisher_futures.append(publisher_future)
                                    retry_count += 1
                                except Exception:
                                    logging.exception('Exception publishing job')
                            else:
                                failed_count += 1
                                if self.status is not None and 'crawls' in self.status and crawl_name in self.status['crawls']:
                                    crawl = self.status['crawls'][crawl_name]
                                    if 'failed_count' not in crawl:
                                        crawl['failed_count'] = 0
                                    crawl['failed_count'] += 1
                        subscriber.acknowledge(request={
                            'subscription': subscription,
                            'ack_ids': [msg.ack_id]})
                    else:
                        done = True
                except Exception:
                    logging.exception('Error processing retry queue')
                    done = True
        subscriber.close()
        if len(publisher_futures):
            futures.wait(publisher_futures, return_when=futures.ALL_COMPLETED)
        if retry_count:
            logging.info("%d tests submitted for retry", retry_count)
            self.status['last'] = time.time()
        if failed_count:
            logging.info("%d tests failed all retries", failed_count)

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

    def save_status(self):
        """Save the crawls status file"""
        try:
            with open(self.status_file, 'wt') as f:
                json.dump(self.status, f, indent=4, sort_keys=True)
        except Exception:
            logging.exception('Error saving status')

    def num_to_str(self, num):
        """encode a number as an alphanum sequence"""
        num_str = ''
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
        filemode='w',
        format="%(asctime)s.%(msecs)03d - %(message)s", datefmt="%H:%M:%S")
    run_once()
    crawl = Crawl()
    crawl.run()
    logging.info('Done')

    # Force a hard exit so unclean threads can't hang the process
    os._exit(0)
