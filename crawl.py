"""
Main entry point for managing the monthly crawl
"""
from datetime import datetime
from urllib.parse import urlparse
import fcntl
import logging
import queue
import os
import random
import string
import threading
import time
try:
    import ujson as json
except BaseException:
    import json

RUN_TIME = 3500
RETRY_COUNT = 2
MAX_DEPTH = 1
MAX_BREADTH = 1
TESTING = False

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
        self.done_queue = 'crawl-complete'
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
        self.previous_count = None
        self.previous_time = None
        self.status_file = os.path.join(self.data_path, 'status.json')
        self.crawled_file = os.path.join(self.data_path, 'crawled.json')
        self.status_mutex = threading.Lock()
        self.crawled = None
        self.load_status()
        self.crux_keys = []
        crux_file = os.path.join(self.root_path, 'crux_keys.json')
        if os.path.exists(crux_file):
            with open(crux_file, 'rt') as f:
                self.crux_keys = json.load(f)
        self.job_queue = queue.Queue(maxsize=2000)

    def run(self):
        """Main Crawl entrypoint"""
        self.job_thread = threading.Thread(target=self.submit_jobs)
        self.job_thread.start()
        self.start_crawl()
        if not self.status['done']:
            threads = []
            for _ in range(10):
                thread = threading.Thread(target=self.retry_thread)
                thread.start()
                threads.append(thread)
            thread = threading.Thread(target=self.failed_thread)
            thread.start()
            threads.append(thread)
            for _ in range(10):
                thread = threading.Thread(target=self.completed_thread)
                thread.start()
                threads.append(thread)

            # Pump jobs until time is up
            time.sleep(RUN_TIME)

            # Wait for the subscriptions to exit
            for thread in threads:
                try:
                    thread.join(timeout=60)
                except Exception:
                    pass

            # Check the status of the overall crawl
            with self.status_mutex:
                self.check_done()
                self.status['tm'] = time.time()
                if self.previous_pending and self.previous_time and self.status is not None and 'pending' in self.status:
                    elapsed = self.status['tm'] - self.previous_time
                    count = self.previous_pending - self.status['pending']
                    if self.previous_count and 'count' in self.status and self.status['count'] > self.previous_count:
                        count += self.status['count'] - self.previous_count
                    if elapsed > 0 and count > 0:
                        self.status['rate'] = int(round((count * 3600) / elapsed))
                self.save_status()
                if 'rate' in self.status:
                    logging.info('Done - Test rate: %d tests per hour', self.status['rate'])
                else:
                    logging.info('Done')
        self.must_exit = True
        self.job_thread.join(timeout=600)
        self.save_status()
        if not self.status['done']:
            self.save_crawled()

    def retry_thread(self):
        """Thread for retry queue subscription"""
        from google.cloud import pubsub_v1
        subscriber = pubsub_v1.SubscriberClient()
        subscription = subscriber.subscription_path(self.project, self.retry_queue)
        flow_control = pubsub_v1.types.FlowControl(max_messages=15)
        subscription_future = subscriber.subscribe(subscription, callback=self.retry_job, flow_control=flow_control, await_callbacks_on_shutdown=True)
        with subscriber:
            try:
                subscription_future.result(timeout=RUN_TIME)
            except TimeoutError:
                subscription_future.cancel()
                subscription_future.result(timeout=30)
            except Exception:
                logging.exception('Error waiting on subscription')

    def failed_thread(self):
        """Thread for falied queue subscription"""
        from google.cloud import pubsub_v1
        subscriber = pubsub_v1.SubscriberClient()
        subscription = subscriber.subscription_path(self.project, self.failed_queue)
        flow_control = pubsub_v1.types.FlowControl(max_messages=1)
        subscription_future = subscriber.subscribe(subscription, callback=self.retry_job, flow_control=flow_control, await_callbacks_on_shutdown=True)
        with subscriber:
            try:
                subscription_future.result(timeout=RUN_TIME)
            except TimeoutError:
                subscription_future.cancel()
                subscription_future.result(timeout=30)
            except Exception:
                logging.exception('Error waiting on subscription')

    def completed_thread(self):
        """Thread for completed queue subscription"""
        from google.cloud import pubsub_v1
        subscriber = pubsub_v1.SubscriberClient()
        subscription = subscriber.subscription_path(self.project, self.completed_queue)
        flow_control = pubsub_v1.types.FlowControl(max_messages=15)
        subscription_future = subscriber.subscribe(subscription, callback=self.crawl_job, flow_control=flow_control, await_callbacks_on_shutdown=True)
        with subscriber:
            try:
                subscription_future.result(timeout=RUN_TIME)
            except TimeoutError:
                subscription_future.cancel()
                subscription_future.result(timeout=30)
            except Exception:
                logging.exception('Error waiting on subscription')

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
                    with self.status_mutex:
                        if self.status is not None and 'crawls' in self.status and crawl_name in self.status['crawls']:
                            crawl = self.status['crawls'][crawl_name]
                            if 'failed_count' not in crawl:
                                crawl['failed_count'] = 0
                            crawl['failed_count'] += 1
        except Exception:
            logging.exception('Error processing pubsub job')
        try:
            message.ack()
        except Exception:
            pass

    def crawl_job(self, message):
        """Pubsub callback for jobs that completed (for crawling deeper into)"""
        try:
            import copy
            job = json.loads(message.data.decode('utf-8'))
            if job is not None and 'metadata' in job and 'layout' in job['metadata'] and \
                    'crawl_depth' in job['metadata'] and job['metadata']['crawl_depth'] < MAX_DEPTH and \
                    'results' in job:
                crawl_name = job['metadata']['layout']
                job['metadata']['crawl_depth'] += 1
                if job['metadata']['crawl_depth'] == MAX_DEPTH and 'pubsub_completed_queue' in job:
                    del job['pubsub_completed_queue']
                job['metadata']['retry_count'] = 0
                job['metadata']['parent_page_id'] = job['metadata']['page_id']
                job['metadata']['parent_page_url'] = job['metadata']['tested_url']
                job['metadata']['parent_page_test_id'] = job['Test ID']
                parent_origin = 'none'
                try:
                    parent_origin = urlparse(job['metadata']['parent_page_url']).hostname.lower()
                except Exception:
                    logging.exception('Error extracting origin from parent URL')
                links = []
                try:
                    links = job['results']['1']['FirstView']['1']['crawl_links']
                except Exception:
                    logging.exception('Failed to get crawl links from result')
                # Keep track of which pages on this origin have been visited
                visited = job['metadata'].get('visited', [job['url']])
                width = 0
                crawl_links = []
                max_children = MAX_BREADTH * MAX_DEPTH
                root_page = job['metadata']['root_page_id']
                skip_extensions = ['.jpg', '.jpeg', '.gif', '.png', '.webp', '.avif', '.webm', '.pdf', '.tiff', '.zip']
                if links:
                    for link in links:
                        try:
                            link_origin = urlparse(link).hostname.lower()
                            _, extension = os.path.splitext(urlparse(link).path.lower())
                            if link_origin == parent_origin and extension not in skip_extensions and link not in visited:
                                with self.status_mutex:
                                    if root_page not in self.crawled:
                                        self.crawled[root_page] = 0
                                    self.crawled[root_page] += 1
                                    if self.crawled[root_page] > max_children:
                                        break
                                width += 1
                                if width > MAX_BREADTH:
                                    break
                                crawl_links.append(link)
                                visited.append(link)
                        except Exception:
                            logging.exception('Error parsing URL')
                job['metadata']['visited'] = visited
                # Create the new jobs
                width = 0
                for link in crawl_links:
                    page_id = None
                    with self.status_mutex:
                        self.status['count'] += 1
                        page_id = self.status['count']
                        self.status['crawls'][crawl_name]['url_count'] += 1
                    new_job = copy.deepcopy(job)
                    del new_job['results']
                    new_job['Test ID'] = '{0}l{1}'.format(job['metadata']['parent_page_test_id'], width)
                    new_job['url'] = link
                    new_job['metadata']['link_depth'] = width
                    new_job['metadata']['page_id'] = page_id
                    new_job['metadata']['tested_url'] = link
                    self.job_queue.put(new_job, block=True, timeout=300)
                    width += 1
            else:
                logging.debug('Invalid crawl job')
        except Exception:
            logging.exception('Error processing pubsub job')
        try:
            message.ack()
        except Exception:
            pass

    def start_crawl(self):
        """Start a new crawl if necessary"""
        if self.status is None or 'crawl' not in self.status or self.status['crawl'] != self.current_crawl:
            try:
                # Delete the old log
                try:
                    os.unlink('crawl.log')
                except Exception:
                    pass
                self.crawled = {}
                self.update_url_lists()
                self.submit_initial_tests()
                self.save_status()
            except Exception:
                logging.exception('Error starting new crawl')
        elif self.status is not None and not self.status.get('done'):
            self.load_crawled()

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
        logging.debug('Submit thread started')
        from google.cloud import pubsub_v1
        batch_settings = pubsub_v1.types.BatchSettings(
            max_messages = 1000,                  # 1000 messages
            max_bytes = 1 * 1000 * 1000 * 100,    # 100MB
            max_latency = 1,                      # 1s
        )
        publisher = pubsub_v1.PublisherClient(batch_settings)
        test_queue = publisher.topic_path(self.project, self.crawl_queue)
        pending_count = 0
        total_count = 0
        while not self.must_exit:
            try:
                while not self.must_exit:
                    job = self.job_queue.get(block=True, timeout=5)
                    if job is not None:
                        job_str = json.dumps(job)
                        try:
                            publisher.publish(test_queue, job_str.encode())
                            logging.debug(job_str)
                            pending_count += 1
                            total_count += 1
                            if pending_count >= 1000:
                                logging.info('Queued %d tests (%d in this batch)...', total_count, pending_count)
                                pending_count = 0
                                with self.status_mutex:
                                    self.status['last'] = time.time()
                        except Exception:
                            logging.exception('Error publishing test to queue')
                    self.job_queue.task_done()
            except Exception:
                pass
            if pending_count:
                logging.info('Queued %d tests (%d in this batch)...', total_count, pending_count)
                pending_count = 0
                with self.status_mutex:
                    self.status['last'] = time.time()
        publisher.stop()
        logging.debug('Submit thread complete')

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
                                        'link_depth': 0,
                                        'root_page_id': index,
                                        'root_page_url': url,
                                        'root_page_test_id': test_id
                                    },
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
                                if MAX_DEPTH > 0:
                                    job['pubsub_completed_queue'] = completed_queue
                                    job['pubsub_completed_metrics'] = ['crawl_links']
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
                        from google.cloud import storage, pubsub_v1
                        publisher = pubsub_v1.PublisherClient()
                        done_queue = publisher.topic_path(self.project, self.done_queue)
                        client = storage.Client()
                        bucket = client.get_bucket(self.bucket)
                        for crawl_name in self.crawls:
                            gcs_path = self.har_archive + '/' + self.crawls[crawl_name]['crawl_name'] + '/done'
                            blob = bucket.blob(gcs_path)
                            blob.upload_from_string('')
                            logging.info('Uploaded done file to gs://%s/%s', self.bucket, gcs_path)
                            message = 'gs://{}/{}/{}'.format(self.bucket, self.har_archive, self.crawls[crawl_name]['crawl_name'])
                            publisher.publish(done_queue, message.encode())
                            logging.debug('%s posted to done queue %s', message, self.done_queue)
                        logging.info('Crawl complete')
                        self.status['done'] = True
                        self.crawled = {}
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
            if 'count' in self.status:
                self.previous_count = self.status['count']
            if 'tm' in self.status:
                self.previous_time = self.status['tm']

    def load_crawled(self):
        logging.info('Loading crawled...')
        if os.path.exists(self.crawled_file):
            try:
                with open(self.crawled_file, 'rt') as f:
                    self.crawled = json.load(f)
            except Exception:
                logging.exception('Error loading status')
        if self.crawled is None:
            self.crawled = {}
        logging.info('Loading crawled complete')

    def save_crawled(self):
        logging.info('Saving crawled...')
        try:
            with open(self.crawled_file, 'wt') as f:
                json.dump(self.crawled, f)
        except Exception:
            logging.exception('Error saving crawled history')
        logging.info('Saving crawled complete')

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
    try:
        lock_handle = open(os.path.realpath(__file__) + '.lock','w')
        fcntl.flock(lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except:
        logging.critical('Already running')
        os._exit(0)

if __name__ == '__main__':
    if TESTING:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s.%(msecs)03d - %(message)s", datefmt="%H:%M:%S")
    else:
        logging.basicConfig(
            level=logging.INFO,
            filename='crawl.log',
            format="%(asctime)s.%(msecs)03d - %(message)s", datefmt="%H:%M:%S")
    run_once()
    crawl = Crawl()
    crawl.run()
    # Force a hard exit so unclean threads can't hang the process
    os._exit(0)
