"""
Main entry point for managing the monthly crawl
"""
from concurrent import futures
from datetime import datetime
import logging
import os
import string
import fcntl
try:
    import ujson as json
except BaseException:
    import json

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
        self.current_crawl = self.now.strftime('%Y-%d')
        self.id_characters = string.digits + string.ascii_uppercase
        self.project = 'httparchive'
        self.bucket = 'httparchive'
        self.crawl_queue = 'crawl-queue'
        self.retry_queue = 'crawl-queue-retry'
        self.failed_queue = 'crawl-queue-failed'
        self.test_archive = 'results'
        self.har_archive = 'har_test'
        self.status = None
        self.status_file = os.path.join(self.data_path, 'status.json')
        self.load_status()

    def run(self):
        """Main Crawl entrypoint"""
        self.start_crawl()
        self.retry_jobs()
        self.check_queues()

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
        test_queue = publisher.topic_path(self.project, self.crawl_queue)
        retry_queue = publisher.topic_path(self.project, self.retry_queue)
        while not all_done:
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
            'crawls': {}
        }

        # Close the CSV files
        for crawl_name in url_lists:
            crawl = url_lists[crawl_name]
            self.status['crawls'][crawl_name] = {
                'url_count': crawl['count']
            }
            logging.info('%s URLs submitted: %d', crawl_name, crawl['count'])
            crawl['fp'].close()

    def retry_jobs(self):
        """Retry any jobs in the retry queue"""

    def check_queues(self):
        """Check the pub/sub queue lengths to see the crawl progress"""
    
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
    logging.basicConfig(level=logging.INFO, format="%(asctime)s.%(msecs)03d - %(message)s", datefmt="%H:%M:%S")
    run_once()
    crawl = Crawl()
    crawl.run()
    logging.info('Done')

    # Force a hard exit so unclean threads can't hang the process
    os._exit(0)
