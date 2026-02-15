import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

BOT_NAME = 'colemanfurniture_scraper'

FETCHER_MODULES = ['colemanfurniture_scraper.fetcher']
NEWFETCHER_MODULE = 'colemanfurniture_scraper.fetcher'

ROBOTSTXT_OBEY = True

CONCURRENT_REQUESTS = int(os.getenv('MAX_WORKERS', '32'))
CONCURRENT_REQUESTS_PER_DOMAIN = 8

DOWNLOAD_DELAY = float(os.getenv('DOWNLOAD_DELAY', '0.1'))
RANDOMIZE_DOWNLOAD_DELAY = False

AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1
AUTOTHROTTLE_MAX_DELAY = 10
AUTOTHROTTLE_TARGET_CONCURRENCY = 4.0

COOKIES_ENABLED = False

HTTPCACHE_ENABLED = False
HTTPCACHE_EXPIRATION_SECS = 0
HTTPCACHE_DIR = 'httpcache'
HTTPCACHE_IGNORE_HTTP_CODES = []
HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'

REQUEST_FINGERPRINTER_IMPLEMENTATION = '2.7'
TWISTED_REACTOR = 'twisted.internet.asyncioreactor.AsyncioSelectorReactor'
FEED_EXPORT_ENCODING = 'utf-8'

LOG_LEVEL = 'INFO'
LOG_ENABLED = True
LOG_FILE = None
LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
LOG_DATEFORMAT = '%Y-%m-%d %H:%M:%S'
LOG_STDOUT = True

FTP_HOST = os.getenv('FTP_HOST', '')
FTP_USER = os.getenv('FTP_USER', '')
FTP_PASS = os.getenv('FTP_PASS', '')
FTP_PATH = os.getenv('FTP_PATH', '/uploads/')

OUTPUT_DIR = 'colemanfurniture_scraper/output'