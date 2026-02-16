import os
import sys
import json
import argparse
import logging

logger = logging.getLogger("app")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)
logging.getLogger("scrapy").setLevel(logging.CRITICAL)
logging.getLogger("twisted").setLevel(logging.CRITICAL)

logging.getLogger("twisted").propagate = False
logging.getLogger("scrapy.core.engine").setLevel(logging.CRITICAL)
logging.getLogger("scrapy.dupefilter").setLevel(logging.CRITICAL)
logging.getLogger("scrapy.downloadermiddlewares").setLevel(logging.CRITICAL)

from pathlib import Path
from urllib.parse import urljoin, urlparse
from datetime import datetime
import multiprocessing
from multiprocessing import Process
import time

sys.path.insert(0, str(Path(__file__).parent.parent))

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from fetcher.product_fetcher import ProductFetcher

class AshleyURLSpider(scrapy.Spider):
    """Fast parallel URL fetcher from manufacturer API"""
    name = "ashley_url_fetcher"
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.manufacturer_id = kwargs.get('manufacturer_id', '250')
        self.base_api = f"https://colemanfurniture.com/manufacturer/detail/{self.manufacturer_id}"
        self.ashley_urls = set()
        self.start_page = int(kwargs.get('start_page', 1))
        self.end_page = int(kwargs.get('end_page', 150))
        self.url_list = kwargs.get('url_list')
        self.concurrent_pages = int(kwargs.get('concurrent_pages', 20))
        
    def start_requests(self):
        """Start multiple page requests concurrently"""
        for page in range(self.start_page, self.end_page + 1):
            yield self.create_page_request(page)
    
    def create_page_request(self, page):
        url = f"{self.base_api}?order=recommended&p={page}&storeid=1"
        return scrapy.Request(
            url,
            callback=self.parse_page,
            meta={'page': page},
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json, text/javascript, */*; q=0.01',
                'X-Requested-With': 'XMLHttpRequest',
                'Referer': 'https://colemanfurniture.com/ashley-furniture.html'
            },
            dont_filter=True
        )
    
    def parse_page(self, response):
        page = response.meta['page']
        
        if response.status != 200 or len(response.body) < 50:
            return
        
        try:
            data = response.json()
            data_obj = data.get('data', {})
            content = data_obj.get('content', {})
            
            if not content:
                return
            
            products = content.get('products', [])
            if isinstance(products, dict):
                products = [products]
            
            page_urls = []
            seen_on_page = set()
            
            for product in products:
                url = product.get('url')
                if url and isinstance(url, str) and url.strip():
                    url = url.strip().strip('"').strip("'")
                    
                    if not url.startswith(('http://', 'https://')):
                        if url.startswith('/'):
                            full_url = urljoin('https://colemanfurniture.com', url)
                        else:
                            full_url = urljoin('https://colemanfurniture.com/', url)
                    else:
                        full_url = url
                    
                    parsed = urlparse(full_url)
                    normalized_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}"
                    
                    if parsed.scheme and parsed.netloc:
                        if normalized_url not in seen_on_page:
                            seen_on_page.add(normalized_url)
                            page_urls.append(normalized_url)
                        else:
                            logger.debug(f"Duplicate main URL on same page {page}: {normalized_url}")
                    else:
                        logger.warning(f"Invalid main URL on page {page}: {url}")
                
                associated_bundles = product.get('associatedBundles', [])
                if isinstance(associated_bundles, list):
                    for bundle in associated_bundles:
                        if isinstance(bundle, dict):
                            bundle_url = bundle.get('url')
                            if bundle_url and isinstance(bundle_url, str) and bundle_url.strip():
                                bundle_url = bundle_url.strip().strip('"').strip("'")
                                
                                if not bundle_url.startswith(('http://', 'https://')):
                                    if bundle_url.startswith('/'):
                                        full_bundle_url = urljoin('https://colemanfurniture.com', bundle_url)
                                    else:
                                        full_bundle_url = urljoin('https://colemanfurniture.com/', bundle_url)
                                else:
                                    full_bundle_url = bundle_url
                                
                                parsed = urlparse(full_bundle_url)
                                normalized_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}"
                                
                                if parsed.scheme and parsed.netloc:
                                    if normalized_url not in seen_on_page:
                                        seen_on_page.add(normalized_url)
                                        page_urls.append(normalized_url)
                                        logger.debug(f"Found associated bundle URL: {normalized_url}")
                                    else:
                                        logger.debug(f"Duplicate bundle URL on same page {page}: {normalized_url}")
                                else:
                                    logger.warning(f"Invalid bundle URL on page {page}: {bundle_url}")
            
            new_urls_count = 0
            for url in page_urls:
                if url not in self.ashley_urls:
                    self.ashley_urls.add(url)
                    if self.url_list is not None:
                        self.url_list.append(url)
                    new_urls_count += 1
            
            logger.info(f"Page {page}: Found {len(page_urls)} valid products ({new_urls_count} new, {len(page_urls) - new_urls_count} already seen in previous pages)")
            
        except Exception as e:
            logger.error(f"Error on page {page}: {e}")
    
    def closed(self, reason):
        logger.info(f"Collected {len(self.ashley_urls)} Ashley product URLs from pages {self.start_page}-{self.end_page}")

def clean_url_string(url):
    """Clean individual URL string"""
    if not url or not isinstance(url, str):
        return None
    
    url = url.strip().strip('"').strip("'")
    
    if not url:
        return None
    
    if not url.startswith(('http://', 'https://')):
        if url.startswith('/'):
            url = f"https://colemanfurniture.com{url}"
        else:
            url = f"https://colemanfurniture.com/{url}"
    
    return url

def validate_urls_file(file_path):
    """Validate and clean URLs in the input file"""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        try:
            data = json.loads(content)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in file {file_path}: {e}")
            return []
        
        if isinstance(data, dict) and 'urls' in data:
            urls = data['urls']
            is_dict_format = True
        elif isinstance(data, list):
            urls = data
            is_dict_format = False
            data = {"urls": data, "total_urls": 0, "manufacturer_id": "250"}
        else:
            logger.error(f"Unexpected JSON structure in {file_path}")
            return []
        
        valid_urls = []
        for url in urls:
            cleaned_url = clean_url_string(url)
            if cleaned_url:
                parsed = urlparse(cleaned_url)
                if parsed.scheme and parsed.netloc and '.' in parsed.netloc:
                    valid_urls.append(cleaned_url)
                else:
                    logger.warning(f"Invalid URL after cleaning: '{url}' -> '{cleaned_url}'")
            else:
                logger.warning(f"Failed to clean URL: '{url}'")
        
        if is_dict_format:
            data['urls'] = valid_urls
            data['total_urls'] = len(valid_urls)
        else:
            data['urls'] = valid_urls
            data['total_urls'] = len(valid_urls)
        
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Validated URLs file: {len(valid_urls)} valid URLs (removed {len(urls) - len(valid_urls)} invalid)")
        
        if valid_urls:
            logger.info(f"Sample cleaned URL: {valid_urls[0]}")
        
        return valid_urls
        
    except Exception as e:
        logger.error(f"Error validating URLs file: {e}")
        import traceback
        traceback.print_exc()
        return []

def run_scraper_chunk(chunk_id, total_chunks, chunk_urls, output_dir, job_id, manufacturer_id, 
                     product_concurrency, sitemap_offset, max_sitemaps, max_urls_per_sitemap):
    chunk_output = f'{output_dir}/output_ashley_{manufacturer_id}_{job_id}_chunk_{chunk_id}.csv'
    logger.info(f"Chunk {chunk_id + 1}/{total_chunks}: Starting with {len(chunk_urls)} URLs -> {chunk_output}")
    settings = get_project_settings()
    settings.set('FEED_URI', chunk_output)
    settings.set('FEED_FORMAT', 'csv')
    settings.set('CONCURRENT_REQUESTS', product_concurrency)
    settings.set('CONCURRENT_REQUESTS_PER_DOMAIN', min(product_concurrency, 12))
    settings.set('DOWNLOAD_DELAY', 0.2)
    settings.set('RANDOMIZE_DOWNLOAD_DELAY', True)
    settings.set('DOWNLOAD_TIMEOUT', 30)
    settings.set('RETRY_ENABLED', True)
    settings.set('RETRY_TIMES', 1)
    settings.set('RETRY_HTTP_CODES', [405, 429, 500, 502, 503, 504, 400, 403, 404, 408])
    settings.set('COOKIES_ENABLED', True)
    settings.set('ROBOTSTXT_OBEY', False)
    settings.set('LOG_LEVEL', 'INFO')
    settings.set('LOG_STDOUT', True)
    settings.set('FEED_EXPORT_FIELDS', [
        'Ref Product URL',
        'Ref Product ID', 
        'Ref Variant ID',
        'Ref Category',
        'Ref Category URL',
        'Ref Brand Name',
        'Ref Product Name',
        'Ref SKU',
        'Ref MPN',
        'Ref GTIN',
        'Ref Price',
        'Ref Main Image',
        'Ref Quantity',
        'Ref Group Attr 1',
        'Ref Group Attr 2',
        'Ref Images',
        'Ref Dimensions',
        'Ref Status',
        'Ref Highlights',
        'Date Scrapped'
    ])
    settings.set('DUPEFILTER_CLASS', 'scrapy.dupefilters.RFPDupeFilter')
    
    process = CrawlerProcess(settings)
    process.crawl(ProductFetcher,
                 website_url="https://colemanfurniture.com",
                 ashley_urls=chunk_urls,
                 is_ashley=True,
                 chunk_mode=True,
                 chunk_id=chunk_id,
                 total_chunks=total_chunks,
                 sitemap_offset=sitemap_offset,
                 max_sitemaps=max_sitemaps,
                 max_urls_per_sitemap=max_urls_per_sitemap,
                 job_id=f"{job_id}_chunk_{chunk_id}")
    
    try:
        process.start()
        logger.info(f"Chunk {chunk_id + 1}/{total_chunks}: Completed -> {chunk_output}")
        return chunk_output
    except Exception as e:
        logger.error(f"Chunk {chunk_id + 1}/{total_chunks}: Failed - {e}")
        return None

def split_into_chunks(url_list, chunk_size):
    chunks = []
    for i in range(0, len(url_list), chunk_size):
        chunks.append(url_list[i:i + chunk_size])
    return chunks

def normalize_and_deduplicate():
    import sys
    import json
    from urllib.parse import urlparse
    
    def normalize_url(url):
        if not url:
            return None
        url = url.rstrip('/')
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    
    if len(sys.argv) < 4 or sys.argv[1] != '--deduplicate':
        print("Usage: python run_ashley_scraper.py --deduplicate input1.json input2.json ... --output output.json")
        return
    
    input_files = []
    output_file = None
    
    i = 2
    while i < len(sys.argv):
        if sys.argv[i] == '--output' and i + 1 < len(sys.argv):
            output_file = sys.argv[i + 1]
            i += 2
        else:
            input_files.append(sys.argv[i])
            i += 1
    
    if not input_files or not output_file:
        print("Error: Need input files and output file", file=sys.stderr)
        return
    
    all_urls = []
    manufacturer_id = "250"
    
    for file_path in input_files:
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                urls = data.get('urls', [])
                all_urls.extend(urls)
                if 'manufacturer_id' in data and manufacturer_id == "250":
                    manufacturer_id = data['manufacturer_id']
        except Exception as e:
            print(f"Error reading {file_path}: {e}", file=sys.stderr)
    
    print(f"Total URLs before deduplication: {len(all_urls)}", file=sys.stderr)
    
    url_map = {}
    for url in all_urls:
        if url:
            normalized = normalize_url(url)
            if normalized and normalized not in url_map:
                url_map[normalized] = url
    
    unique_urls = list(url_map.values())
    
    print(f"Unique URLs after normalization: {len(unique_urls)}", file=sys.stderr)
    print(f"Removed {len(all_urls) - len(unique_urls)} duplicates", file=sys.stderr)
    
    output_data = {
        "urls": unique_urls,
        "total_urls": len(unique_urls),
        "manufacturer_id": manufacturer_id
    }
    
    with open(output_file, 'w') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"Output written to {output_file}", file=sys.stderr)
    print(f"OUTPUT_FILE={output_file}")

def main():
    parser = argparse.ArgumentParser(description='Ashley Furniture Scraper')
    
    parser.add_argument('--manufacturer-id', default='250', help='Manufacturer ID for Ashley')
    parser.add_argument('--start-page', type=int, default=1, help='Start page number')
    parser.add_argument('--end-page', type=int, default=150, help='End page number')
    parser.add_argument('--chunk', type=int, default=0, help='Chunk ID for URL collection')
    parser.add_argument('--url-concurrency', type=int, default=20, help='Concurrent URL requests')
    
    parser.add_argument('--urls-file', help='JSON file containing Ashley URLs')
    parser.add_argument('--product-concurrency', type=int, default=12, help='Concurrent product requests per chunk')
    
    parser.add_argument('--product-chunks', type=int, default=1, help='Number of parallel chunks for product scraping')
    parser.add_argument('--chunk-size', type=int, default=0, help='Number of URLs per chunk (0 = auto-calculate)')
    
    parser.add_argument('--job-id', default='ashley', help='Job identifier')
    parser.add_argument('--output-dir', default='output', help='Output directory')
    parser.add_argument('--sitemap-offset', type=int, default=0, help='Sitemap offset (compatibility)')
    parser.add_argument('--max-sitemaps', type=int, default=0, help='Max sitemaps (compatibility)')
    parser.add_argument('--max-urls-per-sitemap', type=int, default=0, help='Max URLs per sitemap (compatibility)')
    
    args = parser.parse_args()
    if args.deduplicate:
        normalize_and_deduplicate()
        return
    os.makedirs(args.output_dir, exist_ok=True)
    
    if args.urls_file is None:
        logger.info("="*60) 
        logger.info(f"MODE: Collect Ashley URLs - Chunk {args.chunk}")
        logger.info(f"Pages: {args.start_page} - {args.end_page}")
        logger.info("="*60)
        
        url_list = []
        
        settings = {
            "LOG_LEVEL": "INFO",
            "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "CONCURRENT_REQUESTS": args.url_concurrency,
            "CONCURRENT_REQUESTS_PER_DOMAIN": args.url_concurrency,
            "DOWNLOAD_DELAY": 0.25,
            "COOKIES_ENABLED": False,
            "ROBOTSTXT_OBEY": False,
            "DOWNLOAD_TIMEOUT": 10,
            "RETRY_ENABLED": False,
        }
        
        process = CrawlerProcess(settings)
        process.crawl(AshleyURLSpider,
                     manufacturer_id=args.manufacturer_id,
                     start_page=args.start_page,
                     end_page=args.end_page,
                     url_list=url_list,
                     concurrent_pages=args.url_concurrency)
        process.start()
        
        valid_urls = []
        for url in url_list:
            cleaned_url = clean_url_string(url)
            if cleaned_url:
                parsed = urlparse(cleaned_url)
                if parsed.scheme and parsed.netloc and '.' in parsed.netloc:
                    valid_urls.append(cleaned_url)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f'{args.output_dir}/ashley_urls_chunk_{args.chunk}_{args.job_id}_{timestamp}.json'
        
        with open(output_file, 'w') as f:
            json.dump({
                "manufacturer_id": args.manufacturer_id,
                "chunk": args.chunk,
                "start_page": args.start_page,
                "end_page": args.end_page,
                "total_urls": len(valid_urls),
                "urls": valid_urls
            }, f, indent=2)
        
        logger.info(f"Saved {len(valid_urls)} valid URLs to {output_file}")
        
        if valid_urls:
            logger.info(f"Sample URL: {valid_urls[0]}")
        
        print(f"OUTPUT_FILE={output_file}")
    
    else:
        logger.info("="*60)
        logger.info(f"MODE: Scrape Ashley Products - PARALLEL CHUNKS")
        logger.info(f"URLs file: {args.urls_file}")
        logger.info(f"Parallel chunks: {args.product_chunks}")
        logger.info(f"URLs per chunk: {args.chunk_size if args.chunk_size > 0 else 'auto'}")
        logger.info(f"Concurrency per chunk: {args.product_concurrency}")
        logger.info("="*60)
        
        if not os.path.exists(args.urls_file):
            logger.error(f"URLs file not found: {args.urls_file}")
            sys.exit(1)
        
        all_ashley_urls = validate_urls_file(args.urls_file)
        
        if not all_ashley_urls:
            logger.error("No valid URLs found to scrape!")
            sys.exit(1)
        
        total_urls = len(all_ashley_urls)
        logger.info(f"Total URLs to scrape: {total_urls}")
        
        if args.product_chunks <= 1:
            logger.info("Running in single process mode...")
            
            timestamp = os.getenv('GITHUB_RUN_ID', 'local')
            domain = f"ashley_{args.manufacturer_id}"
            output_file = f'{args.output_dir}/output_{domain}_{args.job_id}_{timestamp}.csv'
            
            settings = get_project_settings()
            
            settings.set('FEED_URI', output_file)
            settings.set('FEED_FORMAT', 'csv')
            settings.set('CONCURRENT_REQUESTS', args.product_concurrency)
            settings.set('CONCURRENT_REQUESTS_PER_DOMAIN', min(args.product_concurrency, 12))
            settings.set('DOWNLOAD_DELAY', 0.2)
            settings.set('RANDOMIZE_DOWNLOAD_DELAY', True)
            settings.set('DOWNLOAD_TIMEOUT', 30)
            settings.set('RETRY_ENABLED', True)
            settings.set('RETRY_TIMES', 1)
            settings.set('COOKIES_ENABLED', True)
            settings.set('ROBOTSTXT_OBEY', False)
            settings.set('FEED_EXPORT_FIELDS', [
                'Ref Product URL', 'Ref Product ID', 'Ref Variant ID', 'Ref Category',
                'Ref Category URL', 'Ref Brand Name', 'Ref Product Name', 'Ref SKU',
                'Ref MPN', 'Ref GTIN', 'Ref Price', 'Ref Main Image', 'Ref Quantity',
                'Ref Group Attr 1', 'Ref Group Attr 2', 'Ref Images', 'Ref Dimensions',
                'Ref Status', 'Ref Highlights', 'Date Scrapped'
            ])
            settings.set('DUPEFILTER_CLASS', 'scrapy.dupefilters.RFPDupeFilter')
            
            process = CrawlerProcess(settings)
            process.crawl(ProductFetcher,
                         website_url="https://colemanfurniture.com",
                         ashley_urls=all_ashley_urls,
                         is_ashley=True,
                         chunk_mode=False,
                         sitemap_offset=args.sitemap_offset,
                         max_sitemaps=args.max_sitemaps,
                         max_urls_per_sitemap=args.max_urls_per_sitemap,
                         job_id=args.job_id)
            process.start()
            
            logger.info(f"Scraped {len(all_ashley_urls)} Ashley products to {output_file}")
            print(f"OUTPUT_FILE={output_file}")
            
        else:
            if args.chunk_size > 0:
                chunk_size = args.chunk_size
            else:
                chunk_size = total_urls // args.product_chunks
                if total_urls % args.product_chunks != 0:
                    chunk_size += 1
            
            chunks = split_into_chunks(all_ashley_urls, chunk_size)
            logger.info(f"Split into {len(chunks)} chunks of ~{chunk_size} URLs each")
            
            chunks = chunks[:args.product_chunks]
            logger.info(f"Processing {len(chunks)} chunks in parallel")
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            job_timestamp = f"{args.job_id}_{timestamp}"
            
            processes = []
            chunk_outputs = []
            
            start_time = time.time()
            
            for i, chunk_urls in enumerate(chunks):
                p = Process(target=run_scraper_chunk, args=(
                    i, len(chunks), chunk_urls, args.output_dir, job_timestamp, 
                    args.manufacturer_id, args.product_concurrency,
                    args.sitemap_offset, args.max_sitemaps, args.max_urls_per_sitemap
                ))
                processes.append(p)
                p.start()
                logger.info(f"Started process for chunk {i + 1}/{len(chunks)} ({len(chunk_urls)} URLs)")
                
                time.sleep(0.5)
            
            for i, p in enumerate(processes):
                p.join()
                logger.info(f"Completed process for chunk {i + 1}/{len(chunks)}")
            
            end_time = time.time()
            elapsed = end_time - start_time
            
            combined_output = f'{args.output_dir}/output_ashley_{args.manufacturer_id}_{args.job_id}_{timestamp}_combined.csv'
            
            logger.info("="*60)
            logger.info(f"Merging chunk outputs...")
            
            all_dfs = []
            total_products = 0
            
            try:
                import pandas as pd
                has_pandas = True
            except ImportError:
                has_pandas = False
                logger.warning("pandas not installed, skipping merge. Chunk files are available separately.")
            
            if has_pandas:
                for i in range(len(chunks)):
                    chunk_file = f'{args.output_dir}/output_ashley_{args.manufacturer_id}_{job_timestamp}_chunk_{i}.csv'
                    if os.path.exists(chunk_file):
                        try:
                            df = pd.read_csv(chunk_file)
                            all_dfs.append(df)
                            products_in_chunk = len(df)
                            total_products += products_in_chunk
                            logger.info(f"  + Chunk {i + 1}: {products_in_chunk} products")
                        except Exception as e:
                            logger.error(f"  - Failed to read chunk {i + 1}: {e}")
                
                if all_dfs:
                    combined_df = pd.concat(all_dfs, ignore_index=True)
                    combined_df.to_csv(combined_output, index=False)
                    logger.info(f"Combined {total_products} products into {combined_output}")
                else:
                    logger.error("No chunk outputs found!")
                    combined_output = None
            else:
                combined_output = None
                logger.info("Chunk files are available in the output directory:")
                for i in range(len(chunks)):
                    chunk_file = f'{args.output_dir}/output_ashley_{args.manufacturer_id}_{job_timestamp}_chunk_{i}.csv'
                    if os.path.exists(chunk_file):
                        logger.info(f"  - {chunk_file}")
            
            logger.info("="*60)
            logger.info(f"SCRAPE COMPLETED")
            logger.info(f"   Total URLs: {total_urls}")
            logger.info(f"   Parallel chunks: {len(chunks)}")
            logger.info(f"   Time elapsed: {elapsed:.2f} seconds ({elapsed/60:.2f} minutes)")
            if combined_output:
                logger.info(f"   Combined output: {combined_output}")
            logger.info("="*60)
            
            if combined_output:
                print(f"OUTPUT_FILE={combined_output}")

if __name__ == '__main__':
    multiprocessing.freeze_support()
    main()