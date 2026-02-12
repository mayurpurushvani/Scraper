# scripts/run_ashley_scraper.py
import os
import sys
import json
import argparse
import logging
from pathlib import Path
from urllib.parse import urljoin, urlparse
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from fetcher.product_fetcher import ProductFetcher

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AshleyURLSpider(scrapy.Spider):
    """Fast parallel URL fetcher from manufacturer API"""
    name = "ashley_url_fetcher"
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.manufacturer_id = kwargs.get('manufacturer_id', '250')
        self.base_api = f"https://colemanfurniture.com/manufacturer/detail/{self.manufacturer_id}"
        self.ashley_urls = set()
        self.start_page = int(kwargs.get('start_page', 1))
        self.end_page = int(kwargs.get('end_page', 1000))
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
            for product in products:
                url = product.get('url')
                if url and isinstance(url, str) and url.strip():
                    # Clean the URL - remove any quotes
                    url = url.strip().strip('"').strip("'")
                    
                    # Fix relative URLs
                    if not url.startswith(('http://', 'https://')):
                        if url.startswith('/'):
                            full_url = urljoin('https://colemanfurniture.com', url)
                        else:
                            full_url = urljoin('https://colemanfurniture.com/', url)
                    else:
                        full_url = url
                    
                    # Validate the URL
                    parsed = urlparse(full_url)
                    if parsed.scheme and parsed.netloc:
                        page_urls.append(full_url)
                    else:
                        logger.warning(f"Invalid URL on page {page}: {url}")
            
            if page_urls:
                for url in page_urls:
                    self.ashley_urls.add(url)
                    if self.url_list is not None:
                        self.url_list.append(url)
                
                logger.info(f"Page {page}: Found {len(page_urls)} valid products (Total: {len(self.ashley_urls)})")
                
        except Exception as e:
            logger.error(f"Error on page {page}: {e}")
    
    def closed(self, reason):
        logger.info(f"✅ Collected {len(self.ashley_urls)} Ashley product URLs from pages {self.start_page}-{self.end_page}")

class AshleyProductFetcher(ProductFetcher):
    """Custom ProductFetcher that uses Ashley URLs instead of sitemaps"""
    
    def __init__(self, *args, **kwargs):
        self.ashley_urls = kwargs.pop('ashley_urls', [])
        super().__init__(*args, **kwargs)
    
    def clean_url(self, url):
        """Clean and validate URL"""
        if not url or not isinstance(url, str):
            return None
        
        # Remove whitespace and quotes
        url = url.strip().strip('"').strip("'")
        
        # Skip empty URLs
        if not url:
            return None
        
        # Ensure URL has scheme
        if not url.startswith(('http://', 'https://')):
            if url.startswith('/'):
                url = f"https://colemanfurniture.com{url}"
            else:
                url = f"https://colemanfurniture.com/{url}"
        
        # Validate URL format
        parsed = urlparse(url)
        if parsed.scheme and parsed.netloc:
            return url
        else:
            return None
    
    def start_requests(self):
        """Override to use Ashley URLs instead of sitemaps"""
        # Filter and validate URLs before sending requests
        valid_urls = []
        for url in self.ashley_urls:
            cleaned_url = self.clean_url(url)
            if cleaned_url:
                valid_urls.append(cleaned_url)
            else:
                logger.warning(f"Skipping invalid URL format: {url}")
        
        self.logger.info(f"Starting to process {len(valid_urls)} valid Ashley product URLs (filtered from {len(self.ashley_urls)} total)")
        
        for url in valid_urls:
            yield scrapy.Request(
                url,
                callback=self.parse_product_page_with_check,
                meta={'url': url},
                errback=self.handle_product_error,
                priority=5,
                dont_filter=True
            )
    
    def handle_product_error(self, failure):
        """Handle request failures"""
        url = failure.request.meta.get('url', 'Unknown')
        self.logger.error(f"Product page request failed for {url}: {failure.value}")

def clean_url_string(url):
    """Clean individual URL string"""
    if not url or not isinstance(url, str):
        return None
    
    # Remove whitespace and quotes
    url = url.strip().strip('"').strip("'")
    
    # Skip empty URLs
    if not url:
        return None
    
    # Ensure URL has scheme
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
        
        # Try to parse JSON
        try:
            data = json.loads(content)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in file {file_path}: {e}")
            return []
        
        # Extract URLs based on structure
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
        
        # Clean and validate URLs
        valid_urls = []
        for url in urls:
            cleaned_url = clean_url_string(url)
            if cleaned_url:
                # Additional validation - check if it's a proper URL
                parsed = urlparse(cleaned_url)
                if parsed.scheme and parsed.netloc and '.' in parsed.netloc:
                    valid_urls.append(cleaned_url)
                else:
                    logger.warning(f"Invalid URL after cleaning: '{url}' -> '{cleaned_url}'")
            else:
                logger.warning(f"Failed to clean URL: '{url}'")
        
        # Update the data with cleaned URLs
        if is_dict_format:
            data['urls'] = valid_urls
            data['total_urls'] = len(valid_urls)
        else:
            data['urls'] = valid_urls
            data['total_urls'] = len(valid_urls)
        
        # Write back cleaned URLs
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"✅ Validated URLs file: {len(valid_urls)} valid URLs (removed {len(urls) - len(valid_urls)} invalid)")
        
        # Show sample of cleaned URLs
        if valid_urls:
            logger.info(f"Sample cleaned URL: {valid_urls[0]}")
        
        return valid_urls
        
    except Exception as e:
        logger.error(f"Error validating URLs file: {e}")
        import traceback
        traceback.print_exc()
        return []

def main():
    parser = argparse.ArgumentParser(description='Ashley Furniture Scraper')
    
    # URL collection parameters
    parser.add_argument('--manufacturer-id', default='250', help='Manufacturer ID for Ashley')
    parser.add_argument('--start-page', type=int, default=1, help='Start page number')
    parser.add_argument('--end-page', type=int, default=1000, help='End page number')
    parser.add_argument('--chunk', type=int, default=0, help='Chunk ID for parallel processing')
    parser.add_argument('--url-concurrency', type=int, default=20, help='Concurrent URL requests')
    
    # Product scraping parameters
    parser.add_argument('--urls-file', help='JSON file containing Ashley URLs')
    parser.add_argument('--product-concurrency', type=int, default=32, help='Concurrent product requests')
    
    # Common parameters
    parser.add_argument('--job-id', default='ashley', help='Job identifier')
    parser.add_argument('--output-dir', default='output', help='Output directory')
    parser.add_argument('--sitemap-offset', type=int, default=0, help='Sitemap offset (compatibility)')
    parser.add_argument('--max-sitemaps', type=int, default=0, help='Max sitemaps (compatibility)')
    parser.add_argument('--max-urls-per-sitemap', type=int, default=0, help='Max URLs per sitemap (compatibility)')
    
    args = parser.parse_args()
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # MODE 1: Collect URLs only
    if args.urls_file is None:
        logger.info("="*60)
        logger.info(f"MODE: Collect Ashley URLs - Chunk {args.chunk}")
        logger.info(f"Pages: {args.start_page} - {args.end_page}")
        logger.info("="*60)
        
        # Collect URLs
        url_list = []
        
        settings = {
            "LOG_LEVEL": "INFO",
            "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "CONCURRENT_REQUESTS": args.url_concurrency,
            "CONCURRENT_REQUESTS_PER_DOMAIN": args.url_concurrency,
            "DOWNLOAD_DELAY": 0.1,
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
        
        # Clean and validate collected URLs
        valid_urls = []
        for url in url_list:
            cleaned_url = clean_url_string(url)
            if cleaned_url:
                parsed = urlparse(cleaned_url)
                if parsed.scheme and parsed.netloc and '.' in parsed.netloc:
                    valid_urls.append(cleaned_url)
        
        # Save URLs to file
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
        
        logger.info(f"✅ Saved {len(valid_urls)} valid URLs to {output_file} (removed {len(url_list) - len(valid_urls)} invalid)")
        
        # Show sample of saved URLs
        if valid_urls:
            logger.info(f"Sample URL: {valid_urls[0]}")
        
        print(f"OUTPUT_FILE={output_file}")
    
    # MODE 2: Scrape products from URLs file
    else:
        logger.info("="*60)
        logger.info(f"MODE: Scrape Ashley Products")
        logger.info(f"URLs file: {args.urls_file}")
        logger.info("="*60)
        
        # First, check if file exists
        if not os.path.exists(args.urls_file):
            logger.error(f"URLs file not found: {args.urls_file}")
            sys.exit(1)
        
        # Show raw file content for debugging
        try:
            with open(args.urls_file, 'r') as f:
                raw_content = f.read()
            logger.debug(f"Raw file content (first 200 chars): {raw_content[:200]}")
        except Exception as e:
            logger.error(f"Could not read file: {e}")
        
        # Validate and clean URLs file
        ashley_urls = validate_urls_file(args.urls_file)
        
        if not ashley_urls:
            logger.error("No valid URLs found to scrape!")
            sys.exit(1)
        
        logger.info(f"First 5 valid URLs: {ashley_urls[:5]}")
        
        # Generate output filename
        timestamp = os.getenv('GITHUB_RUN_ID', 'local')
        domain = f"ashley_{args.manufacturer_id}"
        output_file = f'{args.output_dir}/output_{domain}_{args.job_id}_{timestamp}.csv'
        
        # Get project settings
        settings = get_project_settings()
        
        # Configure settings
        settings.set('FEED_URI', output_file)
        settings.set('FEED_FORMAT', 'csv')
        settings.set('CONCURRENT_REQUESTS', args.product_concurrency)
        settings.set('DOWNLOAD_DELAY', 0.5)
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
        
        # Run the custom ProductFetcher with validated URLs
        process = CrawlerProcess(settings)
        process.crawl(AshleyProductFetcher,
                     website_url="https://colemanfurniture.com",
                     ashley_urls=ashley_urls,
                     sitemap_offset=args.sitemap_offset,
                     max_sitemaps=args.max_sitemaps,
                     max_urls_per_sitemap=args.max_urls_per_sitemap,
                     job_id=args.job_id)
        process.start()
        
        logger.info(f"✅ Scraped {len(ashley_urls)} Ashley products to {output_file}")
        print(f"OUTPUT_FILE={output_file}")

if __name__ == '__main__':
    main()
