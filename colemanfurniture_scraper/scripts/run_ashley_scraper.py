import os
import sys
import json
import argparse
import logging
from pathlib import Path
from urllib.parse import urljoin
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
                if url:
                    if not url.startswith('http'):
                        full_url = urljoin('https://colemanfurniture.com', url)
                    else:
                        full_url = url
                    page_urls.append(full_url)
            
            if page_urls:
                for url in page_urls:
                    self.ashley_urls.add(url)
                    if self.url_list is not None:
                        self.url_list.append(url)
                
                logger.info(f"Page {page}: Found {len(page_urls)} products (Total: {len(self.ashley_urls)})")
                
        except Exception as e:
            logger.error(f"Error on page {page}: {e}")
    
    def closed(self, reason):
        logger.info(f"✅ Collected {len(self.ashley_urls)} Ashley product URLs from pages {self.start_page}-{self.end_page}")

class AshleyProductFetcher(ProductFetcher):
    """Custom ProductFetcher that uses Ashley URLs instead of sitemaps"""
    
    def __init__(self, *args, **kwargs):
        self.ashley_urls = kwargs.pop('ashley_urls', [])
        super().__init__(*args, **kwargs)
    
    def start_requests(self):
        """Override to use Ashley URLs instead of sitemaps"""
        self.logger.info(f"Starting to process {len(self.ashley_urls)} Ashley product URLs")
        
        for url in self.ashley_urls:
            yield scrapy.Request(
                url,
                callback=self.parse_product_page_with_check,
                meta={'url': url},
                errback=self.handle_product_error,
                priority=5,
                dont_filter=True
            )

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
        
        # Save URLs to file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f'{args.output_dir}/ashley_urls_chunk_{args.chunk}_{args.job_id}_{timestamp}.json'
        
        with open(output_file, 'w') as f:
            json.dump({
                "manufacturer_id": args.manufacturer_id,
                "chunk": args.chunk,
                "start_page": args.start_page,
                "end_page": args.end_page,
                "total_urls": len(url_list),
                "urls": url_list
            }, f, indent=2)
        
        logger.info(f"✅ Saved {len(url_list)} URLs to {output_file}")
        print(f"OUTPUT_FILE={output_file}")
    
    # MODE 2: Scrape products from URLs file
    else:
        logger.info("="*60)
        logger.info(f"MODE: Scrape Ashley Products")
        logger.info(f"URLs file: {args.urls_file}")
        logger.info("="*60)
        
        # Load URLs
        with open(args.urls_file, 'r') as f:
            data = json.load(f)
            if 'urls' in data:
                ashley_urls = data['urls']
            elif isinstance(data, list):
                ashley_urls = data
            else:
                ashley_urls = []
        
        logger.info(f"Loaded {len(ashley_urls)} Ashley product URLs")
        
        if not ashley_urls:
            logger.error("No URLs found to scrape!")
            sys.exit(1)
        
        # Generate output filename (matching your run.py pattern)
        timestamp = os.getenv('GITHUB_RUN_ID', 'local')
        domain = f"ashley_{args.manufacturer_id}"
        output_file = f'{args.output_dir}/output_{domain}_{args.job_id}_{timestamp}.csv'
        
        # Get project settings
        settings = get_project_settings()
        
        # Configure settings (matching your run.py)
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
        
        # Run the custom ProductFetcher
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