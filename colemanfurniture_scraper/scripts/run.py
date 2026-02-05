import os
import sys
import argparse
import logging
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from fetcher.product_fetcher import ProductFetcher

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description='Run Coleman and Homegallerystores product scraper')
    
    parser.add_argument('--website-url', required=True, 
                       help='Website URL to scrape (e.g., https://colemanfurniture.com)')
    sitemap_offset = os.getenv('SITEMAP_OFFSET', '0')
    max_sitemaps = os.getenv('MAX_SITEMAPS', '0')
    max_urls_per_sitemap = os.getenv('MAX_URLS_PER_SITEMAP', '0')
    max_workers = os.getenv('MAX_WORKERS', '16')
    job_id = os.getenv('GITHUB_JOB', '')
    parser.add_argument('--sitemap-offset', type=int, default=int(sitemap_offset),
                       help='Offset for sitemap processing')
    parser.add_argument('--max-sitemaps', type=int, default=int(max_sitemaps),
                       help='Maximum sitemaps to process (0 for all)')
    parser.add_argument('--max-urls-per-sitemap', type=int, default=int(max_urls_per_sitemap),
                       help='Maximum URLs per sitemap (0 for all)')
    parser.add_argument('--job-id', default='job_id',
                       help='Job identifier for output file')
    parser.add_argument('--output-dir', default='output',
                       help='Output directory for CSV files')
    
    args = parser.parse_args()
    
    os.makedirs(args.output_dir, exist_ok=True)
    
    settings = get_project_settings()
    
    timestamp = os.getenv('GITHUB_RUN_ID', 'local')
    domain = args.website_url.replace('https://', '').replace('http://', '').split('/')[0].replace('.', '_')
    output_file = f'{args.output_dir}/output_{domain}_{args.job_id}_{timestamp}.csv'
    settings.set('FEED_URI', output_file)
    settings.set('FEED_FORMAT', 'csv')
    
    max_workers = int(os.getenv('MAX_WORKERS', '16'))
    settings.set('CONCURRENT_REQUESTS', max_workers)
    
    download_delay = float(os.getenv('DOWNLOAD_DELAY', '0.5'))
    settings.set('DOWNLOAD_DELAY', download_delay)
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
        'Ref Status',
        'Date Scrapped'
    ])
    process = CrawlerProcess(settings)
    
    logger.info(f"Starting scraper for: {args.website_url}")
    logger.info(f"Output will be saved to: {output_file}")
    logger.info(f"Job parameters: offset={args.sitemap_offset}, max_sitemaps={args.max_sitemaps}")
    
    process.crawl(ProductFetcher,
                  website_url=args.website_url,
                  sitemap_offset=args.sitemap_offset,
                  max_sitemaps=args.max_sitemaps,
                  max_urls_per_sitemap=args.max_urls_per_sitemap,
                  job_id=args.job_id)
    process.start()
    logger.info(f"Scraping completed. Output saved to: {output_file}")
    return output_file

if __name__ == '__main__':
    main()