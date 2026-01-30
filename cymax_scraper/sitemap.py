import json
import time
import sys
import os
from bs4 import BeautifulSoup
from fetcher import Fetcher

def load_config():
    with open('config.json', 'r') as f:
        return json.load(f)

config = load_config()

def log(msg):
    sys.stderr.write(f"[SITEMAP {time.strftime('%H:%M:%S')}] {msg}\n")
    sys.stderr.flush()

def is_product_url(url):
    if not url or len(url) < 50:
        return False
    url_lower = url.lower()
    skip_words = ['sitemap', 'blog', 'news', 'about', 'contact', 'policy', 'privacy', 'xml']
    return (url_lower.endswith(('.html', '.htm')) and 
            'cymax.com' in url_lower and 
            not any(skip in url_lower for skip in skip_words))

def get_product_urls(limit=None, offset=0, max_sitemaps=None):
    config_limit = config['scraping']['sitemap_limit']
    if limit is None:
        limit = config_limit
    
    sitemap_offset = int(os.getenv("SITEMAP_OFFSET", "0"))
    max_sitemaps_env = int(os.getenv("MAX_SITEMAPS", "0"))
    
    log(f"Params: limit={limit}, offset={offset}, max_sitemaps={max_sitemaps}")
    log(f"Env: SITEMAP_OFFSET={sitemap_offset}, MAX_SITEMAPS={max_sitemaps_env}")
    
    fetcher = Fetcher()
    all_urls = set()
    sitemap_urls = config['sitemap']['urls']
    
    if offset > 0:
        log(f"Skipping first {offset} sitemaps")
        sitemap_urls = sitemap_urls[offset:]
    
    if max_sitemaps and max_sitemaps > 0:
        log(f"Limiting to {max_sitemaps} sitemaps")
        sitemap_urls = sitemap_urls[:max_sitemaps]
    
    total_sitemaps = len(sitemap_urls)
    log(f"Processing {total_sitemaps} sitemaps")
    
    for i, sm_url in enumerate(sitemap_urls):
        log(f"[{i+1}/{total_sitemaps}] Fetching sitemap: {sm_url}")
        html = fetcher.fetch(sm_url)
        
        if html and len(html) > 5000:
            soup = BeautifulSoup(html, 'lxml')
            loc_tags = soup.find_all('loc')
            log(f"Found {len(loc_tags)} <loc> tags")
            
            product_count = 0
            for loc in loc_tags:
                url = loc.get_text().strip()
                if is_product_url(url) and len(all_urls) < limit:
                    all_urls.add(url)
                    product_count += 1
            
            log(f"Extracted {product_count} products (total: {len(all_urls)})")
        else:
            log(f"Failed to fetch sitemap: {sm_url}")
        
        time.sleep(config['delays']['inter_request_delay'])
    
    fetcher.close()
    
    product_urls = list(all_urls)[:limit]
    
    log(f"FINAL: {len(product_urls)} product URLs ready")
    return product_urls

get_product_urls.__doc__ = """
Original signature preserved for backward compatibility.
New params: offset=0, max_sitemaps=None
"""

if __name__ == "__main__":
    urls = get_product_urls(limit=10, offset=0, max_sitemaps=1)
    print(f"Sample: {urls[:2]}")
    print(f"Total: {len(urls)}")
