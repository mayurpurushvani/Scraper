import os
import sys
import time
import pandas as pd
import json
import gc
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from sitemap import get_product_urls
from fetcher import Fetcher
from parser import parse_product

CURR_URL = os.getenv("CURR_URL", "https://www.cymax.com").rstrip("/")
SITEMAP_OFFSET = int(os.getenv("SITEMAP_OFFSET", "0"))
MAX_SITEMAPS = int(os.getenv("MAX_SITEMAPS", "1"))
MAX_URLS_PER_SITEMAP = int(os.getenv("MAX_URLS_PER_SITEMAP", "500"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "2"))  # Reduced for GitHub Actions

OUTPUT_CSV = f"cymax_chunk_{SITEMAP_OFFSET}.csv"
SCRAPED_DATE = datetime.now().strftime("%Y-%m-%d")

def log(msg):
    sys.stderr.write(f"[{time.strftime('%H:%M:%S')}] {msg}\n")
    sys.stderr.flush()

csv_lock = threading.Lock()
total_perfect = 0
processed_urls = 0

def load_config():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'config.json')
    with open(config_path, 'r') as f:
        return json.load(f)

config = load_config()

def is_perfect_data(data):
    name = data.get('Ref Product Name', '').strip()
    return len(name) > config['product_validation']['min_name_length']

def append_to_csv(df, filename):
    with csv_lock:
        if df.empty:
            return
        file_exists = os.path.exists(filename)
        df.to_csv(filename, mode='a', index=False, header=not file_exists)
        log(f"SAVED {len(df)} rows to {filename}")

def process_product(url, worker_id):
    global total_perfect, processed_urls
    
    short_url = url.split('/')[-1][:40] if len(url) > 40 else url
    
    try:
        log(f"[Worker {worker_id}] Processing: {short_url}")
        
        fetcher = Fetcher()
        html = fetcher.fetch(url)
        fetcher.close()

        if not html:
            log(f"[Worker {worker_id}] No HTML for: {short_url}")
            return 0
            
        # Check if it's a listing page
        if '<div id="products-list-page"' in html or 'id="products-list-page"' in html:
            log(f"[Worker {worker_id}] SKIP LISTING: {short_url}")
            return 0

        if html and len(html) > 12000:
            data = parse_product(html, url)
            if data and is_perfect_data(data):
                df = pd.DataFrame([data])
                append_to_csv(df, OUTPUT_CSV)
                total_perfect += 1
                log(f"[Worker {worker_id}] #{total_perfect}: {data.get('Ref Product Name', '')[:50]}")
                return 1
            else:
                log(f"[Worker {worker_id}] No valid data: {short_url}")
        else:
            log(f"[Worker {worker_id}] Too small: {len(html)//1000 if html else 0}KB")
            
    except Exception as e:
        log(f"[Worker {worker_id}] Error: {str(e)[:100]}")
    
    finally:
        with csv_lock:
            processed_urls += 1
    
    return 0

def main():
    log(f"Cymax scraper started - Chunk {SITEMAP_OFFSET}")
    log(f"URL: {CURR_URL}")
    log(f"Workers: {MAX_WORKERS}")
    
    all_urls = get_product_urls(limit=MAX_URLS_PER_SITEMAP, offset=SITEMAP_OFFSET, max_sitemaps=MAX_SITEMAPS)
    if not all_urls:
        log("NO URLS FOUND")
        sys.exit(1)
    
    log(f"Processing {len(all_urls)} URLs with {MAX_WORKERS} workers")
    
    # Create CSV with header
    header_df = pd.DataFrame(columns=[
        'Ref Product URL', 'Ref Product ID', 'Ref Varient ID', 'Ref Category', 'Ref Category URL',
        'Ref Brand Name', 'Ref Product Name', 'Ref SKU', 'Ref MPN', 'Ref GTIN', 'Ref Price',
        'Ref Main Image', 'Ref Quantity', 'Ref Group Attr 1', 'Ref Group Attr 2', 'Ref Status',
        'Date Scrapped'
    ])
    header_df.to_csv(OUTPUT_CSV, index=False)
    
    # Process URLs with limited concurrency
    successful = 0
    failed = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for i, url in enumerate(all_urls):
            # Submit tasks with rate limiting
            future = executor.submit(process_product, url, (i % MAX_WORKERS) + 1)
            futures[future] = url
            
            # Small delay between submissions
            if i % 5 == 0:
                time.sleep(0.5)
        
        # Process results as they complete
        for future in as_completed(futures):
            try:
                result = future.result()
                if result == 1:
                    successful += 1
                else:
                    failed += 1
                    
                progress = successful + failed
                log(f"Progress: {successful} saved | {failed} failed | {progress}/{len(all_urls)} ({progress/len(all_urls)*100:.1f}%)")
                
            except Exception as e:
                log(f"Future error: {e}")
                failed += 1
    
    # Summary
    log(f"COMPLETE: {successful} successful, {failed} failed → {OUTPUT_CSV}")
    
    # Final file check
    if os.path.exists(OUTPUT_CSV):
        try:
            df = pd.read_csv(OUTPUT_CSV)
            log(f"Final CSV has {len(df)} rows")
        except:
            log("Could not read final CSV")
    else:
        log("No output CSV created")

if __name__ == "__main__":
    main()