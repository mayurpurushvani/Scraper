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
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "3"))

OUTPUT_CSV = f"cymax_chunk_{SITEMAP_OFFSET}.csv"
SCRAPED_DATE = datetime.now().strftime("%Y-%m-%d")

def log(msg):
    sys.stderr.write(f"[{time.strftime('%H:%M:%S')}] {msg}\n")
    sys.stderr.flush()

csv_lock = threading.Lock()
total_perfect = 0

def load_config():
    with open('config.json', 'r') as f:
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
        print(f"SAVED {len(df)} rows to {filename}")

def process_product(url, worker_id):
    global total_perfect
    short_url = url.split('/')[-1][:40]
    print(f"[{worker_id}] {short_url}")
    
    try:
        fetcher = Fetcher()
        html = fetcher.fetch(url)
        fetcher.close()
        
        if html and len(html) > 12000:
            data = parse_product(html, url)
            if is_perfect_data(data):
                append_to_csv(pd.DataFrame([data]), OUTPUT_CSV)
                total_perfect += 1
                print(f"#{total_perfect}: {data.get('Ref Product Name', '')[:50]}")
                return 1
            else:
                print(f"No data: {short_url}")
        else:
            print(f"Too small: {len(html)//1000 if html else 0}KB")
            
    except Exception as e:
        print(f"Error: {str(e)[:50]}")
    
    return 0

def main():
    log(f"Cymax scraper started - Chunk {SITEMAP_OFFSET}")
    log(f"URL: {CURR_URL}")
    
    all_urls = get_product_urls(limit=MAX_URLS_PER_SITEMAP, offset=SITEMAP_OFFSET, max_sitemaps=MAX_SITEMAPS)
    if not all_urls:
        log("NO URLS FOUND")
        sys.exit(1)
    
    log(f"Processing {len(all_urls)} URLs with {MAX_WORKERS} workers")
    
    # CSV Header
    with open(OUTPUT_CSV, 'w', encoding='utf-8') as f:
        f.write("Ref Product URL,Ref Product ID,Ref Varient ID,Ref Category,Ref Category URL,Ref Brand Name,Ref Product Name,Ref SKU,Ref MPN,Ref GTIN,Ref Price,Ref Main Image,Ref Quantity,Ref Group Attr 1,Ref Group Attr 2,Ref Status,Date Scrapped\n")
    
    # Process URLs
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_product, url, i+1): url 
                  for i, url in enumerate(all_urls)}
        
        completed = 0
        for future in as_completed(futures):
            completed += 1
            print(f"Progress: {total_perfect} saved | {completed}/{len(all_urls)} ({completed/len(all_urls)*100:.1f}%)")
    
    log(f"COMPLETE: {total_perfect} products → {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
