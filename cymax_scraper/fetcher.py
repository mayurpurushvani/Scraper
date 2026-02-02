from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import json
import time
import random
import os
import traceback

def load_config():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'config.json')
    with open(config_path, 'r') as f:
        return json.load(f)

config = load_config()

def log(msg):
    print(f"[FETCHER] {msg}")

class Fetcher:
    def __init__(self):
        self.driver = None

    def _create_fresh_browser(self):
        """Create a fresh browser instance optimized for GitHub Actions"""
        options = Options()

        # Headless mode for GitHub Actions
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        
        # Anti-detection
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-infobars")
        options.add_argument("--disable-extensions")
        
        # Random user agent
        user_agents = [
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
        ]
        options.add_argument(f"user-agent={random.choice(user_agents)}")
        
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)

        try:
            # Use the ChromeDriver installed in the workflow
            service = Service('/usr/local/bin/chromedriver')
            driver = webdriver.Chrome(service=service, options=options)
            
            # Execute anti-detection scripts
            driver.execute_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
                Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
            """)
            
            log("✓ Browser created successfully")
            return driver
            
        except Exception as e:
            log(f"Error creating browser: {e}")
            
            # Fallback: try without service
            try:
                driver = webdriver.Chrome(options=options)
                log("✓ Browser created with fallback method")
                return driver
            except Exception as e2:
                log(f"Fallback also failed: {e2}")
                raise

    def fetch(self, url: str, retries=None) -> str:
        """Fetch HTML content from URL with retry logic"""
        if retries is None:
            retries = config['scraping']['retry_attempts']
        
        log(f"Fetching: {url} (max {retries} retries)")
        
        for attempt in range(1, retries + 1):
            driver = None
            try:
                log(f"[{attempt}/{retries}] Creating browser...")
                driver = self._create_fresh_browser()
                
                # Set reasonable timeouts
                driver.set_page_load_timeout(30)
                driver.set_script_timeout(20)
                
                log(f"[{attempt}/{retries}] Loading: {url}")
                driver.get(url)
                
                # Random delay to appear human
                delay = random.uniform(
                    config['delays']['human_delay_min'],
                    config['delays']['human_delay_max']
                )
                time.sleep(delay)
                
                # Scroll to trigger lazy loading
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
                time.sleep(1)
                
                html = driver.page_source
                
                # Check for blocks
                if any(block in html.lower() for block in [
                    "sorry, you have been blocked",
                    "cloudflare ray id",
                    "checking your browser",
                    "access denied"
                ]):
                    log(f"[{attempt}/{retries}] Block detected")
                    raise Exception("Blocked by website")
                
                # Check if it's a product page
                if 'id="products-list-page"' in html or 'class="product-list"' in html:
                    log(f"[{attempt}/{retries}] Listing page, skipping")
                    return None
                
                if len(html) > config['scraping']['min_html_size']:
                    log(f"[{attempt}/{retries}] Success ({len(html)//1000}KB)")
                    return html
                else:
                    log(f"[{attempt}/{retries}] HTML too small ({len(html)//1000}KB)")
                    
            except Exception as e:
                log(f"[{attempt}/{retries}] Error: {str(e)[:80]}")
                
            finally:
                if driver:
                    try:
                        driver.quit()
                    except:
                        pass
            
            # Wait before retry (exponential backoff)
            if attempt < retries:
                wait_time = min(2 ** attempt, 30)  # Cap at 30 seconds
                log(f"[{attempt}/{retries}] Retrying in {wait_time}s...")
                time.sleep(wait_time)
        
        log(f"Failed after {retries} attempts: {url}")
        return None

    def close(self):
        """Clean up resources"""
        pass
