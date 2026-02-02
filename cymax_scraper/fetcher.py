from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType
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
        options = Options()

        # GitHub Actions specific settings
        options.add_argument("--headless=new")  # Use new headless mode
        options.add_argument("--no-sandbox")    # Required for GitHub Actions
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        
        # Anti-detection settings
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-infobars")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-notifications")
        
        # User agent rotation
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ]
        options.add_argument(f"user-agent={random.choice(user_agents)}")
        
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)

        try:
            # Use webdriver-manager to automatically handle ChromeDriver
            log("Installing ChromeDriver via webdriver-manager...")
            service = Service(ChromeDriverManager(chrome_type=ChromeType.GOOGLE).install())
            driver = webdriver.Chrome(service=service, options=options)
            
        except Exception as e:
            log(f"Error with webdriver-manager: {e}")
            # Fallback: try to find Chrome and chromedriver in system
            try:
                # Set Chrome binary location
                chrome_paths = [
                    "/usr/bin/google-chrome-stable",
                    "/usr/bin/google-chrome",
                    "/usr/bin/chromium-browser",
                    "/opt/chrome/chrome"
                ]
                
                for path in chrome_paths:
                    if os.path.exists(path):
                        options.binary_location = path
                        break
                
                # Set ChromeDriver path
                chromedriver_paths = [
                    "/usr/local/bin/chromedriver",
                    "/usr/bin/chromedriver",
                    "/usr/lib/chromium-browser/chromedriver"
                ]
                
                chromedriver_path = None
                for path in chromedriver_paths:
                    if os.path.exists(path):
                        chromedriver_path = path
                        break
                
                if chromedriver_path:
                    service = Service(chromedriver_path)
                    driver = webdriver.Chrome(service=service, options=options)
                else:
                    # Last resort: let Selenium find it
                    driver = webdriver.Chrome(options=options)
                    
            except Exception as e2:
                log(f"All fallbacks failed: {e2}")
                raise Exception(f"Cannot create browser: {e2}")

        # Execute anti-detection scripts
        try:
            driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                "userAgent": random.choice(user_agents),
                "platform": "Linux"
            })
            
            driver.execute_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
                Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
            """)
        except:
            pass  # Ignore if CDP commands fail

        return driver

    def fetch(self, url: str, retries=None) -> str:
        if retries is None:
            retries = config['scraping']['retry_attempts']
        
        log(f"[{retries} retries] Target: {url}")
        
        for attempt in range(1, retries + 1):
            driver = None
            try:
                log(f"[{attempt}/{retries}] Creating browser for: {url}")
                driver = self._create_fresh_browser()
                
                # Set timeouts
                driver.set_page_load_timeout(30)
                driver.set_script_timeout(30)
                
                log(f"[{attempt}/{retries}] Navigating to: {url}")
                driver.get(url)
                
                # Add human-like delays
                time.sleep(random.uniform(3, 5))
                
                # Scroll a bit to trigger lazy loading
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 3);")
                time.sleep(random.uniform(1, 2))
                
                html = driver.page_source
                
                # Better block detection
                block_indicators = [
                    "sorry, you have been blocked",
                    "cloudflare ray id",
                    "checking your browser",
                    "access denied",
                    "security check",
                    "captcha",
                    "distil",
                    "incapsula",
                    "shield"
                ]
                
                html_lower = html.lower()
                if any(indicator in html_lower for indicator in block_indicators):
                    log(f"[{attempt}/{retries}] BLOCKED: {url}")
                    if driver:
                        driver.quit()
                    continue
                
                # Check if it's a listing page (not a product page)
                if 'id="products-list-page"' in html or 'class="product-list"' in html:
                    log(f"[{attempt}/{retries}] Listing page detected: {url}")
                    if driver:
                        driver.quit()
                    return None
                
                if len(html) > config['scraping']['min_html_size']:
                    log(f"[{attempt}/{retries}] SUCCESS ({len(html)//1000}KB): {url}")
                    driver.quit()
                    return html
                else:
                    log(f"[{attempt}/{retries}] Too small ({len(html)//1000}KB): {url}")
                
            except Exception as e:
                log(f"[{attempt}/{retries}] ERROR: {str(e)}")
                log(f"Traceback: {traceback.format_exc()}")
            
            finally:
                if driver:
                    try:
                        driver.quit()
                    except:
                        pass
            
            # Exponential backoff for retries
            if attempt < retries:
                wait_time = 2 ** attempt  # 2, 4, 8, etc.
                log(f"[{attempt}/{retries}] Retrying in {wait_time}s: {url}")
                time.sleep(wait_time)
        
        log(f"[{retries}x FAILED] {url}")
        return None

    def close(self):
        pass