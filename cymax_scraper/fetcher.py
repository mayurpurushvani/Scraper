from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import shutil
import json
import time
import random
import tempfile
import os

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

        temp_dir = tempfile.mkdtemp(prefix="chrome_")
        options.add_argument(f"--user-data-dir={temp_dir}")

        options.add_argument("--start-maximized")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-extensions")

        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)

        chromedriver_path = shutil.which("chromedriver")
        service = Service(chromedriver_path)

        driver = webdriver.Chrome(service=service, options=options)

        driver.execute_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
            window.chrome = { runtime: {} };
        """)
        return driver


    def fetch(self, url: str, retries=None) -> str:
        if retries is None:
            retries = config['scraping']['retry_attempts']
        
        log(f"[{retries} retries] Target: {url}")
        
        for attempt in range(1, retries + 1):
            driver = None
            try:
                driver = self._create_fresh_browser()
                print(driver)
                log(f"[{attempt}/{retries}] Fresh browser for: {url}")
                
                driver.get(url)
                time.sleep(random.uniform(
                    config['delays']['human_delay_min'], 
                    config['delays']['human_delay_max']
                ))
                
                html = driver.page_source
                log(f"[{attempt}/{retries}] Page size: {len(html)//1000}KB for {url}")
                
                blocks = ["sorry, you have been blocked", "cloudflare ray id", "checking your browser"]
                if any(block in html.lower() for block in blocks):
                    log(f"[{attempt}/{retries}] BLOCKED: {url}")
                    if driver:
                        driver.quit()
                    continue
                
                if len(html) > config['scraping']['min_html_size']:
                    print(len(html))
                    log(f"[{attempt}/{retries}] SUCCESS ({len(html)//1000}KB): {url}")
                    return html
                else:
                    log(f"[{attempt}/{retries}] Too small ({len(html)//1000}KB): {url}")
                
            except Exception as e:
                log(f"[{attempt}/{retries}] ERROR ({str(e)[:40]}): {url}")
            
            finally:
                if driver:
                    try:
                        driver.quit()
                    except:
                        pass
            
            if attempt < retries:
                log(f"[{attempt}/{retries}] Retrying in 3s: {url}")
                time.sleep(3)
        
        log(f"[{retries}x FAILED] {url}")
        return None

    def close(self):
        pass