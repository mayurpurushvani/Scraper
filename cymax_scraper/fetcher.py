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

PROXY_LIST = [
    "202.133.88.173:80",
    "213.142.156.97:80",
    "138.68.60.8:8080",
    "91.98.78.64:80",
    "212.47.232.28:80",
    "38.248.248.61:10002",
    "188.239.43.6:80",
    "50.203.147.152:80",
    "181.143.104.84:3000"
]

def create_proxy_extension(proxy_host, proxy_port):
    manifest_json = """
    {
        "version": "1.0.0",
        "manifest_version": 2,
        "name": "Chrome Proxy",
        "permissions": [
            "proxy",
            "tabs",
            "unlimitedStorage",
            "storage",
            "<all_urls>",
            "webRequest",
            "webRequestBlocking"
        ],
        "background": {
            "scripts": ["background.js"],
            "persistent": false
        },
        "minimum_chrome_version": "22.0.0"
    }
    """
    background_js = f"""
    var config = {{
        mode: "fixed_servers",
        rules: {{
            singleProxy: {{
                scheme: "http",
                host: "{proxy_host}",
                port: parseInt({proxy_port})
            }},
            bypassList: ["localhost"]
        }}
    }};

    chrome.proxy.settings.set({{value: config, scope: "regular"}}, function() {{}});

    chrome.webRequest.onAuthRequired.addListener(
        function() {{
            return {{cancel: false}};
        }},
        {{urls: ["<all_urls>"]}},
        ['blocking']
    );
    """

    extension_dir = tempfile.mkdtemp(prefix="proxy_ext_")
    
    with open(os.path.join(extension_dir, 'manifest.json'), 'w') as f:
        f.write(manifest_json)
    
    with open(os.path.join(extension_dir, 'background.js'), 'w') as f:
        f.write(background_js)
    
    return extension_dir

class Fetcher:
    def __init__(self):
        self.proxy_list = PROXY_LIST
        self.proxy_index = 0
        log(f"Loaded {len(self.proxy_list)} WORKING PROXIES - Ready for Cloudflare bypass!")

    def get_next_proxy(self):
        proxy = self.proxy_list[self.proxy_index % len(self.proxy_list)]
        self.proxy_index += 1
        host, port = proxy.split(':')
        log(f"Using proxy #{(self.proxy_index-1)%len(self.proxy_list)+1}: {proxy}")
        return host, int(port)

    def _create_fresh_browser(self):
        options = Options()
        
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        
        try:
            from selenium.webdriver.chrome.service import Service
            from webdriver_manager.chrome import ChromeDriverManager
            
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
        except Exception as e:
            log(f"Error with webdriver-manager: {e}")
            chromedriver_path = shutil.which("chromedriver")
            if not chromedriver_path:
                chromedriver_path = "/usr/bin/chromedriver"
            service = Service(chromedriver_path)
            driver = webdriver.Chrome(service=service, options=options)
        
        driver.execute_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
        """)       
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
                
                driver.set_page_load_timeout(30)
                
                log(f"[{attempt}/{retries}] Navigating to: {url}")
                driver.get(url)
                
                time.sleep(random.uniform(2, 4))
                
                if "Checking your browser" in driver.page_source:
                    log(f"[{attempt}/{retries}] Cloudflare challenge detected")
                    time.sleep(5)
                    driver.refresh()
                    time.sleep(random.uniform(2, 4))
                
                html = driver.page_source
                
                block_indicators = [
                    "sorry, you have been blocked",
                    "cloudflare ray id",
                    "checking your browser",
                    "access denied",
                    "security check"
                ]
                
                html_lower = html.lower()
                if any(indicator in html_lower for indicator in block_indicators):
                    log(f"[{attempt}/{retries}] BLOCKED: {url}")
                    if driver:
                        driver.quit()
                    continue
                
                if len(html) > config['scraping']['min_html_size']:
                    log(f"[{attempt}/{retries}] SUCCESS ({len(html)//1000}KB): {url}")
                    driver.quit()
                    return html
                else:
                    log(f"[{attempt}/{retries}] Too small ({len(html)//1000}KB): {url}")
                
            except Exception as e:
                log(f"[{attempt}/{retries}] ERROR: {str(e)}")
                import traceback
                log(f"Traceback: {traceback.format_exc()}")
            
            finally:
                if driver:
                    try:
                        driver.quit()
                    except:
                        pass
            
            if attempt < retries:
                wait_time = 2 ** attempt
                log(f"[{attempt}/{retries}] Retrying in {wait_time}s: {url}")
                time.sleep(wait_time)
        
        log(f"[{retries}x FAILED] {url}")
        return None

    def close(self):
        pass
