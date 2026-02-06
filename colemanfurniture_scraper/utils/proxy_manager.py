import requests
from bs4 import BeautifulSoup
import concurrent.futures
import time
from typing import List, Tuple, Optional
import random
import logging

logger = logging.getLogger(__name__)

class ProxyManager:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.timeout = 10
        self.working_proxies = []
        self.last_proxy_fetch = 0
        self.proxy_cache_time = 300
    
    def _get_proxies_from_sources(self) -> List[str]:
        proxy_sources = [
            "https://free-proxy-list.net/",
            "https://www.sslproxies.org/",
            "https://www.us-proxy.org/",
            "https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all"
        ]
        
        all_proxies = []
        
        for url in proxy_sources:
            try:
                logger.debug(f"Scraping proxies from: {url}")
                response = requests.get(url, headers=self.headers, timeout=10)
                
                if "proxyscrape" in url:
                    proxies = response.text.strip().split('\r\n')
                    all_proxies.extend([f"http://{proxy}" for proxy in proxies if proxy])
                else:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    table = soup.find('table', {'id': 'proxylisttable'})
                    
                    if table:
                        rows = table.find_all('tr')[1:]
                        for row in rows:
                            cols = row.find_all('td')
                            if len(cols) >= 2:
                                ip = cols[0].text.strip()
                                port = cols[1].text.strip()
                                proxy_type = "https" if cols[6].text.strip() == "yes" else "http"
                                all_proxies.append(f"{proxy_type}://{ip}:{port}")
                
                time.sleep(1)
                
            except Exception as e:
                logger.debug(f"Failed to scrape {url}: {e}")
                continue
        
        unique_proxies = list(set(all_proxies))
        logger.info(f"Scraped {len(unique_proxies)} unique proxies")
        return unique_proxies
    
    def _test_proxy_speed(self, proxy: str, test_url: str = "http://httpbin.org/ip") -> Tuple[bool, float]:
        try:
            start_time = time.time()
            response = requests.get(
                test_url,
                proxies={"http": proxy, "https": proxy},
                timeout=self.timeout,
                headers=self.headers
            )
            response_time = time.time() - start_time
            
            if response.status_code == 200:
                logger.debug(f"Proxy {proxy} working, speed: {response_time:.2f}s")
                return True, response_time
            return False, 0
                
        except Exception as e:
            logger.debug(f"Proxy {proxy} failed: {e}")
            return False, 0
    
    def _find_fastest_proxy(self, proxies: List[str], target_url: str = None) -> Optional[str]:
        if not proxies:
            return None
        
        fastest_proxy = None
        fastest_time = float('inf')
        
        test_proxies = proxies[:20]
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_proxy = {
                executor.submit(self._test_proxy_speed, proxy, target_url or "http://httpbin.org/ip"): proxy 
                for proxy in test_proxies
            }
            
            for future in concurrent.futures.as_completed(future_to_proxy):
                proxy = future_to_proxy[future]
                try:
                    is_working, response_time = future.result()
                    if is_working and response_time < fastest_time:
                        fastest_time = response_time
                        fastest_proxy = proxy
                        logger.info(f"New fastest proxy: {proxy} ({response_time:.2f}s)")
                except Exception:
                    continue
        
        return fastest_proxy
    
    def get_proxy_for_homegallery(self) -> Optional[str]:
        current_time = time.time()
        if (current_time - self.last_proxy_fetch > self.proxy_cache_time or 
            len(self.working_proxies) < 5):
            
            logger.info("Fetching fresh proxies for HomeGallery...")
            all_proxies = self._get_proxies_from_sources()
            
            if not all_proxies:
                logger.warning("No proxies scraped")
                return None
            
            self.working_proxies = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures = {executor.submit(self._test_proxy_speed, proxy): proxy for proxy in all_proxies[:50]}
                
                for future in concurrent.futures.as_completed(futures):
                    proxy = futures[future]
                    try:
                        is_working, _ = future.result()
                        if is_working:
                            self.working_proxies.append(proxy)
                    except Exception:
                        continue
            
            self.last_proxy_fetch = current_time
            logger.info(f"Found {len(self.working_proxies)} working proxies")
        
        if not self.working_proxies:
            logger.warning("No working proxies available")
            return None
        
        homegallery_url = "https://homegallerystores.com"
        fastest_proxy = self._find_fastest_proxy(self.working_proxies, homegallery_url)
        
        if fastest_proxy:
            logger.info(f"Using proxy for HomeGallery: {fastest_proxy}")
            return fastest_proxy
        
        if self.working_proxies:
            proxy = random.choice(self.working_proxies)
            logger.info(f"Using random proxy: {proxy}")
            return proxy
        
        return None