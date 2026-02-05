import requests
import xml.etree.ElementTree as ET
import gzip
from typing import List
from urllib.parse import urljoin
import logging

logger = logging.getLogger(__name__)

class SitemapProcessor:   
    @staticmethod
    def get_sitemap_from_robots(site_url: str) -> str:
        site_url = site_url.rstrip('/')
        robots_url = urljoin(site_url + '/', 'robots.txt')
        
        logger.info(f"Checking robots.txt at: {robots_url}")
        
        try:
            response = requests.get(robots_url, timeout=10)
            if response.status_code == 200:
                for line in response.text.split('\n'):
                    line = line.strip()
                    if line.lower().startswith('sitemap:'):
                        sitemap_url = line.split(':', 1)[1].strip()
                        logger.info(f"Found sitemap in robots.txt: {sitemap_url}")
                        return sitemap_url
        except Exception as e:
            logger.warning(f"Failed to get robots.txt: {e}")
        
        common_paths = [
            '/sitemap.xml',
            '/sitemap_index.xml',
            '/sitemap/sitemap.xml',
            '/sitemap/sitemap_index.xml',
            '/sitemap.xml.gz',
            '/sitemap_index.xml.gz',
            '/sitemap/hgs/sitemap_index.xml.gz',
            '/sitemap/hgs/sitemap_index.xml',
            '/hgs/sitemap_index.xml.gz',
        ]
        
        logger.info("Trying common sitemap paths...")
        for path in common_paths:
            sitemap_url = urljoin(site_url + '/', path.lstrip('/'))
            try:
                logger.debug(f"Trying: {sitemap_url}")
                response = requests.head(sitemap_url, timeout=5, allow_redirects=True)
                if response.status_code == 200:
                    content_type = response.headers.get('content-type', '').lower()
                    if any(x in content_type for x in ['xml', 'gzip', 'octet-stream']):
                        logger.info(f"Found sitemap at common path: {sitemap_url}")
                        return sitemap_url
            except Exception as e:
                logger.debug(f"Failed for {sitemap_url}: {e}")
                continue
        
        raise ValueError(f"No sitemap found for {site_url}")
    
    @staticmethod
    def extract_all_sitemaps(main_sitemap_url: str) -> List[str]:
        logger.info(f"Extracting sitemaps from: {main_sitemap_url}")
        
        try:
            response = requests.get(main_sitemap_url, timeout=30)
            content = response.content
            
            if main_sitemap_url.endswith('.gz'):
                content = gzip.decompress(content)
            
            root = ET.fromstring(content)
            ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
            
            sitemaps = []
            for sitemap in root.findall('ns:sitemap/ns:loc', ns):
                if sitemap.text:
                    sitemaps.append(sitemap.text)
            
            if not sitemaps:
                for url in root.findall('ns:url/ns:loc', ns):
                    if url.text:
                        sitemaps.append(url.text)
                
                if not sitemaps:
                    for sitemap in root.findall('sitemap/loc'):
                        if sitemap.text:
                            sitemaps.append(sitemap.text)
                    
                    if not sitemaps:
                        for url in root.findall('url/loc'):
                            if url.text:
                                sitemaps.append(url.text)
                    
                    if not sitemaps:
                        sitemaps = [main_sitemap_url]
            
            logger.info(f"Extracted {len(sitemaps)} sitemaps/URLs")
            return sitemaps
            
        except Exception as e:
            logger.error(f"Failed to parse sitemap {main_sitemap_url}: {e}")
            try:
                root = ET.fromstring(content)
                sitemaps = []
                for url in root.findall('.//loc'):
                    if url.text:
                        sitemaps.append(url.text)
                
                if sitemaps:
                    logger.info(f"Extracted {len(sitemaps)} URLs without namespace")
                    return sitemaps
            except:
                pass
            
            raise Exception(f"Failed to parse sitemap {main_sitemap_url}: {e}")
    
    @staticmethod
    def get_sitemap_chunks(all_sitemaps: List[str], offset: int, limit: int) -> List[str]:
        if limit == 0:
            chunk = all_sitemaps[offset:]
        else:
            chunk = all_sitemaps[offset:offset + limit]
        
        logger.info(f"Returning chunk: offset={offset}, limit={limit}, size={len(chunk)}")
        return chunk