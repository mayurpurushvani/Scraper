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
            
            is_gzipped = False
            if main_sitemap_url.endswith('.gz'):
                if len(content) >= 2 and content[:2] == b'\x1f\x8b':
                    try:
                        content = gzip.decompress(content)
                        is_gzipped = True
                        logger.info("Successfully decompressed gzipped sitemap")
                    except gzip.BadGzipFile:
                        logger.warning(f"URL has .gz extension but content is not gzipped: {main_sitemap_url}")
                        pass
                else:
                    logger.warning(f"URL has .gz extension but doesn't have gzip magic bytes: {main_sitemap_url}")
            
            try:
                root = ET.fromstring(content)
            except ET.ParseError as e:
                content_str = content.decode('utf-8', errors='ignore')
                if content_str.strip().startswith('<!'):
                    logger.error(f"Sitemap URL returned HTML instead of XML. Status: {response.status_code}")
                    logger.error(f"Response preview: {content_str[:200]}")
                    
                    base_url = '/'.join(main_sitemap_url.split('/')[:3])
                    alternative_sitemaps = [
                        f"{base_url}/sitemap.xml",
                        f"{base_url}/sitemap_index.xml",
                        f"{base_url}/sitemap/sitemap.xml",
                    ]
                    
                    for alt_sitemap in alternative_sitemaps:
                        try:
                            logger.info(f"Trying alternative: {alt_sitemap}")
                            alt_response = requests.get(alt_sitemap, timeout=10)
                            if alt_response.status_code == 200:
                                alt_content = alt_response.content
                                if alt_sitemap.endswith('.gz') and len(alt_content) >= 2 and alt_content[:2] == b'\x1f\x8b':
                                    alt_content = gzip.decompress(alt_content)
                                root = ET.fromstring(alt_content)
                                main_sitemap_url = alt_sitemap
                                logger.info(f"Found valid sitemap at: {alt_sitemap}")
                                break
                        except Exception:
                            continue
                    else:
                        raise Exception(f"Sitemap URL returned HTML (status: {response.status_code}). "
                                      f"Could be a 404 page, login page, or blocked by security.")
                
                else:
                    raise
            
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
                    if b'xml' not in content.lower():
                        urls = []
                        for line in content.decode('utf-8', errors='ignore').split('\n'):
                            line = line.strip()
                            if line and line.startswith('http'):
                                urls.append(line)
                        if urls:
                            logger.info(f"Found {len(urls)} URLs in text sitemap")
                            return urls
                    
                    sitemaps = [main_sitemap_url]
            
            logger.info(f"Extracted {len(sitemaps)} sitemaps/URLs")
            return sitemaps
            
        except Exception as e:
            logger.error(f"Failed to parse sitemap {main_sitemap_url}: {str(e)}")
            
            try:
                logger.info("Attempting fallback text extraction...")
                if 'content' in locals():
                    content_str = content.decode('utf-8', errors='ignore')
                    urls = []
                    for line in content_str.split('\n'):
                        line = line.strip()
                        if line and ('http://' in line or 'https://' in line):
                            import re
                            url_matches = re.findall(r'https?://[^\s<>"\']+', line)
                            urls.extend(url_matches)
                    
                    if urls:
                        logger.info(f"Fallback extracted {len(urls)} URLs")
                        return list(set(urls))
            except Exception as fallback_error:
                logger.error(f"Fallback also failed: {fallback_error}")
            
            raise Exception(f"Failed to parse sitemap {main_sitemap_url}: {str(e)}")
    
    @staticmethod
    def get_sitemap_chunks(all_sitemaps: List[str], offset: int, limit: int) -> List[str]:
        if limit == 0:
            chunk = all_sitemaps[offset:]
        else:
            chunk = all_sitemaps[offset:offset + limit]
        
        logger.info(f"Returning chunk: offset={offset}, limit={limit}, size={len(chunk)}")
        return chunk