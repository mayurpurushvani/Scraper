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
            # Try different approaches for HomeGallery
            if 'homegallerystores' in main_sitemap_url:
                # Approach 1: Try with session and cookies
                session = requests.Session()
                session.headers.update({
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'application/xml,text/xml,*/*',
                    'Accept-Encoding': 'gzip, deflate',
                    'Accept-Language': 'en-US,en;q=0.9',
                })
                
                response = session.get(main_sitemap_url, timeout=30)
                
                # If 403, try without .gz extension
                if response.status_code == 403 and main_sitemap_url.endswith('.gz'):
                    non_gz_url = main_sitemap_url[:-3]
                    logger.info(f"Trying without .gz: {non_gz_url}")
                    response = session.get(non_gz_url, timeout=30)
                
                if response.status_code != 200:
                    # Try one more time with different headers
                    logger.info("Trying with different headers...")
                    response = requests.get(
                        main_sitemap_url,
                        headers={
                            'User-Agent': 'Mozilla/5.0 (compatible; Bingbot/2.0; +http://www.bing.com/bingbot.htm)',
                            'Accept': 'application/xml,text/xml,*/*'
                        },
                        timeout=30
                    )
            else:
                # Regular approach for other sites
                response = requests.get(main_sitemap_url, timeout=30)
            
            if response.status_code != 200:
                raise Exception(f"Failed to fetch sitemap: Status {response.status_code}")
            
            content = response.content
            
            # Try to decompress if it's gzipped
            try:
                if main_sitemap_url.endswith('.gz') or response.headers.get('content-encoding') == 'gzip':
                    content = gzip.decompress(content)
            except (gzip.BadGzipFile, OSError):
                # Not a valid gzip file, use content as-is
                logger.debug("Content is not a valid gzip file, using as-is")
                pass
            
            # Try to parse with different namespace approaches
            root = ET.fromstring(content)
            
            # Try multiple namespace approaches
            namespace_attempts = [
                {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'},
                {'ns': ''},  # Empty namespace
                {},  # No namespace
            ]
            
            sitemaps = []
            for ns in namespace_attempts:
                try:
                    # Look for sitemap entries first
                    sitemap_elements = root.findall('.//ns:sitemap/ns:loc', ns) if 'ns' in ns else root.findall('.//sitemap/loc')
                    if sitemap_elements:
                        for sitemap in sitemap_elements:
                            if sitemap.text:
                                sitemaps.append(sitemap.text)
                        break
                    
                    # If no sitemap entries, look for url entries
                    url_elements = root.findall('.//ns:url/ns:loc', ns) if 'ns' in ns else root.findall('.//url/loc')
                    if url_elements:
                        for url in url_elements:
                            if url.text:
                                sitemaps.append(url.text)
                        break
                        
                except Exception as e:
                    logger.debug(f"Namespace attempt failed: {e}")
                    continue
            
            if not sitemaps:
                # Last resort: find all loc elements
                for loc in root.findall('.//loc'):
                    if loc.text:
                        sitemaps.append(loc.text)
                
                if not sitemaps:
                    sitemaps = [main_sitemap_url]
            
            logger.info(f"Extracted {len(sitemaps)} sitemaps/URLs")
            return sitemaps
            
        except Exception as e:
            logger.error(f"Failed to parse sitemap: {e}")
            # Re-raise for the spider to handle
            raise Exception(f"Failed to parse sitemap {main_sitemap_url}: {e}")
    
    @staticmethod
    def get_sitemap_chunks(all_sitemaps: List[str], offset: int, limit: int) -> List[str]:
        if limit == 0:
            chunk = all_sitemaps[offset:]
        else:
            chunk = all_sitemaps[offset:offset + limit]
        
        logger.info(f"Returning chunk: offset={offset}, limit={limit}, size={len(chunk)}")
        return chunk
