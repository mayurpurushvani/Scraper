import gzip
import xml.etree.ElementTree as ET
import json
import re
import random
from datetime import datetime
from urllib.parse import urlparse, urljoin
from scrapy import Spider, Request
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    from utils.sitemap_processor import SitemapProcessor
except ImportError:
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class ProductFetcher(Spider):
    name = 'product'
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Ashley mode flags
        self.is_ashley = kwargs.get('is_ashley', False)
        self.ashley_urls = kwargs.get('ashley_urls', [])
        
        # UNIVERSAL CHUNK PROCESSING PARAMETERS
        self.chunk_id = kwargs.get('chunk_id', 0)
        self.total_chunks = kwargs.get('total_chunks', 1)
        self.chunk_mode = kwargs.get('chunk_mode', False)
        self.chunk_size = kwargs.get('chunk_size', 0)  # 0 means auto-calculate
        
        self.website_url = kwargs.get('website_url')
        if not self.website_url:
            raise ValueError("website_url parameter is required")
        
        self.sitemap_offset = int(kwargs.get('sitemap_offset', 0))
        self.max_sitemaps = int(kwargs.get('max_sitemaps', 0))
        self.max_urls_per_sitemap = int(kwargs.get('max_urls_per_sitemap', 0))
        self.job_id = kwargs.get('job_id', '')
        
        parsed_url = urlparse(self.website_url)
        self.domain = parsed_url.netloc
        self.base_domain = '.'.join(self.domain.split('.')[-2:]).replace('.', '_')
        
        # Cache for JSON-LD parsing
        self.json_ld_cache = {}
        self.json_ld_xpath = '//script[@type="application/ld+json"]/text()'
        
        # Rotating user agents
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
        ]
        
        # Bundle URL deduplication
        self._processed_bundle_urls = set()
        
        # Log chunk info if in chunk mode
        if self.chunk_mode:
            self.logger.info(f"📦 Chunk {self.chunk_id + 1}/{self.total_chunks} initialized for {self.website_url}")
        
        # Skip sitemap discovery for Ashley mode
        if not self.is_ashley:
            try:
                sitemap_processor = SitemapProcessor()
                self.sitemap_index_url = sitemap_processor.get_sitemap_from_robots(self.website_url)
                self.logger.info(f"Found sitemap index: {self.sitemap_index_url}")
                
                self.all_sitemaps = sitemap_processor.extract_all_sitemaps(self.sitemap_index_url)
                self.logger.info(f"Total sitemaps discovered: {len(self.all_sitemaps)}")
                
                # APPLY CHUNK LOGIC TO SITEMAPS IF IN CHUNK MODE
                if self.chunk_mode and self.total_chunks > 1:
                    # Split sitemaps into chunks
                    chunk_size = len(self.all_sitemaps) // self.total_chunks
                    if self.chunk_size > 0:
                        chunk_size = self.chunk_size
                    
                    start_idx = self.chunk_id * chunk_size
                    
                    if self.chunk_id == self.total_chunks - 1:
                        self.sitemap_chunk = self.all_sitemaps[start_idx:]
                    else:
                        self.sitemap_chunk = self.all_sitemaps[start_idx:start_idx + chunk_size]
                    
                    self.logger.info(f"📦 Chunk {self.chunk_id + 1}/{self.total_chunks}: Processing {len(self.sitemap_chunk)} sitemaps")
                else:
                    self.sitemap_chunk = sitemap_processor.get_sitemap_chunks(
                        self.all_sitemaps, 
                        self.sitemap_offset, 
                        self.max_sitemaps
                    )
                    self.logger.info(f"This job will process {len(self.sitemap_chunk)} sitemaps")
                
            except Exception as e:
                self.logger.error(f"Failed to discover sitemap: {e}")
                raise
    
    def get_headers(self, referer=None):
        """Generate random browser headers"""
        headers = {
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
        }
        
        if referer:
            headers['Referer'] = referer
        
        return headers
          
    def start_requests(self):
        """Handle both Ashley mode with direct URLs and sitemap mode with chunk support"""
        
        # ASHLEY MODE with direct URLs
        if self.is_ashley:
            # Filter URLs for this chunk if in chunk mode
            urls_to_process = self.ashley_urls
            
            if self.chunk_mode and self.total_chunks > 1:
                # Calculate chunk size
                if self.chunk_size > 0:
                    chunk_size = self.chunk_size
                else:
                    chunk_size = len(self.ashley_urls) // self.total_chunks
                
                start_idx = self.chunk_id * chunk_size
                
                if self.chunk_id == self.total_chunks - 1:
                    # Last chunk gets remaining URLs
                    urls_to_process = self.ashley_urls[start_idx:]
                else:
                    urls_to_process = self.ashley_urls[start_idx:start_idx + chunk_size]
                
                self.logger.info(f"🚀 Ashley Chunk {self.chunk_id + 1}/{self.total_chunks}: Processing {len(urls_to_process)} URLs")
            else:
                self.logger.info(f"🚀 Ashley mode: Processing {len(self.ashley_urls)} direct product URLs")
            
            # Create requests for each URL
            for i, url in enumerate(urls_to_process):
                # Add referer for subsequent requests
                headers = self.get_headers()
                if i > 0:
                    headers['Referer'] = urls_to_process[0]
                
                yield Request(
                    url,
                    callback=self.parse_product_page_with_check,
                    meta={
                        'url': url, 
                        'is_ashley': True, 
                        'retry_count': 0,
                        'chunk_id': self.chunk_id,
                        'chunk_mode': self.chunk_mode
                    },
                    errback=self.handle_product_error,
                    priority=10,
                    dont_filter=True,
                    headers=headers
                )
            return
        
        # SITEMAP MODE for non-Ashley websites
        if not hasattr(self, 'sitemap_chunk') or not self.sitemap_chunk:
            self.logger.error("No sitemaps to process")
            return
        
        self.logger.info(f"Starting to process {len(self.sitemap_chunk)} sitemaps")

        for sitemap_url in self.sitemap_chunk:
            yield Request(
                sitemap_url,
                callback=self.parse_product_sitemap,
                meta={
                    'sitemap_level': 1,
                    'chunk_id': self.chunk_id,
                    'chunk_mode': self.chunk_mode
                },
                errback=self.handle_sitemap_error,
                dont_filter=True
            )
    
    def parse_product_sitemap(self, response):
        """Parse sitemap and apply chunking to URLs if needed"""
        if response.url.endswith('.gz'):
            content = gzip.decompress(response.body)
            root = ET.fromstring(content)
        else:
            root = ET.fromstring(response.body)
        
        ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        url_elements = root.findall('ns:url/ns:loc', ns)
        all_urls = [elem.text for elem in url_elements if elem.text]
        
        # Apply max URLs per sitemap limit
        if self.max_urls_per_sitemap > 0:
            all_urls = all_urls[:self.max_urls_per_sitemap]
        
        # Filter PLP vs PDP
        plp_count = 0
        pdp_count = 0
        pdp_urls = []

        for url in all_urls:
            if self._is_plp_url(url):
                plp_count += 1
                continue
            pdp_urls.append(url)
        
        pdp_count = len(pdp_urls)
        
        # APPLY URL-LEVEL CHUNKING IF IN CHUNK MODE
        urls_to_process = pdp_urls
        chunk_info = ""
        
        if self.chunk_mode and self.total_chunks > 1:
            # Calculate chunk size for URLs within this sitemap
            if self.chunk_size > 0:
                chunk_size = self.chunk_size
            else:
                chunk_size = len(pdp_urls) // self.total_chunks
            
            if chunk_size > 0:
                start_idx = self.chunk_id * chunk_size
                
                if self.chunk_id == self.total_chunks - 1:
                    urls_to_process = pdp_urls[start_idx:]
                else:
                    urls_to_process = pdp_urls[start_idx:start_idx + chunk_size]
                
                chunk_info = f" (Chunk {self.chunk_id + 1}/{self.total_chunks}: {len(urls_to_process)} URLs)"
        
        self.logger.info(f"Sitemap {response.url}: {len(all_urls)} total, {plp_count} PLP, {pdp_count} PDP{chunk_info}")
        
        for url in urls_to_process:
            yield Request(
                url,
                callback=self.parse_product_page_with_check,
                meta={
                    'url': url,
                    'chunk_id': self.chunk_id,
                    'chunk_mode': self.chunk_mode
                },
                errback=self.handle_product_error,
                dont_filter=True
            )
    
    def _is_plp_url(self, url: str) -> bool:
        parsed_url = urlparse(url)
        path = parsed_url.path.strip('/')
        if not path:
            return True
        return '/' in path

    def _extract_json_ld_data(self, response):
        """Extract all JSON-LD data from a page"""
        json_scripts = response.xpath(self.json_ld_xpath).getall()
        
        product_data = {
            'sku': '',
            'mpn': '',
            'brand': '',
            'price': '',
            'image': '',
            'color': '',
            'status': '',
            'category': '',
            'category_url': ''
        }
        
        for script in json_scripts:
            try:
                # Clean script
                script_text = script.strip()
                if script_text.startswith('<!--'):
                    script_text = script_text[4:]
                if script_text.endswith('-->'):
                    script_text = script_text[:-3]
                script_text = script_text.strip()
                
                if not script_text:
                    continue
                    
                data = json.loads(script_text)
                
                # Handle both dict and list
                if isinstance(data, dict):
                    items = [data]
                elif isinstance(data, list):
                    items = data
                else:
                    items = []
                
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    
                    item_type = item.get('@type')
                    
                    # Product data
                    if item_type in ('Product', 'ProductGroup') or \
                       (isinstance(item_type, list) and any(t in ('Product', 'ProductGroup') for t in item_type)):
                        
                        if item.get('sku'):
                            product_data['sku'] = str(item['sku'])
                        if item.get('mpn'):
                            product_data['mpn'] = str(item['mpn'])
                        if item.get('color'):
                            product_data['color'] = str(item['color'])
                        if item.get('image'):
                            product_data['image'] = str(item['image'])
                        
                        brand = item.get('brand', {})
                        if isinstance(brand, dict):
                            product_data['brand'] = str(brand.get('name', ''))
                        elif brand:
                            product_data['brand'] = str(brand)
                        
                        offers = item.get('offers', {})
                        if isinstance(offers, dict):
                            if offers.get('price'):
                                product_data['price'] = str(offers['price'])
                            
                            availability = str(offers.get('availability', '')).lower()
                            if 'instock' in availability:
                                product_data['status'] = 'Active'
                            elif 'outofstock' in availability or 'soldout' in availability:
                                product_data['status'] = 'Out of Stock'
                            elif 'preorder' in availability:
                                product_data['status'] = 'Active'
                    
                    # Breadcrumb data
                    elif item_type == 'BreadcrumbList':
                        categories = []
                        urls = []
                        
                        for element in item.get('itemListElement', []):
                            item_data = element.get('item', {})
                            name = item_data.get('name', '')
                            url = item_data.get('@id', '')
                            
                            if name and name.lower() not in ['home', 'shop', 'all']:
                                categories.append(name)
                            if url:
                                urls.append(url)
                        
                        if len(categories) > 1:
                            categories = categories[:-1]
                        if categories:
                            product_data['category'] = ' > '.join(categories)
                        
                        if len(urls) >= 2:
                            product_data['category_url'] = urls[-2]
                            
            except json.JSONDecodeError:
                continue
            except Exception:
                continue
        
        return product_data

    def parse_product_page_with_check(self, response):
        """Parse product page with caching and error handling"""
        # Check cache for product detection
        cache_key = response.url
        if cache_key in self.json_ld_cache:
            has_product_json = self.json_ld_cache[cache_key]
        else:
            # Quick check for product JSON-LD
            has_product_json = False
            json_scripts = response.xpath(self.json_ld_xpath).getall()
            for script in json_scripts[:3]:
                try:
                    script_text = script.strip()
                    if script_text.startswith('<!--'):
                        script_text = script_text[4:]
                    if script_text.endswith('-->'):
                        script_text = script_text[:-3]
                    script_text = script_text.strip()
                    
                    if not script_text:
                        continue
                        
                    data = json.loads(script_text)
                    if isinstance(data, dict):
                        if 'Product' in str(data.get('@type', '')):
                            has_product_json = True
                            break
                    elif isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict) and 'Product' in str(item.get('@type', '')):
                                has_product_json = True
                                break
                        if has_product_json:
                            break
                except Exception:
                    continue
            self.json_ld_cache[cache_key] = has_product_json
        
        # Extract JSON-LD data
        json_data = self._extract_json_ld_data(response)
        
        # Build item with exact column structure
        item = {
            'Ref Product URL': response.url,
            'Ref Product ID': '',
            'Ref Variant ID': '',
            'Ref Category': json_data['category'],
            'Ref Category URL': json_data['category_url'],
            'Ref Brand Name': json_data['brand'],
            'Ref Product Name': '',
            'Ref SKU': json_data['sku'],
            'Ref MPN': json_data['mpn'],
            'Ref GTIN': '',
            'Ref Price': json_data['price'],
            'Ref Main Image': json_data['image'],
            'Ref Quantity': '',
            'Ref Group Attr 1': json_data['color'],
            'Ref Group Attr 2': '',
            'Ref Images': '',
            'Ref Dimensions': '',
            'Ref Status': json_data['status'],
            'Ref Highlights': '',
            'Date Scrapped': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Add non-JSON fields
        product_id = response.xpath('//div[@data-id]/@data-id').get()
        if product_id:
            item['Ref Product ID'] = product_id.strip()
        
        product_name = response.xpath('//*[@id="contentId"]/div/div[1]/div[2]/div[2]/h1/text()').get()
        if product_name:
            item['Ref Product Name'] = product_name.strip()
        
        # Extract additional fields with error handling
        try:
            item['Ref Images'] = self.extract_main_images(response)
        except Exception as e:
            self.logger.error(f"Image extraction failed: {e}")
            item['Ref Images'] = ''
        
        try:
            item['Ref Highlights'] = self.extract_highlights(response)
        except Exception as e:
            self.logger.error(f"Highlight extraction failed: {e}")
            item['Ref Highlights'] = ''
        
        try:
            item['Ref Dimensions'] = self.extract_dimensions(response)
        except Exception as e:
            self.logger.error(f"Dimension extraction failed: {e}")
            item['Ref Dimensions'] = ''
        
        # Add chunk metadata to output (optional)
        if self.chunk_mode:
            item['Chunk ID'] = str(self.chunk_id)
            item['Total Chunks'] = str(self.total_chunks)
        
        yield item
        
        if has_product_json:
            for bundle_item in self.extract_bundle_products(response):
                yield bundle_item
    
    def extract_bundle_products(self, response):
        """Extract bundle products with safety limits and deduplication"""
        json_script = response.xpath('//script[@data-hypernova-key="App"]/text()').get()
        if not json_script:
            return
        
        try:
            script = json_script.strip()
            if script.startswith('<!--'):
                script = script[4:]
            if script.endswith('-->'):
                script = script[:-3]
            script = script.strip()
            
            if not script:
                return
            
            data = json.loads(script)
            simple_items = data.get('data', {}).get('content', {}).get('productLayouts', {}).get('simpleItems', [])
            
            # Limit to 5 bundle products max
            for item in simple_items[:5]:
                if isinstance(item, dict):
                    sub_product_url = item.get('url')
                    if not sub_product_url or not isinstance(sub_product_url, str):
                        continue
                    
                    sub_product_url = sub_product_url.strip()
                    
                    # Skip if same as parent or already processed
                    if sub_product_url == response.url or sub_product_url in self._processed_bundle_urls:
                        continue
                    
                    self._processed_bundle_urls.add(sub_product_url)
                    
                    self.logger.info(f"Found bundle product: {sub_product_url}")
                    yield Request(
                        sub_product_url,
                        callback=self.parse_product_page_with_check,
                        meta={
                            'url': sub_product_url,
                            'chunk_id': self.chunk_id,
                            'chunk_mode': self.chunk_mode
                        },
                        errback=self.handle_product_error,
                        priority=1,
                        dont_filter=True
                    )
        except Exception as e:
            self.logger.error(f"Error extracting bundle products: {e}")

    # Keep all original methods below
    def parse_product_page(self, response):
        item = {}
        sku = self.extract_sku(response)
        item['Ref Product URL'] = response.url
        item['Ref SKU'] = sku
        item['Date Scrapped'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        item['Ref Product Name'] = self.extract_product_name(response)
        item['Ref Price'] = self.extract_price(response)
        item['Ref MPN'] = self.extract_mpn(response)
        item['Ref GTIN'] = self.extract_gtin(response)
        item['Ref Brand Name'] = self.extract_brand(response)
        item['Ref Main Image'] = self.extract_main_image(response)
        item['Ref Category'] = self.extract_category(response)
        item['Ref Category URL'] = self.extract_category_url(response)
        item['Ref Quantity'] = self.extract_quantity(response)
        item['Ref Status'] = self.extract_status(response)
        item['Ref Product ID'] = self.extract_product_id(response)
        item['Ref Variant ID'] = self.extract_variant_id(response)
        item['Ref Group Attr 1'] = self.extract_group_attr1(response, 1)
        item['Ref Group Attr 2'] = self.extract_group_attr2(response, 2)
        item['Ref Images'] = self.extract_main_images(response)
        item['Ref Highlights'] = self.extract_highlights(response)
        item['Ref Dimensions'] = self.extract_dimensions(response)
        yield item
    
    def extract_product_name(self, response):
        selectors = [
            '//*[@id="contentId"]/div/div[1]/div[2]/div[2]/h1/text()'
        ]
        return self.extract_using_selectors(response, selectors)
    
    def extract_price(self, response):
        for script in response.xpath(self.json_ld_xpath).getall():
            try:
                data = json.loads(script)
                if data.get('@type') == 'Product' or data.get('@type') == 'ProductGroup':
                    offers = data.get('offers', {})
                    if isinstance(offers, dict) and 'price' in offers:
                        return str(offers['price'])
            except:
                continue
        return ''
    
    def extract_sku(self, response):
        for script in response.xpath(self.json_ld_xpath).getall():
            try:
                data = json.loads(script)
                if data.get('@type') == 'Product' or data.get('@type') == 'ProductGroup':
                    return data.get('sku', '')
            except:
                continue
        return ''
    
    def extract_mpn(self, response):
        for script in response.xpath(self.json_ld_xpath).getall():
            try:
                data = json.loads(script)
                if data.get('@type') == 'Product' or data.get('@type') == 'ProductGroup':
                    return data.get('mpn', '')
            except:
                continue
        return ''

    def extract_gtin(self, response):
        return ''
    
    def extract_brand(self, response):
        for script in response.xpath(self.json_ld_xpath).getall():
            try:
                data = json.loads(script)
                if data.get('@type') == 'Product' or data.get('@type') == 'ProductGroup':
                    brand = data.get('brand', {})
                    if isinstance(brand, dict):
                        return brand.get('name', '')
                    else:
                        return str(brand)
            except:
                continue
        return ''
    
    def extract_main_image(self, response):
        for script in response.xpath(self.json_ld_xpath).getall():
            try:
                data = json.loads(script)
                if data.get('@type') == 'Product' or data.get('@type') == 'ProductGroup':
                    return data.get('image', '')
            except:
                continue
        return ''
    
    def extract_category(self, response):
        for script in response.xpath(self.json_ld_xpath).getall():
            try:
                data = json.loads(script)
                if data.get('@type') == 'BreadcrumbList':
                    categories = []
                    for item in data.get('itemListElement', []):
                        item_data = item.get('item', {})
                        name = item_data.get('name', '')
                        if name and name.lower() not in ['home', 'shop', 'all']:
                            categories.append(name)
                    if len(categories) > 1:
                        categories = categories[:-1]
                    if categories:
                        return ' > '.join(categories)
            except:
                continue
        return ''

    def extract_category_url(self, response):
        for script in response.xpath(self.json_ld_xpath).getall():
            try:
                data = json.loads(script)
                if data.get('@type') == 'BreadcrumbList':
                    urls = []
                    for item in data.get('itemListElement', []):
                        item_data = item.get('item', {})
                        url = item_data.get('@id', '')
                        if url:
                            urls.append(url)
                    if len(urls) >= 2:
                        return urls[-2]
            except:
                continue
        return ''
    
    def extract_quantity(self, response):
        return ''

    def extract_status(self, response):
        for script in response.xpath(self.json_ld_xpath).getall():
            try:
                data = json.loads(script)
                if data.get('@type') == 'Product' or data.get('@type') == 'ProductGroup':
                    offers = data.get('offers', {})
                    if isinstance(offers, dict):
                        availability = str(offers.get('availability', '')).lower()
                        if 'instock' in availability:
                            return 'Active'
                        elif 'outofstock' in availability or 'soldout' in availability:
                            return 'Out of Stock'
                        elif 'preorder' in availability:
                            return 'Active'
            except:
                continue
        return ''
    
    def extract_product_id(self, response):
        product_id = response.xpath('//div[@data-id]/@data-id').get()        
        if product_id:
            return product_id.strip()
        return ''
    
    def extract_variant_id(self, response):
        return ''
    
    def extract_group_attr1(self, response, attr_num):
        for script in response.xpath(self.json_ld_xpath).getall():
            try:
                data = json.loads(script)
                if data.get('@type') == 'Product' or data.get('@type') == 'ProductGroup':
                    return data.get('color', '')
            except:
                continue
        return ''

    def extract_group_attr2(self, response, attr_num):
        return ''
       
    def extract_using_selectors(self, response, selectors):
        for selector in selectors:
            if selector.startswith('//'):
                result = response.xpath(selector).get()
            else:
                result = response.css(selector).get()
            
            if result:
                cleaned = result.strip()
                if cleaned:
                    return cleaned
        return ''

    def extract_highlights(self, response):
        highlights = []
        highlight_items = response.xpath('//div[contains(@class, "product-hightlights-items-item")]')
        # Limit to 5 highlights
        for item in highlight_items[:5]:
            title = item.xpath('.//span[contains(@class, "product-hightlights-items-item-title")]/text()').get()
            desc = item.xpath('.//p[contains(@class, "product-hightlights-items-item-desc")]/text()').get()
            if title:
                highlights.append({
                    'title': title.strip() if title else '',
                    'desc': desc.strip() if desc else ''
                })
        return json.dumps(highlights, indent=2) if highlights else ''
    
    def extract_main_images(self, response):
        image_urls = []
        json_script = response.xpath('//script[@data-hypernova-key="App"]/text()').get()
        if json_script:
            json_script = json_script.strip()
            if json_script.startswith('<!--'):
                json_script = json_script[4:]
            if json_script.endswith('-->'):
                json_script = json_script[:-3]
            json_script = json_script.strip()
            try:
                data = json.loads(json_script)
                main_data = data.get('data', {})
                content = main_data.get('content', {})
                gallery = content.get('gallery', [])
                if isinstance(gallery, list):
                    # Limit to 10 images
                    for img in gallery[:10]:
                        if isinstance(img, dict):
                            original_url = img.get('original')
                            if original_url:
                                image_urls.append(original_url)
            except Exception as e:
                self.logger.error(f"Failed to extract images: {e}")
        return '\n'.join(image_urls) if image_urls else ''

    def extract_dimensions(self, response):
        """Extract dimensions with limits to prevent infinite loops"""
        json_script = response.xpath('//script[@data-hypernova-key="App"]/text()').get()
        if not json_script:
            return ''
        
        json_script = json_script.strip()
        if json_script.startswith('<!--'):
            json_script = json_script[4:]
        if json_script.endswith('-->'):
            json_script = json_script[:-3]
        json_script = json_script.strip()
        
        try:
            data = json.loads(json_script)
            content = data.get('data', {}).get('content', {})
            
            result = {}
            
            # ONLY process setIncludes - remove all nested loops
            setIncludes = content.get('setIncludes', {})
            items = setIncludes.get('items', [])
            
            # Limit to first 3 items
            for item in items[:3]:
                if not isinstance(item, dict):
                    continue
                
                item_short_name = item.get('itemShortName', '')
                if not item_short_name:
                    continue
                    
                dimension = item.get('dimension', {})
                dimensions_list = dimension.get('list', [])
                
                # Limit to first 3 dimensions
                dimension_data = []
                for dim in dimensions_list[:3]:
                    if dim and isinstance(dim, str):
                        dimension_data.append(dim)
                
                if dimension_data:
                    result[item_short_name.lower()] = dimension_data
            
            # If no dimensions, try simpleItems
            if not result:
                simpleItems = content.get('productLayouts', {}).get('simpleItems', [])
                if isinstance(simpleItems, list):
                    for item in simpleItems[:3]:
                        if not isinstance(item, dict):
                            continue
                        
                        item_short_name = item.get('itemShortName', '')
                        if not item_short_name:
                            continue
                            
                        dimension = item.get('dimension', {})
                        dimensions_list = dimension.get('list', [])
                        
                        dimension_data = []
                        for dim in dimensions_list[:3]:
                            if dim and isinstance(dim, str):
                                dimension_data.append(dim)
                        
                        if dimension_data:
                            result[item_short_name.lower()] = dimension_data
            
            # If still no dimensions, try accordion
            if not result:
                accordion_data = content.get('accordion', {})
                dimensions_data = accordion_data.get('dimensions', {})
                
                if dimensions_data and isinstance(dimensions_data, dict):
                    dimension_list = dimensions_data.get('dimensionList', [])
                    
                    dimension_data = []
                    for dim in dimension_list[:3]:
                        if dim and isinstance(dim, str):
                            dimension_data.append(dim)
                    
                    if dimension_data:
                        result["dimensions"] = dimension_data
            
            # Return simple JSON
            if result:
                return json.dumps(result, indent=2)
            return ''
            
        except Exception as e:
            self.logger.error(f"Error extracting dimensions: {e}")
            return ''

    def is_valid_image_url(self, url):
        if not url or not isinstance(url, str):
            return False
        
        image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg', '.bmp']
        url_lower = url.lower()
        
        if not (url_lower.startswith('http://') or url_lower.startswith('https://')):
            return False
        
        if not any(url_lower.endswith(ext) for ext in image_extensions):
            if not any(ext in url_lower for ext in image_extensions):
                return False
        
        return True
        
    def clean_price(self, price_text):
        if not price_text:
            return ''
        
        cleaned = re.sub(r'[^\d.,]', '', price_text)
        
        if ',' in cleaned and '.' in cleaned:
            if cleaned.rfind(',') > cleaned.rfind('.'):
                cleaned = cleaned.replace('.', '').replace(',', '.')
            else:
                cleaned = cleaned.replace(',', '')
        
        try:
            price_float = float(cleaned)
            return f"{price_float:.2f}"
        except ValueError:
            return cleaned
    
    def handle_sitemap_error(self, failure):
        """Handle sitemap request failures"""
        url = failure.request.url
        chunk_id = failure.request.meta.get('chunk_id', 0)
        self.logger.error(f"Chunk {chunk_id}: Sitemap request failed for {url}: {failure.value}")
    
    def handle_product_error(self, failure):
        """Handle request failures with retry logic"""
        url = failure.request.meta.get('url', 'Unknown')
        retry_count = failure.request.meta.get('retry_count', 0)
        chunk_id = failure.request.meta.get('chunk_id', 0)
        
        # Check if it's a 405 or 429 error
        if hasattr(failure.value, 'response') and failure.value.response:
            status = failure.value.response.status
            self.logger.error(f"Chunk {chunk_id}: Product page failed for {url} - Status: {status}, Retry: {retry_count}")
            
            # Retry up to 3 times for specific errors
            if status in [405, 429, 500, 502, 503, 504] and retry_count < 3:
                self.logger.info(f"Chunk {chunk_id}: Retrying {url} (attempt {retry_count + 1}/3)")
                
                headers = self.get_headers(referer='https://colemanfurniture.com/ashley-furniture.html')
                
                retry_request = failure.request.copy()
                retry_request.meta['retry_count'] = retry_count + 1
                retry_request.headers.update(headers)
                retry_request.dont_filter = True
                return retry_request
        else:
            self.logger.error(f"Chunk {chunk_id}: Product page request failed for {url}: {failure.value}")