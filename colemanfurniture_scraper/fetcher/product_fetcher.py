import gzip
import xml.etree.ElementTree as ET
import json
import re
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
        
        # Cache for parsed JSON-LD to avoid re-parsing
        self.json_ld_cache = {}
        
        try:
            sitemap_processor = SitemapProcessor()
            self.sitemap_index_url = sitemap_processor.get_sitemap_from_robots(self.website_url)
            self.logger.info(f"Found sitemap index: {self.sitemap_index_url}")
            
            self.all_sitemaps = sitemap_processor.extract_all_sitemaps(self.sitemap_index_url)
            self.logger.info(f"Total sitemaps discovered: {len(self.all_sitemaps)}")
            
            self.sitemap_chunk = sitemap_processor.get_sitemap_chunks(
                self.all_sitemaps, 
                self.sitemap_offset, 
                self.max_sitemaps
            )
            
            self.logger.info(f"This job will process {len(self.sitemap_chunk)} sitemaps")
            
        except Exception as e:
            self.logger.error(f"Failed to discover sitemap: {e}")
            raise
    
    def start_requests(self):
        if not hasattr(self, 'sitemap_chunk') or not self.sitemap_chunk:
            self.logger.error("No sitemaps to process")
            return
        
        self.logger.info(f"Starting to process {len(self.sitemap_chunk)} sitemaps")
        
        # Process sitemaps in parallel with higher concurrency
        for sitemap_url in self.sitemap_chunk:
            yield Request(
                sitemap_url,
                callback=self.parse_product_sitemap,
                meta={'sitemap_level': 1},
                errback=self.handle_sitemap_error,
                priority=10  # Higher priority for sitemaps
            )
    
    def parse_product_sitemap(self, response):
        # Optimize XML parsing
        if response.url.endswith('.gz'):
            content = gzip.decompress(response.body)
            root = ET.fromstring(content)
        else:
            root = ET.fromstring(response.body)
        
        # Pre-compile namespace
        ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        
        # Faster URL extraction
        all_urls = []
        for url_elem in root.findall('ns:url/ns:loc', ns):
            if url_elem.text:
                all_urls.append(url_elem.text)
        
        if self.max_urls_per_sitemap > 0:
            all_urls = all_urls[:self.max_urls_per_sitemap]
        
        self.logger.info(f"Processing {len(all_urls)} URLs from sitemap")
        
        plp_count = 0
        pdp_count = 0
        pdp_requests = []
        
        # Batch process URL classification
        for url in all_urls:
            if self._is_plp_url(url):
                plp_count += 1
                continue
            pdp_count += 1
            pdp_requests.append(url)
        
        self.logger.info(f"Filtered {plp_count} PLP pages, {pdp_count} PDP pages to scrape")
        
        # Yield all requests at once with proper priority
        for url in pdp_requests:
            yield Request(
                url,
                callback=self.parse_product_page_with_check,
                meta={'url': url},
                errback=self.handle_product_error,
                priority=5,  # Lower than sitemaps but higher than sub-products
                dont_filter=True  # Avoid duplicate filtering overhead
            )
    
    def _is_plp_url(self, url: str) -> bool:
        # Optimized PLP detection - faster path check
        parsed_url = urlparse(url)
        path = parsed_url.path.strip('/')
        
        if not path:
            return True
        
        # Quick check for common PDP patterns
        if '/p-' in path or '/product/' in path or '/item/' in path:
            return False
        
        return '/' in path

    def parse_product_page_with_check(self, response):
        # Cache the parsed JSON-LD
        cache_key = response.url
        if cache_key in self.json_ld_cache:
            has_product_json = self.json_ld_cache[cache_key]
        else:
            has_product_json = False
            json_scripts = response.xpath('//script[@type="application/ld+json"]/text()').getall()
            
            for script in json_scripts:
                try:
                    data = json.loads(script.strip())
                    if isinstance(data, dict):
                        data_type = data.get('@type')
                        if data_type:
                            if isinstance(data_type, str):
                                if 'Product' in data_type:
                                    has_product_json = True
                                    break
                            elif isinstance(data_type, list):
                                if any('Product' in str(t) for t in data_type):
                                    has_product_json = True
                                    break
                        elif data.get('name') and (data.get('offers') or data.get('sku')):
                            has_product_json = True
                            break
                    elif isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict):
                                item_type = item.get('@type')
                                if item_type:
                                    if isinstance(item_type, str) and 'Product' in item_type:
                                        has_product_json = True
                                        break
                                    elif isinstance(item_type, list) and any('Product' in str(t) for t in item_type):
                                        has_product_json = True
                                        break
                        if has_product_json:
                            break
                except:
                    continue
            
            self.json_ld_cache[cache_key] = has_product_json
        
        if has_product_json:
            self.logger.info(f"Found Product JSON-LD for {response.url}")
            # Extract all data in one pass
            item = self.extract_all_product_data(response)
            yield item
            # Extract bundle products with lower priority
            yield from self.extract_bundle_products(response)
        else:
            self.logger.warning(f"No Product JSON-LD found for {response.url}")
            item = self.extract_all_product_data(response)
            yield item
    
    def extract_bundle_products(self, response):
        json_script = response.xpath('//script[@data-hypernova-key="App"]/text()').get()
        if not json_script:
            return
        
        try:
            # Optimize JSON cleaning
            json_script = json_script.strip()
            if json_script.startswith('<!--'):
                json_script = json_script[4:]
            if json_script.endswith('-->'):
                json_script = json_script[:-3]
            json_script = json_script.strip()
            
            data = json.loads(json_script)
            content = data.get('data', {}).get('content', {})
            product_layouts = content.get('productLayouts', {})
            simple_items = product_layouts.get('simpleItems', [])
            
            # Process bundle products in batch
            bundle_requests = []
            for item in simple_items:
                if isinstance(item, dict):
                    sub_product_url = item.get('url')
                    if sub_product_url and sub_product_url != response.url:
                        bundle_requests.append(sub_product_url)
            
            # Yield bundle requests with lower priority
            for url in bundle_requests:
                yield Request(
                    url,
                    callback=self.parse_product_page_with_check,
                    meta={'url': url},
                    errback=self.handle_product_error,
                    priority=1,  # Lower priority
                    dont_filter=True
                )
        except Exception as e:
            self.logger.error(f"Error extracting bundle products: {e}")

    def extract_all_product_data(self, response):
        """Extract all product data in a single pass to minimize XPath queries"""
        item = {}
        
        # Parse JSON-LD once and reuse
        json_ld_data = None
        json_ld_product = None
        
        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
            try:
                json_ld_data = json.loads(script.strip())
                if isinstance(json_ld_data, dict):
                    if json_ld_data.get('@type') == 'Product' or json_ld_data.get('@type') == 'ProductGroup':
                        json_ld_product = json_ld_data
                        break
                    elif json_ld_data.get('@type') == 'BreadcrumbList':
                        item['Ref Category'] = self.extract_category_from_json(json_ld_data)
                        item['Ref Category URL'] = self.extract_category_url_from_json(json_ld_data)
                elif isinstance(json_ld_data, list):
                    for entry in json_ld_data:
                        if isinstance(entry, dict):
                            if entry.get('@type') == 'Product' or entry.get('@type') == 'ProductGroup':
                                json_ld_product = entry
                                break
                            elif entry.get('@type') == 'BreadcrumbList':
                                item['Ref Category'] = self.extract_category_from_json(entry)
                                item['Ref Category URL'] = self.extract_category_url_from_json(entry)
            except:
                continue
        
        # Extract basic product info
        item['Ref Product URL'] = response.url
        item['Date Scrapped'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        if json_ld_product:
            item['Ref SKU'] = json_ld_product.get('sku', '')
            item['Ref MPN'] = json_ld_product.get('mpn', '')
            item['Ref GTIN'] = ''  # Not provided in JSON-LD
            
            # Extract brand
            brand = json_ld_product.get('brand', {})
            if isinstance(brand, dict):
                item['Ref Brand Name'] = brand.get('name', '')
            else:
                item['Ref Brand Name'] = str(brand)
            
            # Extract price
            offers = json_ld_product.get('offers', {})
            if isinstance(offers, dict):
                item['Ref Price'] = str(offers.get('price', ''))
                # Extract status
                availability = str(offers.get('availability', '')).lower()
                if 'instock' in availability:
                    item['Ref Status'] = 'Active'
                elif 'outofstock' in availability or 'soldout' in availability:
                    item['Ref Status'] = 'Out of Stock'
                elif 'preorder' in availability:
                    item['Ref Status'] = 'Active'
                else:
                    item['Ref Status'] = ''
            else:
                item['Ref Price'] = ''
                item['Ref Status'] = ''
            
            # Extract image
            item['Ref Main Image'] = json_ld_product.get('image', '')
            
            # Extract color/group attr
            item['Ref Group Attr 1'] = json_ld_product.get('color', '')
        else:
            item['Ref SKU'] = ''
            item['Ref MPN'] = ''
            item['Ref GTIN'] = ''
            item['Ref Brand Name'] = ''
            item['Ref Price'] = ''
            item['Ref Main Image'] = ''
            item['Ref Group Attr 1'] = ''
            item['Ref Status'] = ''
        
        # Set defaults for missing fields
        if 'Ref Category' not in item:
            item['Ref Category'] = ''
        if 'Ref Category URL' not in item:
            item['Ref Category URL'] = ''
        
        # Extract remaining fields with single XPath queries
        item['Ref Product Name'] = self.extract_product_name(response)
        item['Ref Product ID'] = self.extract_product_id(response)
        item['Ref Variant ID'] = ''  # Not provided
        item['Ref Quantity'] = ''  # Not provided
        item['Ref Group Attr 2'] = ''  # Not provided
        
        # Extract complex fields
        item['Ref Images'] = self.extract_main_images(response)
        item['Ref Dimensions'] = self.extract_dimensions(response)
        item['Ref Highlights'] = self.extract_highlights(response)
        
        return item
    
    def extract_category_from_json(self, data):
        """Extract category from BreadcrumbList JSON"""
        try:
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
            pass
        return ''
    
    def extract_category_url_from_json(self, data):
        """Extract category URL from BreadcrumbList JSON"""
        try:
            urls = []
            for item in data.get('itemListElement', []):
                item_data = item.get('item', {})
                url = item_data.get('@id', '')
                if url:
                    urls.append(url)
            if len(urls) >= 2:
                return urls[-2]
        except:
            pass
        return ''
    
    def extract_product_name(self, response):
        selectors = [
            '//*[@id="contentId"]/div/div[1]/div[2]/div[2]/h1/text()'
        ]
        return self.extract_using_selectors(response, selectors)
    
    def extract_product_id(self, response):
        # Single XPath query - no loop needed
        product_id = response.xpath('//div[@data-id]/@data-id').get()
        return product_id if product_id else ''
    
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
        # Optimize XPath queries
        highlights = []
        highlight_items = response.xpath('//div[contains(@class, "product-hightlights-items-item")]')
        
        # Batch process XPath evaluations
        for item in highlight_items:
            title = item.xpath('.//span[contains(@class, "product-hightlights-items-item-title")]/text()').get()
            desc = item.xpath('.//p[contains(@class, "product-hightlights-items-item-desc")]/text()').get()
            if title:
                highlights.append({
                    'title': title.strip() if title else '',
                    'desc': desc.strip() if desc else ''
                })
        
        if highlights:
            return json.dumps(highlights, separators=(',', ':'))  # Compact JSON
        return ''
    
    def extract_main_images(self, response):
        json_script = response.xpath('//script[@data-hypernova-key="App"]/text()').get()
        if not json_script:
            return ''
        
        try:
            # Optimize JSON cleaning
            json_script = json_script.strip()
            if json_script.startswith('<!--'):
                json_script = json_script[4:]
            if json_script.endswith('-->'):
                json_script = json_script[:-3]
            json_script = json_script.strip()
            
            data = json.loads(json_script)
            main_data = data.get('data', {})
            content = main_data.get('content', {})
            gallery = content.get('gallery', [])
            
            if isinstance(gallery, list):
                image_urls = []
                for img in gallery:
                    if isinstance(img, dict):
                        original_url = img.get('original')
                        if original_url:
                            image_urls.append(original_url)
                return '\n'.join(image_urls) if image_urls else ''
        except Exception as e:
            self.logger.error(f"Failed to extract images: {e}")
        
        return ''
    
    def extract_dimensions(self, response):
        json_script = response.xpath('//script[@data-hypernova-key="App"]/text()').get()
        if not json_script:
            return ''
        
        try:
            # Optimize JSON cleaning
            json_script = json_script.strip()
            if json_script.startswith('<!--'):
                json_script = json_script[4:]
            if json_script.endswith('-->'):
                json_script = json_script[:-3]
            json_script = json_script.strip()
            
            data = json.loads(json_script)
            content = data.get('data', {}).get('content', {})
            result = {}
            
            # Batch process all dimension sources
            dimension_sources = [
                ('items', content.get('setIncludes', {}).get('items', [])),
                ('configurables', self._extract_configurables(content.get('setIncludes', {}).get('items', []))),
                ('additional', content.get('additionalItems', {}).get('items', [])),
                ('simple', content.get('productLayouts', {}).get('simpleItems', []))
            ]
            
            for source_name, source_data in dimension_sources:
                if isinstance(source_data, list):
                    for item in source_data:
                        if not isinstance(item, dict):
                            continue
                        
                        item_short_name = item.get('itemShortName', '')
                        dimension = item.get('dimension', {})
                        
                        if not item_short_name or not dimension:
                            continue
                        
                        # Extract image URL
                        image_obj = dimension.get('image', {})
                        image_url = ''
                        if isinstance(image_obj, dict):
                            image_url = image_obj.get('url', '')
                            if image_url and not self.is_valid_image_url(image_url):
                                image_url = ''
                        
                        # Extract dimension data
                        dimensions_list = dimension.get('list', [])
                        dimension_data = [dim for dim in dimensions_list if dim and isinstance(dim, str)]
                        
                        if dimension_data or image_url:
                            result[item_short_name.lower()] = {
                                "url": image_url,
                                "data": dimension_data
                            }
            
            # Check accordion data if no dimensions found
            if not result:
                accordion_data = content.get('accordion', {})
                dimensions_data = accordion_data.get('dimensions', {})
                
                if dimensions_data and isinstance(dimensions_data, dict):
                    dimension_list = dimensions_data.get('dimensionList', [])
                    dimension_data = [dim for dim in dimension_list if dim and isinstance(dim, str)]
                    
                    if dimension_data:
                        image_obj = dimensions_data.get('image', {})
                        image_url = ''
                        if isinstance(image_obj, dict):
                            image_url = image_obj.get('url', '')
                            if image_url and not self.is_valid_image_url(image_url):
                                image_url = ''
                        
                        result["dimensions"] = {
                            "url": image_url,
                            "data": dimension_data
                        }
            
            if result:
                return json.dumps(result, separators=(',', ':'))
            return ''
            
        except Exception as e:
            self.logger.error(f"Error extracting dimensions: {e}")
            return ''
    
    def _extract_configurables(self, items):
        """Helper method to extract configurable items"""
        configurables = []
        for item in items:
            if not isinstance(item, dict):
                continue
            
            configs = item.get('configurables', [])
            for config in configs:
                if not isinstance(config, dict):
                    continue
                
                options = config.get('options', [])
                for option in options:
                    if isinstance(option, dict):
                        configurables.append(option)
        
        return configurables
    
    def is_valid_image_url(self, url):
        if not url or not isinstance(url, str):
            return False
        
        # Fast path - check extension first
        url_lower = url.lower()
        if not (url_lower.startswith('http://') or url_lower.startswith('https://')):
            return False
        
        # Quick extension check
        image_extensions = ('.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg', '.bmp')
        if url_lower.endswith(image_extensions):
            return True
        
        # Fallback check
        return any(ext in url_lower for ext in image_extensions)
    
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
        self.logger.error(f"Sitemap request failed: {failure.value}")
    
    def handle_product_error(self, failure):
        self.logger.error(f"Product page request failed: {failure.value}")