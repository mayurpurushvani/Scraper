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

        # Cache for PLP URLs to avoid rechecking
        self.seen_plp_urls = set()

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

        for sitemap_url in self.sitemap_chunk:
            yield Request(
                sitemap_url,
                callback=self.parse_product_sitemap,
                meta={'sitemap_level': 1},
                errback=self.handle_sitemap_error
            )

    def parse_product_sitemap(self, response):
        """Optimized sitemap parsing with faster XML processing"""
        if response.url.endswith('.gz'):
            content = gzip.decompress(response.body)
            parser = ET.XMLParser(encoding='utf-8')
            root = ET.fromstring(content, parser=parser)
        else:
            parser = ET.XMLParser(encoding='utf-8')
            root = ET.fromstring(response.body, parser=parser)

        ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        all_urls = [url.text for url in root.findall('ns:url/ns:loc', ns) if url.text]

        if self.max_urls_per_sitemap > 0:
            all_urls = all_urls[:self.max_urls_per_sitemap]

        self.logger.info(f"Processing {len(all_urls)} URLs from sitemap")

        plp_count = 0
        pdp_count = 0
        requests = []

        for url in all_urls:
            if self._is_plp_url(url):
                plp_count += 1
                self.seen_plp_urls.add(url)
                continue
            pdp_count += 1
            requests.append(Request(
                url,
                callback=self.parse_product_page_with_check,
                meta={'url': url},
                errback=self.handle_product_error,
                priority=100
            ))

        for req in requests:
            yield req

        self.logger.info(f"Filtered {plp_count} PLP pages, {pdp_count} PDP pages to scrape")

    def _is_plp_url(self, url: str) -> bool:
        """Optimized PLP detection - runs in O(1) with early returns"""
        parsed_url = urlparse(url)
        path = parsed_url.path.strip('/')

        if not path:
            return True

        if not path.endswith('.htm'):
            return True

        path_lower = path.lower()

        if any(x in path_lower for x in (
                'furniture', 'sets', 'clearance', 'sale', 'new', 'shop',
                'bedroom', 'living-room', 'dining-room', 'office',
                'outdoor', 'kids', 'baby', 'collection', 'brand',
                'category', 'department', 'deals', 'closeouts', 'best-sellers'
        )):
            return True

        if len(path_lower.split('-')) <= 4:
            return True

        if re.search(r'\d{4,}|[a-z]+\d{3,}', path_lower):
            return False

        return False

    def parse_product_page_with_check(self, response):
        """Process product pages - ALWAYS process, don't skip"""
        self.logger.info(f"Processing product page: {response.url}")
        
        # Always process the page
        yield from self.parse_product_page(response)
        yield from self.extract_bundle_products(response)

    def extract_bundle_products(self, response):
        """Optimized bundle extraction"""
        if len(response.body) > 500000:
            json_script = response.xpath('//script[@data-hypernova-key="App"]/text()').get()
            if not json_script or 'simpleItems' not in json_script:
                return

            try:
                if json_script.startswith('<!--'):
                    json_script = json_script[json_script.find('{'):json_script.rfind('}') + 1]

                data = json.loads(json_script)
                simple_items = data.get('data', {}).get('content', {}).get('productLayouts', {}).get('simpleItems', [])

                if not simple_items:
                    return

                for item in simple_items:
                    if isinstance(item, dict):
                        sub_url = item.get('url')
                        if sub_url and sub_url != response.url:
                            if self._is_plp_url(sub_url):
                                continue

                            self.logger.debug(f"Bundle sub-product: {item.get('itemShortName', '')}")
                            yield Request(
                                sub_url,
                                callback=self.parse_product_page_with_check,
                                meta={'url': sub_url},
                                errback=self.handle_product_error,
                                priority=10
                            )
            except Exception as e:
                self.logger.debug(f"Error extracting bundle products: {e}")

    def parse_product_page(self, response):
        """Parse product page with robust error handling - NEVER SKIP"""
        item = {}

        # Parse JSON-LD
        json_ld_data = []
        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
            try:
                data = json.loads(script.strip())
                json_ld_data.append(data)
            except:
                continue

        # Parse App script
        json_app_data = None
        app_script = response.xpath('//script[@data-hypernova-key="App"]/text()').get()
        if app_script:
            try:
                app_script = app_script.strip()
                if app_script.startswith('<!--'):
                    app_script = app_script[4:]
                if app_script.endswith('-->'):
                    app_script = app_script[:-3]
                json_app_data = json.loads(app_script.strip())
            except:
                pass

        # Find product and breadcrumb data
        product_data = None
        breadcrumb_data = None

        for data in json_ld_data:
            if isinstance(data, dict):
                data_type = data.get('@type')
                if data_type and 'Product' in str(data_type):
                    product_data = data
                elif data_type == 'BreadcrumbList':
                    breadcrumb_data = data
            elif isinstance(data, list):
                for item_data in data:
                    if isinstance(item_data, dict):
                        item_type = item_data.get('@type')
                        if item_type and 'Product' in str(item_type):
                            product_data = item_data
                        elif item_type == 'BreadcrumbList':
                            breadcrumb_data = item_data

        # FIXED: Always set all fields with fallbacks
        item['Ref Product URL'] = response.url
        item['Date Scrapped'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Extract each field with proper fallbacks
        item['Ref SKU'] = self.extract_sku(response) or ''
        item['Ref Product Name'] = self.extract_product_name(response) or ''
        item['Ref Price'] = self.extract_price(response) or ''
        item['Ref MPN'] = self.extract_mpn(response) or ''
        item['Ref GTIN'] = ''
        item['Ref Brand Name'] = self.extract_brand(response) or ''
        item['Ref Main Image'] = self.extract_main_image(response) or ''
        item['Ref Category'] = self.extract_category(response) or ''
        item['Ref Category URL'] = self.extract_category_url(response) or ''
        item['Ref Status'] = self.extract_status(response) or ''
        item['Ref Product ID'] = self.extract_product_id(response) or ''
        item['Ref Variant ID'] = ''
        item['Ref Group Attr 1'] = self.extract_group_attr1(response, 1) or ''
        item['Ref Group Attr 2'] = ''
        item['Ref Quantity'] = ''
        
        # Extract from App script
        item['Ref Images'] = self.extract_main_images(response) or ''
        item['Ref Dimensions'] = self.extract_dimensions(response) or ''
        item['Ref Highlights'] = self.extract_highlights(response) or ''

        # ALWAYS yield the item, even if some fields are empty
        self.logger.info(f"Yielding product: {item['Ref Product Name'] or 'Unknown'} - {response.url}")
        yield item

    # ORIGINAL EXTRACTORS - KEEP ALL OF THESE!
    
    def extract_product_name(self, response):
        # Try JSON-LD first
        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
            try:
                data = json.loads(script)
                if isinstance(data, dict):
                    if data.get('@type') == 'Product' or data.get('@type') == 'ProductGroup':
                        name = data.get('name')
                        if name:
                            return str(name)
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            if item.get('@type') == 'Product' or item.get('@type') == 'ProductGroup':
                                name = item.get('name')
                                if name:
                                    return str(name)
            except:
                continue
        
        # Fallback to XPath
        selectors = [
            '//*[@id="contentId"]/div/div[1]/div[2]/div[2]/h1/text()',
            '//h1/text()',
            '//meta[@property="og:title"]/@content'
        ]
        return self.extract_using_selectors(response, selectors)

    def extract_price(self, response):
        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
            try:
                data = json.loads(script)
                if isinstance(data, dict):
                    if data.get('@type') == 'Product' or data.get('@type') == 'ProductGroup':
                        offers = data.get('offers', {})
                        if isinstance(offers, dict) and 'price' in offers:
                            return str(offers['price'])
                        elif isinstance(offers, list) and len(offers) > 0:
                            return str(offers[0].get('price', ''))
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            if item.get('@type') == 'Product' or item.get('@type') == 'ProductGroup':
                                offers = item.get('offers', {})
                                if isinstance(offers, dict) and 'price' in offers:
                                    return str(offers['price'])
            except:
                continue
        return ''

    def extract_sku(self, response):
        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
            try:
                data = json.loads(script)
                if isinstance(data, dict):
                    if data.get('@type') == 'Product' or data.get('@type') == 'ProductGroup':
                        sku = data.get('sku')
                        if sku:
                            return str(sku)
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            if item.get('@type') == 'Product' or item.get('@type') == 'ProductGroup':
                                sku = item.get('sku')
                                if sku:
                                    return str(sku)
            except:
                continue
        return ''

    def extract_mpn(self, response):
        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
            try:
                data = json.loads(script)
                if isinstance(data, dict):
                    if data.get('@type') == 'Product' or data.get('@type') == 'ProductGroup':
                        mpn = data.get('mpn')
                        if mpn:
                            return str(mpn)
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            if item.get('@type') == 'Product' or item.get('@type') == 'ProductGroup':
                                mpn = item.get('mpn')
                                if mpn:
                                    return str(mpn)
            except:
                continue
        return ''

    def extract_gtin(self, response):
        return ''

    def extract_brand(self, response):
        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
            try:
                data = json.loads(script)
                if isinstance(data, dict):
                    if data.get('@type') == 'Product' or data.get('@type') == 'ProductGroup':
                        brand = data.get('brand', {})
                        if isinstance(brand, dict):
                            return brand.get('name', '')
                        else:
                            return str(brand)
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            if item.get('@type') == 'Product' or item.get('@type') == 'ProductGroup':
                                brand = item.get('brand', {})
                                if isinstance(brand, dict):
                                    return brand.get('name', '')
                                else:
                                    return str(brand)
            except:
                continue
        return ''

    def extract_main_image(self, response):
        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
            try:
                data = json.loads(script)
                if isinstance(data, dict):
                    if data.get('@type') == 'Product' or data.get('@type') == 'ProductGroup':
                        image = data.get('image')
                        if image:
                            if isinstance(image, list) and len(image) > 0:
                                return str(image[0])
                            return str(image)
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            if item.get('@type') == 'Product' or item.get('@type') == 'ProductGroup':
                                image = item.get('image')
                                if image:
                                    if isinstance(image, list) and len(image) > 0:
                                        return str(image[0])
                                    return str(image)
            except:
                continue
        return ''

    def extract_category(self, response):
        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
            try:
                data = json.loads(script)
                if isinstance(data, dict) and data.get('@type') == 'BreadcrumbList':
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
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and item.get('@type') == 'BreadcrumbList':
                            categories = []
                            for element in item.get('itemListElement', []):
                                item_data = element.get('item', {})
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
        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
            try:
                data = json.loads(script)
                if isinstance(data, dict) and data.get('@type') == 'BreadcrumbList':
                    urls = []
                    for item in data.get('itemListElement', []):
                        item_data = item.get('item', {})
                        url = item_data.get('@id', '')
                        if url:
                            urls.append(url)
                    if len(urls) >= 2:
                        return urls[-2]
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and item.get('@type') == 'BreadcrumbList':
                            urls = []
                            for element in item.get('itemListElement', []):
                                item_data = element.get('item', {})
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
        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
            try:
                data = json.loads(script)
                if isinstance(data, dict):
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
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            if item.get('@type') == 'Product' or item.get('@type') == 'ProductGroup':
                                offers = item.get('offers', {})
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
            return product_id
        return ''

    def extract_variant_id(self, response):
        return ''

    def extract_group_attr1(self, response, attr_num):
        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
            try:
                data = json.loads(script)
                if isinstance(data, dict):
                    if data.get('@type') == 'Product' or data.get('@type') == 'ProductGroup':
                        color = data.get('color')
                        if color:
                            return str(color)
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            if item.get('@type') == 'Product' or item.get('@type') == 'ProductGroup':
                                color = item.get('color')
                                if color:
                                    return str(color)
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
        for item in highlight_items:
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
            try:
                if json_script.startswith('<!--'):
                    json_script = json_script[4:]
                if json_script.endswith('-->'):
                    json_script = json_script[:-3]
                json_script = json_script.strip()
                data = json.loads(json_script)
                gallery = data.get('data', {}).get('content', {}).get('gallery', [])
                if isinstance(gallery, list):
                    for img in gallery:
                        if isinstance(img, dict):
                            original_url = img.get('original')
                            if original_url:
                                image_urls.append(original_url)
            except Exception as e:
                self.logger.debug(f"Failed to extract images: {e}")
        return '\n'.join(image_urls) if image_urls else ''

    def extract_dimensions(self, response):
        json_script = response.xpath('//script[@data-hypernova-key="App"]/text()').get()
        if not json_script:
            return ''

        try:
            if json_script.startswith('<!--'):
                json_script = json_script[4:]
            if json_script.endswith('-->'):
                json_script = json_script[:-3]
            json_script = json_script.strip()
            data = json.loads(json_script)
            content = data.get('data', {}).get('content', {})
            
            result = {}
            
            # Check multiple sources
            setIncludes = content.get('setIncludes', {})
            items = setIncludes.get('items', [])
            
            for item in items:
                if not isinstance(item, dict):
                    continue
                
                item_short_name = item.get('itemShortName', '')
                dimension = item.get('dimension', {})
                if not dimension or not item_short_name:
                    continue
                
                image_url = ''
                if isinstance(dimension.get('image'), dict):
                    image_url = dimension.get('image', {}).get('url', '')
                
                dimensions_list = dimension.get('list', [])
                dimension_data = [dim for dim in dimensions_list if dim and isinstance(dim, str)]
                
                if dimension_data:
                    result[item_short_name.lower()] = {
                        "url": image_url,
                        "data": dimension_data
                    }
            
            # Check additional items
            additional_items = content.get('additionalItems', {}).get('items', [])
            for item in additional_items:
                if not isinstance(item, dict):
                    continue
                
                item_short_name = item.get('itemShortName', '')
                dimension = item.get('dimension', {})
                if not dimension or not item_short_name:
                    continue
                
                image_url = ''
                if isinstance(dimension.get('image'), dict):
                    image_url = dimension.get('image', {}).get('url', '')
                
                dimensions_list = dimension.get('list', [])
                dimension_data = [dim for dim in dimensions_list if dim and isinstance(dim, str)]
                
                if dimension_data:
                    result[item_short_name.lower()] = {
                        "url": image_url,
                        "data": dimension_data
                    }
            
            # Check simple items
            simple_items = content.get('productLayouts', {}).get('simpleItems', [])
            if isinstance(simple_items, list):
                for item in simple_items:
                    if not isinstance(item, dict):
                        continue
                    
                    item_short_name = item.get('itemShortName', '')
                    dimension = item.get('dimension', {})
                    if not dimension or not item_short_name:
                        continue
                    
                    image_url = ''
                    if isinstance(dimension.get('image'), dict):
                        image_url = dimension.get('image', {}).get('url', '')
                    
                    dimensions_list = dimension.get('list', [])
                    dimension_data = [dim for dim in dimensions_list if dim and isinstance(dim, str)]
                    
                    if dimension_data:
                        result[item_short_name.lower()] = {
                            "url": image_url,
                            "data": dimension_data
                        }
            
            # Check configurables
            for item in items:
                if not isinstance(item, dict):
                    continue
                
                configurables = item.get('configurables', [])
                for config in configurables:
                    if not isinstance(config, dict):
                        continue
                    
                    for option in config.get('options', []):
                        if not isinstance(option, dict):
                            continue
                        
                        item_short_name = option.get('itemShortName', '')
                        dimension = option.get('dimension', {})
                        if not dimension or not item_short_name:
                            continue
                        
                        image_url = ''
                        if isinstance(dimension.get('image'), dict):
                            image_url = dimension.get('image', {}).get('url', '')
                        
                        dimensions_list = dimension.get('list', [])
                        dimension_data = [dim for dim in dimensions_list if dim and isinstance(dim, str)]
                        
                        if dimension_data:
                            result[item_short_name.lower()] = {
                                "url": image_url,
                                "data": dimension_data
                            }
            
            # Check accordion dimensions
            if not result:
                dimensions_data = content.get('accordion', {}).get('dimensions', {})
                if dimensions_data and isinstance(dimensions_data, dict):
                    dimension_list = dimensions_data.get('dimensionList', [])
                    image_url = ''
                    if isinstance(dimensions_data.get('image'), dict):
                        image_url = dimensions_data.get('image', {}).get('url', '')
                    
                    dimension_data = [dim for dim in dimension_list if dim and isinstance(dim, str)]
                    
                    if dimension_data:
                        result["dimensions"] = {
                            "url": image_url,
                            "data": dimension_data
                        }
            
            return json.dumps(result, indent=2) if result else ''
            
        except Exception as e:
            self.logger.debug(f"Error extracting dimensions: {e}")
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
        self.logger.error(f"Sitemap request failed: {failure.value}")

    def handle_product_error(self, failure):
        self.logger.error(f"Product page request failed: {failure.value}")
