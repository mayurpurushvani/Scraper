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
        self.seen_urls = set()
        self.seen_skus = set()
        
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
        if response.url.endswith('.gz'):
            content = gzip.decompress(response.body)
            root = ET.fromstring(content)
        else:
            root = ET.fromstring(response.body)
        
        ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        all_urls = [url.text for url in root.findall('ns:url/ns:loc', ns) if url.text]
        
        if self.max_urls_per_sitemap > 0:
            all_urls = all_urls[:self.max_urls_per_sitemap]
        
        self.logger.info(f"Processing {len(all_urls)} URLs from sitemap")
        
        plp_count = 0
        pdp_count = 0
        
        for url in all_urls:
            if url in self.seen_urls:
                self.logger.info(f"Skipping duplicate URL: {url}")
                continue
            self.seen_urls.add(url)
            if self._is_plp_url(url):
                plp_count += 1
                continue
            pdp_count += 1
            yield Request(
                url,
                callback=self.parse_product_page_with_check,
                meta={'url': url},
                errback=self.handle_product_error
            )
        
        self.logger.info(f"Filtered {plp_count} PLP pages, {pdp_count} PDP pages to scrape")
    
    def _is_plp_url(self, url: str) -> bool:
        parsed_url = urlparse(url)
        path = parsed_url.path.strip('/')
        
        if not path:
            return True
        return '/' in path

    def parse_product_page_with_check(self, response):
        json_scripts = response.xpath('//script[@type="application/ld+json"]/text()').getall()
        has_product_json = False
        
        for script in json_scripts:
            try:
                data = json.loads(script.strip())
                if isinstance(data, dict) and data.get('@type') == 'Product':
                    has_product_json = True
                    break
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and item.get('@type') == 'Product':
                            has_product_json = True
                            break
                    if has_product_json:
                        break
            except:
                continue
        
        if has_product_json:
            items = list(self.parse_product_page(response))
            yield from self.extract_bundle_products(response, items[0] if items else None)
        else:
            return
    
    def extract_bundle_products(self, response, main_item):
        if main_item:
            yield main_item
        json_script = response.xpath('//script[@data-hypernova-key="App"]/text()').get()
        if not json_script:
            return
        try:
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
            for item in simple_items:
                if isinstance(item, dict):
                    sub_product_url = item.get('url')
                    item_short_name = item.get('itemShortName', '')
                    if not sub_product_url or sub_product_url == response.url:
                        self.logger.info(f"Skipping self-reference or empty URL: {sub_product_url}")
                        continue
                    if sub_product_url in self.seen_urls:
                        self.logger.info(f"Skipping duplicate URL: {sub_product_url}")
                        continue
                    self.seen_urls.add(sub_product_url)
                    self.logger.info(f"Found unique sub-product: {item_short_name}")
                    yield Request(
                        sub_product_url,
                        callback=self.parse_product_page_with_check,
                        meta={'url': sub_product_url},
                        errback=self.handle_product_error
                    )
        except Exception as e:
            self.logger.error(f"Error extracting bundle products: {e}")

    def parse_product_page(self, response):
        item = {}

        sku = self.extract_sku(response)
        if sku and sku in self.seen_skus:
            self.logger.info(f"Skipping duplicate product with SKU: {sku}")
            return
        if sku:
            self.seen_skus.add(sku)

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
        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
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
        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
            try:
                data = json.loads(script)
                if data.get('@type') == 'Product' or data.get('@type') == 'ProductGroup':
                    return data.get('sku', '')
            except:
                continue
    
    def extract_mpn(self, response):
        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
            try:
                data = json.loads(script)
                if data.get('@type') == 'Product' or data.get('@type') == 'ProductGroup':
                    return data.get('mpn', '')
            except:
                continue

    def extract_gtin(self, response):
        return ''
    
    def extract_brand(self, response):
        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
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
        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
            try:
                data = json.loads(script)
                if data.get('@type') == 'Product' or data.get('@type') == 'ProductGroup':
                    return data.get('image', '')
            except:
                continue
    
    def extract_category(self, response):
        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
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
        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
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
        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
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
            return product_id
        return ''
    
    def extract_variant_id(self, response):
        return ''
    
    def extract_group_attr1(self, response, attr_num):
        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
            try:
                data = json.loads(script)
                if data.get('@type') == 'Product' or data.get('@type') == 'ProductGroup':
                    return data.get('color', '')
            except:
                continue

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
        json_output = json.dumps(highlights, indent=2)
        return json_output
    
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
                    for img in gallery:
                        if isinstance(img, dict):
                            original_url = img.get('original')
                            if original_url:
                                image_urls.append(original_url)
            except Exception as e:
                self.logger.error(f"Failed to extract images: {e}")
                raise
        return '\n'.join(image_urls) if image_urls else ''

    def extract_dimensions(self, response):
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
            setIncludes = content.get('setIncludes', {})
            
            result = {}
            
            items = setIncludes.get('items', [])
            for item in items:
                if not isinstance(item, dict):
                    continue
                
                item_short_name = item.get('itemShortName', '')
                dimension = item.get('dimension', {})
                image_url = dimension.get('image', {}).get('url', '') if isinstance(dimension.get('image'), dict) else ''
                if image_url and not self.is_valid_image_url(image_url):
                    image_url = ''
                dimensions_list = dimension.get('list', [])
                
                dimension_data = []
                for dim in dimensions_list:
                    if dim and isinstance(dim, str):
                        dimension_data.append(dim)
                
                if item_short_name:
                    result[item_short_name.lower()] = {
                        "url": image_url,
                        "data": dimension_data if dimension_data else []
                    }
            
            for item in items:
                if not isinstance(item, dict):
                    continue
                
                configurables = item.get('configurables', [])
                for config in configurables:
                    if not isinstance(config, dict):
                        continue
                    
                    options = config.get('options', [])
                    for option in options:
                        if not isinstance(option, dict):
                            continue
                        
                        item_short_name = option.get('itemShortName', '')
                        dimension = option.get('dimension', {})
                        image_url = dimension.get('image', {}).get('url', '') if isinstance(dimension.get('image'), dict) else ''
                        if image_url and not self.is_valid_image_url(image_url):
                            image_url = ''
                        dimensions_list = dimension.get('list', [])
                        
                        dimension_data = []
                        for dim in dimensions_list:
                            if dim and isinstance(dim, str):
                                dimension_data.append(dim)
                        
                        if item_short_name:
                            result[item_short_name.lower()] = {
                                "url": image_url,
                                "data": dimension_data if dimension_data else []
                            }
            
            additional_items_data = content.get('additionalItems', {})
            if isinstance(additional_items_data, dict):
                additional_items = additional_items_data.get('items', [])
                for item in additional_items:
                    if not isinstance(item, dict):
                        continue
                    
                    item_short_name = item.get('itemShortName', '')
                    dimension = item.get('dimension', {})
                    image_url = dimension.get('image', {}).get('url', '') if isinstance(dimension.get('image'), dict) else ''
                    if image_url and not self.is_valid_image_url(image_url):
                        image_url = ''
                    dimensions_list = dimension.get('list', [])
                    
                    # Get dimension text
                    dimension_data = []
                    for dim in dimensions_list:
                        if dim and isinstance(dim, str):
                            dimension_data.append(dim)
                    
                    # Only add if we have itemShortName
                    if item_short_name:
                        result[item_short_name.lower()] = {
                            "url": image_url,
                            "data": dimension_data if dimension_data else []
                        }

            simpleItems = content.get('productLayouts', {}).get('simpleItems', {})
            if isinstance(simpleItems, list):
                for item in simpleItems:
                    if not isinstance(item, dict):
                        continue
                    item_short_name = item.get('itemShortName', '')
                    dimension = item.get('dimension', {})
                    image_url = dimension.get('image', {}).get('url', '') if isinstance(dimension.get('image'), dict) else ''
                    if image_url and not self.is_valid_image_url(image_url):
                        image_url = ''
                    dimensions_list = dimension.get('list', [])
                    
                    dimension_data = []
                    for dim in dimensions_list:
                        if dim and isinstance(dim, str):
                            dimension_data.append(dim)
                    
                    if item_short_name:
                        result[item_short_name.lower()] = {
                            "url": image_url,
                            "data": dimension_data if dimension_data else []
                        }

            if not result:
                accordion_data = content.get('accordion', {})
                dimensions_data = accordion_data.get('dimensions', {})
                
                if dimensions_data and isinstance(dimensions_data, dict):
                    dimension_list = dimensions_data.get('dimensionList', [])
                    
                    image_url = dimensions_data.get('image', {}).get('url', '') if isinstance(dimensions_data.get('image'), dict) else ''
                    if image_url and not self.is_valid_image_url(image_url):
                        image_url = ''
                    
                    dimension_data = []
                    for dim in dimension_list:
                        if dim and isinstance(dim, str):
                            dimension_data.append(dim)
                    
                    if dimension_data:
                        result["dimensions"] = {
                            "url": image_url,
                            "data": dimension_data
                        }
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
        self.logger.error(f"Sitemap request failed: {failure.value}")
    
    def handle_product_error(self, failure):
        self.logger.error(f"Product page request failed: {failure.value}")