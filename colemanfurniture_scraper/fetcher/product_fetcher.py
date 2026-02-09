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
            return False       
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
            yield from self.parse_product_page(response)
        else:
            return
    
    def parse_product_page(self, response):
        item = {}
        item['Ref Product URL'] = response.url
        item['Date Scrapped'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        item['Ref Product Name'] = self.extract_product_name(response)
        item['Ref Price'] = self.extract_price(response)
        item['Ref SKU'] = self.extract_sku(response)
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
        thumbnail_urls = self.extract_thumbnail_images(response)
        item['Ref Thumbnail Images'] = '\n'.join(thumbnail_urls) if thumbnail_urls else ''
        item['Ref Dimensions'] = self.extract_dimensions(response)
        highlights = self.extract_highlights(response)
        for i in range(1, 15):
            if i <= len(highlights):
                item[f'Highlight Header {i}'] = highlights[i-1]['title']
                item[f'Highlight Description {i}'] = highlights[i-1]['desc']
            else:
                item[f'Highlight Header {i}'] = ''
                item[f'Highlight Description {i}'] = ''
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
                if data.get('@type') == 'Product':
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
                if data.get('@type') == 'Product':
                    return data.get('sku', '')
            except:
                continue
    
    def extract_mpn(self, response):
        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
            try:
                data = json.loads(script)
                if data.get('@type') == 'Product':
                    return data.get('mpn', '')
            except:
                continue

    def extract_gtin(self, response):
        return ''
    
    def extract_brand(self, response):
        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
            try:
                data = json.loads(script)
                if data.get('@type') == 'Product':
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
                if data.get('@type') == 'Product':
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
                if data.get('@type') == 'Product':
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
                if data.get('@type') == 'Product':
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
        return highlights
    
    def extract_thumbnail_images(self, response):
        thumbnail_urls = []
        thumb_images = response.xpath('//img[contains(@class, "image-gallery-thumbnail-image")]/@src').getall()
        
        if not thumb_images:
            thumb_images = response.xpath('//div[contains(@class, "image-gallery-thumbnails")]//img/@src').getall()
        
        if not thumb_images:
            thumb_images = response.xpath('//button[contains(@class, "image-gallery-thumbnail")]//img/@src').getall()
        
        seen = set()
        for url in thumb_images:
            if url and url.strip():
                clean_url = url.strip()
                if clean_url not in seen:
                    seen.add(clean_url)
                    thumbnail_urls.append(clean_url)
        
        return thumbnail_urls

    def extract_dimensions(self, response):
        dimension_lines = []
        dimensions_section = response.xpath('//li[contains(@class, "accordion-item") and .//h2[contains(text(), "Dimensions")]]')
        if dimensions_section:
            dim_tables = dimensions_section.xpath('.//div[contains(@class, "product-dimensions")]//div[contains(@class, "product-info-table")]')
            for table in dim_tables:
                item_name = table.xpath('.//div[contains(@class, "spec-title")]/text()').get('')
                dimension_value = table.xpath('.//div[contains(@class, "spec-value")]/text()').get('')
                if item_name and dimension_value:
                    item_name = item_name.strip()
                    dimension_value = dimension_value.strip()
                    if item_name and dimension_value:
                        dimension_lines.append(f"Key: {item_name}, Value: {dimension_value}")
        return '\n'.join(dimension_lines)

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