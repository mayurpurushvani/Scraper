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
                priority=100  # Higher priority for PDPs
            ))

        # Yield all requests at once
        for req in requests:
            yield req

        self.logger.info(f"Filtered {plp_count} PLP pages, {pdp_count} PDP pages to scrape")

    def _is_plp_url(self, url: str) -> bool:
        """Optimized PLP detection - runs in O(1) with early returns"""
        parsed_url = urlparse(url)
        path = parsed_url.path.strip('/')

        if not path:
            return True

        # Quick extension check
        if not path.endswith('.htm'):
            return True

        # Fast string operations - no regex unless necessary
        path_lower = path.lower()

        # Common PLP keywords for Coleman Furniture
        if any(x in path_lower for x in (
                'furniture', 'sets', 'clearance', 'sale', 'new', 'shop',
                'bedroom', 'living-room', 'dining-room', 'office',
                'outdoor', 'kids', 'baby', 'collection', 'brand',
                'category', 'department', 'deals', 'closeouts', 'best-sellers'
        )):
            return True

        # Quick length check - PDPs are usually longer
        if len(path_lower.split('-')) <= 4:
            return True

        # Check for product codes - but do this last as it's slower
        if re.search(r'\d{4,}|[a-z]+\d{3,}', path_lower):
            return False

        return False  # Default to PDP if uncertain

    def parse_product_page_with_check(self, response):
        """Process product pages with optimized JSON parsing"""
        json_scripts = response.xpath('//script[@type="application/ld+json"]/text()').getall()
        has_product_json = False

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
            except Exception:
                continue

        if has_product_json:
            self.logger.debug(f"Found Product JSON-LD for {response.url}")
        else:
            self.logger.debug(f"No Product JSON-LD found for {response.url}")

        # Always process the page
        yield from self.parse_product_page(response)
        yield from self.extract_bundle_products(response)

    def extract_bundle_products(self, response):
        """Optimized bundle extraction - skip if no App script or large response"""
        # Quick check - skip if response is too large (not a bundle page)
        if len(response.body) > 500000:  # 500KB
            json_script = response.xpath('//script[@data-hypernova-key="App"]/text()').get()
            if not json_script or 'simpleItems' not in json_script:
                return

            try:
                # Faster JSON parsing with minimal operations
                if json_script.startswith('<!--'):
                    json_script = json_script[json_script.find('{'):json_script.rfind('}') + 1]

                data = json.loads(json_script)
                simple_items = data.get('data', {}).get('content', {}).get('productLayouts', {}).get('simpleItems', [])

                if not simple_items:
                    return

                # Process in batch
                for item in simple_items:
                    if isinstance(item, dict):
                        sub_url = item.get('url')
                        if sub_url and sub_url != response.url:
                            # Skip if URL is clearly PLP
                            if self._is_plp_url(sub_url):
                                continue

                            self.logger.debug(f"Bundle sub-product: {item.get('itemShortName', '')}")
                            yield Request(
                                sub_url,
                                callback=self.parse_product_page_with_check,
                                meta={'url': sub_url},
                                errback=self.handle_product_error,
                                priority=10  # Lower priority than main products
                            )
            except Exception as e:
                self.logger.debug(f"Error extracting bundle products: {e}")

    def parse_product_page(self, response):
        """Parse product page with single-pass JSON-LD parsing"""
        item = {}

        # Pre-parse JSON-LD once
        json_ld_data = []
        json_app_data = None

        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
            try:
                data = json.loads(script.strip())
                json_ld_data.append(data)
            except:
                continue

        # Pre-parse App script once
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

        # Extract all fields using pre-parsed data
        item['Ref Product URL'] = response.url
        item['Date Scrapped'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Single pass extraction
        product_data = None
        breadcrumb_data = None

        # Find product and breadcrumb data once
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

        # Extract fields using cached data
        item['Ref SKU'] = self._extract_sku_optimized(product_data, response)
        item['Ref Product Name'] = self._extract_product_name_optimized(product_data, response)
        item['Ref Price'] = self._extract_price_optimized(product_data, response)
        item['Ref MPN'] = self._extract_mpn_optimized(product_data, response)
        item['Ref GTIN'] = self._extract_gtin_optimized(product_data, response)
        item['Ref Brand Name'] = self._extract_brand_optimized(product_data, response)
        item['Ref Main Image'] = self._extract_main_image_optimized(product_data, response)
        item['Ref Category'] = self._extract_category_optimized(breadcrumb_data, response)
        item['Ref Category URL'] = self._extract_category_url_optimized(breadcrumb_data, response)
        item['Ref Status'] = self._extract_status_optimized(product_data, response)
        item['Ref Product ID'] = self._extract_product_id_optimized(response)
        item['Ref Variant ID'] = ''
        item['Ref Group Attr 1'] = self._extract_group_attr1_optimized(product_data, response)
        item['Ref Group Attr 2'] = ''
        item['Ref Quantity'] = ''

        # Extract from App script (pre-parsed)
        if json_app_data:
            item['Ref Images'] = self._extract_main_images_optimized(json_app_data, response)
            item['Ref Dimensions'] = self._extract_dimensions_optimized(json_app_data, response)
            item['Ref Highlights'] = self._extract_highlights_optimized(response)
        else:
            item['Ref Images'] = ''
            item['Ref Dimensions'] = ''
            item['Ref Highlights'] = self._extract_highlights_optimized(response)

        yield item

    # Optimized extractor methods
    def _extract_sku_optimized(self, product_data, response):
        if product_data:
            sku = product_data.get('sku')
            if sku:
                return str(sku)
        return ''

    def _extract_product_name_optimized(self, product_data, response):
        if product_data:
            name = product_data.get('name')
            if name:
                return str(name)
        # Fallback to fast XPath
        result = response.xpath('//h1/text()').get()
        if result and result.strip():
            return result.strip()
        result = response.xpath('//*[@id="contentId"]/div/div[1]/div[2]/div[2]/h1/text()').get()
        return result.strip() if result else ''

    def _extract_price_optimized(self, product_data, response):
        if product_data:
            offers = product_data.get('offers', {})
            if isinstance(offers, dict) and 'price' in offers:
                return str(offers['price'])
        return ''

    def _extract_mpn_optimized(self, product_data, response):
        if product_data:
            mpn = product_data.get('mpn')
            if mpn:
                return str(mpn)
        return ''

    def _extract_gtin_optimized(self, product_data, response):
        return ''

    def _extract_brand_optimized(self, product_data, response):
        if product_data:
            brand = product_data.get('brand', {})
            if isinstance(brand, dict):
                return brand.get('name', '')
            else:
                return str(brand)
        return ''

    def _extract_main_image_optimized(self, product_data, response):
        if product_data:
            image = product_data.get('image')
            if image:
                return str(image)
        return ''

    def _extract_category_optimized(self, breadcrumb_data, response):
        if breadcrumb_data:
            categories = []
            for item in breadcrumb_data.get('itemListElement', []):
                item_data = item.get('item', {})
                name = item_data.get('name', '')
                if name and name.lower() not in ['home', 'shop', 'all']:
                    categories.append(name)
            if len(categories) > 1:
                categories = categories[:-1]
            if categories:
                return ' > '.join(categories)
        return ''

    def _extract_category_url_optimized(self, breadcrumb_data, response):
        if breadcrumb_data:
            urls = []
            for item in breadcrumb_data.get('itemListElement', []):
                item_data = item.get('item', {})
                url = item_data.get('@id', '')
                if url:
                    urls.append(url)
            if len(urls) >= 2:
                return urls[-2]
        return ''

    def _extract_status_optimized(self, product_data, response):
        if product_data:
            offers = product_data.get('offers', {})
            if isinstance(offers, dict):
                availability = str(offers.get('availability', '')).lower()
                if 'instock' in availability:
                    return 'Active'
                elif 'outofstock' in availability or 'soldout' in availability:
                    return 'Out of Stock'
                elif 'preorder' in availability:
                    return 'Active'
        return ''

    def _extract_product_id_optimized(self, response):
        product_id = response.xpath('//div[@data-id]/@data-id').get()
        if product_id:
            return product_id
        return ''

    def _extract_group_attr1_optimized(self, product_data, response):
        if product_data:
            color = product_data.get('color')
            if color:
                return str(color)
        return ''

    def _extract_main_images_optimized(self, json_app_data, response):
        image_urls = []
        if json_app_data:
            try:
                gallery = json_app_data.get('data', {}).get('content', {}).get('gallery', [])
                if isinstance(gallery, list):
                    for img in gallery:
                        if isinstance(img, dict):
                            original_url = img.get('original')
                            if original_url:
                                image_urls.append(original_url)
            except:
                pass
        return '\n'.join(image_urls) if image_urls else ''

    def _extract_dimensions_optimized(self, json_app_data, response):
        if not json_app_data:
            return ''

        try:
            content = json_app_data.get('data', {}).get('content', {})
            result = {}

            # Check multiple sources for dimensions
            dimension_sources = [
                content.get('setIncludes', {}).get('items', []),
                content.get('additionalItems', {}).get('items', []),
                content.get('productLayouts', {}).get('simpleItems', [])
            ]

            for source in dimension_sources:
                if isinstance(source, list):
                    for item in source:
                        if not isinstance(item, dict):
                            continue

                        item_short_name = item.get('itemShortName', '')
                        dimension = item.get('dimension', {})
                        if not dimension or not item_short_name:
                            continue

                        image_url = dimension.get('image', {}).get('url', '') if isinstance(
                            dimension.get('image'), dict) else ''
                        if image_url and not self.is_valid_image_url(image_url):
                            image_url = ''

                        dimensions_list = dimension.get('list', [])
                        dimension_data = [dim for dim in dimensions_list if dim and isinstance(dim, str)]

                        if dimension_data:
                            result[item_short_name.lower()] = {
                                "url": image_url,
                                "data": dimension_data
                            }

            # Check configurables
            for item in content.get('setIncludes', {}).get('items', []):
                if isinstance(item, dict):
                    for config in item.get('configurables', []):
                        if isinstance(config, dict):
                            for option in config.get('options', []):
                                if isinstance(option, dict):
                                    item_short_name = option.get('itemShortName', '')
                                    dimension = option.get('dimension', {})
                                    if not dimension or not item_short_name:
                                        continue

                                    image_url = dimension.get('image', {}).get('url', '') if isinstance(
                                        dimension.get('image'), dict) else ''
                                    if image_url and not self.is_valid_image_url(image_url):
                                        image_url = ''

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
                    image_url = dimensions_data.get('image', {}).get('url', '') if isinstance(
                        dimensions_data.get('image'), dict) else ''
                    if image_url and not self.is_valid_image_url(image_url):
                        image_url = ''

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

    def _extract_highlights_optimized(self, response):
        """Optimized highlights extraction"""
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

    # Original methods kept for backward compatibility
    def extract_product_name(self, response):
        return self._extract_product_name_optimized(None, response)

    def extract_price(self, response):
        return self._extract_price_optimized(None, response)

    def extract_sku(self, response):
        return self._extract_sku_optimized(None, response)

    def extract_mpn(self, response):
        return self._extract_mpn_optimized(None, response)

    def extract_gtin(self, response):
        return ''

    def extract_brand(self, response):
        return self._extract_brand_optimized(None, response)

    def extract_main_image(self, response):
        return self._extract_main_image_optimized(None, response)

    def extract_category(self, response):
        return self._extract_category_optimized(None, response)

    def extract_category_url(self, response):
        return self._extract_category_url_optimized(None, response)

    def extract_quantity(self, response):
        return ''

    def extract_status(self, response):
        return self._extract_status_optimized(None, response)

    def extract_product_id(self, response):
        return self._extract_product_id_optimized(response)

    def extract_variant_id(self, response):
        return ''

    def extract_group_attr1(self, response, attr_num):
        return self._extract_group_attr1_optimized(None, response)

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
        return self._extract_highlights_optimized(response)

    def extract_main_images(self, response):
        return ''

    def extract_dimensions(self, response):
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