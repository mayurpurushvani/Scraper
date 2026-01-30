from bs4 import BeautifulSoup
import re
from datetime import datetime

def parse_product(html, url):
    soup = BeautifulSoup(html, 'lxml')

    product_id = ''
    variant_id = ''
    main_category = ''
    category_url = ''
    brand_name = ''
    product_name = ''
    sku = ''
    mpn = ''
    gtin = ''
    price = ''
    main_image = ''
    quantity = 'In Stock'
    group_attr1 = ''
    group_attr2 = ''
    status = 'Active'
    
    js_match = re.search(r'window\.bvDCC\s*=\s*(\{.*?\});', html, re.DOTALL)
    if js_match:
        raw_json = js_match.group(1)
        
        product_id_match = re.search(r'"productId"\s*:\s*"([^"]+)"', raw_json)
        if product_id_match:
            product_id = product_id_match.group(1)
            variant_id = product_id
            
        product_name_match = re.search(r'"productName"\s*:\s*"([^"]{10,200})"', raw_json)
        if product_name_match:
            product_name = product_name_match.group(1)
            
        brand_match = re.search(r'"brandName"\s*:\s*"([^"]+)"', raw_json)
        if brand_match:
            brand_name = brand_match.group(1)
            
        mpn_match = re.search(r'"manufacturerPartNumbers"\s*:\s*\[\s*"([^"]+)"', raw_json)
        if mpn_match:
            mpn = mpn_match.group(1)
            sku = mpn
            
        image_match = re.search(r'"productImageURL"\s*:\s*"([^"]+)"', raw_json)
        if image_match:
            main_image = image_match.group(1)
        
    url_id_match = re.search(r'/([A-Z0-9]{6,8})\.htm$', url)
    if url_id_match and not product_id:
        product_id = url_id_match.group(1)
        variant_id = url_id_match.group(1)
    
    url_mpn_match = re.search(r'([A-Z0-9-]{6,20})\.htm$', url)
    if url_mpn_match:
        mpn = url_mpn_match.group(1)
        if not sku:
            sku = url_mpn_match.group(1)
    
    breadcrumbs = soup.select('ol.breadcrumb li a')
    categories = []
    category_urls = []

    for li in breadcrumbs:
        text = li.get_text(strip=True)
        href = li.get('href', '')
        if text and href and href != '/':
            categories.append(text)
            
            if href.startswith('http'):
                full_url = href
            elif href.startswith('/'):
                full_url = f"https://www.cymax.com{href}"
            else:
                full_url = f"https://www.cymax.com/{href}"
                
            category_urls.append(full_url)

    main_category = ' > '.join(categories) if categories else ''
    category_url = category_urls[-1] if category_urls else ''
    
    text_content = soup.get_text()
    
    if not product_name:
        for selector in ['h1', '.product-title', '[itemprop="name"]', '.pdp-title']:
            elem = soup.select_one(selector)
            if elem:
                product_name = elem.get_text(strip=True)
                break
    
    if not brand_name:
        for selector in ['.brand', '.manufacturer', '[itemprop="brand"]']:
            elem = soup.select_one(selector)
            if elem:
                brand_name = elem.get_text(strip=True)
                break
    
    if not price:
        for selector in ['.price', '[itemprop="price"]', '.product-price']:
            elem = soup.select_one(selector)
            if elem:
                price = elem.get_text(strip=True).strip()
                break
    
    if not main_image:
        for selector in ['meta[property="og:image"]', '.product-image img']:
            elem = soup.select_one(selector)
            if elem:
                main_image = elem.get('content') or elem.get('src') or elem.get('data-src') or ''
                break
    
    stock_text = text_content.lower()
    if any(word in stock_text for word in ['out of stock', 'sold out', 'unavailable']):
        quantity = 'Out of Stock'
    
    color_match = re.search(r'(Gray|Beige|White|Black|Brown|Espresso|Green|Gold)', url, re.I)
    group_attr1 = color_match.group(1) if color_match else ''
    
    size_match = re.search(r'(\d{2})\s*["\']', text_content)
    group_attr2 = size_match.group(1) if size_match else ''
    
    status = 'Active' if (price or product_name) else 'Inactive'
    
    result = {
        'Ref Product URL': url,
        'Ref Product ID': product_id,
        'Ref Varient ID': variant_id,
        'Ref Category': main_category,
        'Ref Category URL': category_url,
        'Ref Brand Name': brand_name,
        'Ref Product Name': product_name,
        'Ref SKU': sku,
        'Ref MPN': mpn,
        'Ref GTIN': gtin,
        'Ref Price': price,
        'Ref Main Image': main_image,
        'Ref Quantity': quantity,
        'Ref Group Attr 1': group_attr1,
        'Ref Group Attr 2': group_attr2,
        'Ref Status': status,
        'Date Scrapped': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    return result
