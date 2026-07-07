import json, os
import re
import time
import requests
import argparse
# from decorator import append
# from dropbox.file_requests import update
from scrapy import Selector
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
from bson.objectid import ObjectId
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
client = MongoClient(os.getenv('MONGO_URI'))
db = client[os.getenv('DB_NAME')]
collection = db['log_master_pdp']
proxy_log_table = db['3159_proxy_log']

# Todo change scrape do token
# proxy_pathh = rf'D:\Projects\Ronak\Myntra\proxy.txt'
proxy_pathh = os.getenv('PROXY_PATH')
# scrape_do_token = open(proxy_pathh).read()
# proxyModeUrl = f"http://{scrape_do_token}:@proxy.scrape.do:8080"
# proxies = {
#         "http":proxyModeUrl,
#         "https":proxyModeUrl,
#         }

# ========== Config ==========
PROXY_TYPE = 'scrape-do'  # or  'scraperapi'

PROXY_CONFIG = {
        'scraperapi':{
                'token':os.getenv("SCRAPER_API_TOKEN"),
                'base_url':'http://api.scraperapi.com'
                },
        'scrape-do':{
                # 'token': '8db327520ebf45abbe7823d182325a87dc5e2de3b3a',
                'token':f'{os.getenv("SCRAPE_DO_TOKEN")}',
                'base_url':'http://api.scrape.do'
                }
        }

proxy_settings = PROXY_CONFIG[PROXY_TYPE]
TOKEN = proxy_settings['token']
BASE_URL = proxy_settings['base_url']

request_consumed = 0

def c_replace(html=''):
    if isinstance(html, str):
        # HTML entity replacements
        html_entities = {
                "&#233;":"é", "&#224;":"à", "&#225;":"á", "&#226;":"â", "&#227;":"ã",
                "&#228;":"ä", "&#229;":"å", "&#230;":"æ", "&#231;":"ç", "&#232;":"è",
                "&#234;":"ê", "&#235;":"ë", "&#236;":"ì", "&#237;":"í", "&#238;":"î",
                "&#239;":"ï", "&#240;":"ð", "&#241;":"ñ", "&#242;":"ò", "&#243;":"ó",
                "&#244;":"ô", "&#245;":"õ", "&#246;":"ö", "&#248;":"ø", "&#249;":"ù",
                "&#250;":"ú", "&#251;":"û", "&#252;":"ü", "&#253;":"ý", "&#254;":"þ",
                "&#255;":"ÿ", "&#38;":"&", "&#60;":"<", "&#62;":">", "&#34;":"\"",
                "&#39;":"'", "&#160;":" ", "&nbsp;":" ", "&gt;":">", "&lt;":"<",
                "&ge;":">=", "&quot;":"\"", "&amp;":"&"
                }
        for k, v in html_entities.items():
            html = html.replace(k, v)

        # Remove all HTML tags
        html = re.sub(r'<[^>]+>', ' ', html)

        # Normalize whitespace
        html = re.sub(r'[\r\n\t\\]+', ' ', html)
        html = re.sub(r' +', ' ', html).strip()

        return html

    elif isinstance(html, list):
        return [c_replace(i) for i in html if i]
    else:
        raise TypeError(f"Expected str or list, got {type(html)}")

# ========== Utilities ==========

def make_request(url, method='GET', headers=None, json_data=None, retries=3, request_consumed=None):
    # request_consumed = 1
    for _ in range(retries):
        try:
            if method.upper() == 'GET':
                response = requests.get(url, headers=headers)
                request_consumed += 1
            else:
                response = requests.post(url, headers=headers, json=json_data)
                request_consumed += 1

            if response.status_code == 200:
                return response, request_consumed
        except requests.RequestException:
            time.sleep(1)
    return None

def parse_cookies(set_cookie_header):
    cookie_parts = re.split(r', (?=[^ ;]+=)', set_cookie_header)
    cookie_dict = {}
    for part in cookie_parts:
        main = part.split(';')[0]
        if '=' in main:
            k, v = main.split('=', 1)
            cookie_dict[k.strip()] = v.strip()
    return cookie_dict

def get_cookies(counter1, retry_counter, job_id):

    if PROXY_TYPE == 'scraperapi':
        url = (
                f"{BASE_URL}?api_key={TOKEN}"
                f"&url=https://www.myntra.com"
                f"&country_code=in&keep_headers=true&pureCookies=true"
        )
    else:  # scrape-do
        url = (
                f"{BASE_URL}?token={TOKEN}"
                f"&url=https://www.myntra.com"
                f"&pureCookies=true"
        )
    for _ in range(10):
        try:
            response = requests.get(url)
            retry_counter += 1
            proxy_log = {
                    "timestamp":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "api_url":url,
                    "url":url,
                    "proxy_key":str(TOKEN),
                    "proxy_name":PROXY_TYPE,
                    "request_payload":"",
                    "retry_count":retry_counter,
                    "status":"Done" if response.status_code == 200 else "Failed",
                    "status_code":response.status_code,
                    "response_headers":dict(response.headers),
                    "platform_name":'Myntra',
                    "request_name":"PDP",
                    "extra":"cookie request",
                    "job_id":job_id,
                    }
            try:
                proxy_log_table.insert_one(proxy_log)
            except:
                pass
            if response.status_code == 200:
                # counter1 += 1
                cookie_str = response.headers.get("Set-Cookie", "")
                cookie_dict = parse_cookies(cookie_str)
                return cookie_dict, cookie_str, counter1, retry_counter
        except:
            pass
    return {}, "", counter1, retry_counter

# ========== Scraping Logic ==========

def fetch_myntra_product(product_data):
    scraped_at_time = datetime.now().isoformat()
    _id = product_data.get('_id')
    job_id = product_data.get('Job_Id')
    product_url = product_data.get('product_url')

    counter1, retry_counter = 0, 0

    cookies, _, cookie_counter1, cookie_retry_counter = get_cookies(counter1, retry_counter, job_id)
    counter1 += cookie_counter1
    retry_counter += cookie_retry_counter
    if not cookies:
        print("❌ Failed to fetch cookies")
        return {}

    if PROXY_TYPE == 'scraperapi':
        url = (
                f"{BASE_URL}?api_key={TOKEN}"
                f"&url={product_url}"
                f"&country_code=in&keep_headers=true&pureCookies=true"
        )
    else:
        url = (
                f"{BASE_URL}?token={TOKEN}"
                f"&url={product_url}"
                f"&pureCookies=true"
        )

    headers = {
            "User-Agent":"Mozilla/5.0",
            "Accept":"application/json, text/html",
            }

    status_code = 500
    response_text = "{}"

    for i in range(10):
        try:
            try:
                with open("log.txt", "a") as f:
                    f.write(f"requestiitititing Myntra...{datetime.now()}" + "" + "\n")
            except:
                pass
            response = requests.get(url, headers=headers, cookies=cookies)
            response_text = response.text
            retry_counter += 1
            proxy_log = {
                    "timestamp":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "api_url":url,
                    "url":product_url,
                    "proxy_key":str(TOKEN),
                    "proxy_name":PROXY_TYPE,
                    "request_payload":"",
                    "retry_count":retry_counter,
                    "status":"Done" if response.status_code == 200 else "Failed",
                    "status_code":response.status_code,
                    "response_headers":dict(response.headers),
                    "platform_name":'Myntra',
                    "request_name":"PDP",
                    "extra":"main page request",
                    "job_id":job_id,
                    }
            try:
                proxy_log_table.insert_one(proxy_log)
            except:
                pass
            if response.status_code == 200:
                status_code = response.status_code
                counter1 += 1
                break
        except Exception as e:
            print(e)

    try:
        product_id = product_url.split("/")[-2]
    except:
        product_id = "product"
    # folder_name = fr"\\192.168.3.216\D\Myntra_html\{job_id}"
    # if not os.path.exists(folder_name):
    #     os.makedirs(folder_name)
    # file_name = f"Main_{product_id}_{job_id}.html"
    # page_save_path = os.path.join(folder_name, file_name)
    page_save_path = ""

    # try:
    #     with open(page_save_path, "w", encoding='utf-8') as f:
    #         f.write(response_text)
    # except Exception as e:
    #     page_save_path = str(e)
    #     print("page save issue", e)

    if status_code == 200:

        selector = Selector(text=response_text)
        item = {
                'request_consumed':'',
                'breadcrumbs':'',
                'title':'',
                'variant_options':[],
                'rating':'',
                "rating_counts":'',
                'mrp':'',
                'main_images':'',
                'variants':[],
                'selling_price':'',
                'category_levels':'',
                'sku':'',
                'weight':'',
                'size':'',
                'color':'',
                'dimensions_(lxbxh)':'',
                'brand':'',
                'product_description':'',
                'key_benefits':'',
                'key_features':'',
                'key_ingredients':'',
                'how_to_use':'',
                'about_the_product':'',
                'seller_information':'',
                'delivery_timelines':'',
                'stock_status':'',
                'moq_(maximum_order_quantity)':10,
                'product_delivery_sla':'',
                'location':'',
                'a1_content_with_images':'',
                'unit_price':'',
                'source_url':'',
                'scrapedat':'',

                }

        try:
            # ------------------ Breadcrumbs ------------------
            breadcrumbs = []
            try:
                breadcrumbs_json = selector.xpath(
                        "//script[@type='application/ld+json'][contains(text(),'BreadcrumbList')]/text()"
                        ).get()
                if breadcrumbs_json:
                    breadcrumbs_data = json.loads(breadcrumbs_json)
                    breadcrumbs = [b.get('item', {}).get('name', 'Unknown')
                                   for b in breadcrumbs_data.get('itemListElement', [])]
            except Exception:
                pass

            item['breadcrumbs'] = ' > '.join(breadcrumbs)
            item['category_levels'] = ' > '.join(breadcrumbs)
            item['source_url'] = product_url
            item['scrapedat'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

            # ------------------ Product Data ------------------
            try:
                product_json = selector.xpath("//script[contains(text(),'pdpData')]/text()").get()
                product_data = json.loads(product_json.split('= ', 1)[-1]) if product_json else {}
            except Exception:
                product_data = {}

            pdp = product_data.get('pdpData', {})

            item['title'] = c_replace(pdp.get('name', ''))
            # item['rating'] = round(pdp.get('ratings', {}).get('averageRating', 0), 1) ##########
            item['rating'] = float(round(float(pdp.get('ratings', {}).get('averageRating', 0) or 0), 1))
            try:
                mrp_price = pdp.get('price', {}).get('mrp', 0)
                if mrp_price:
                    mrp_price = float(mrp_price)
            except:
                mrp_price = 0
            # item['mrp'] = mrp_price   #########
            item['mrp'] = float(mrp_price or 0)
            item['brand'] = pdp.get('brand', {}).get('name', '')
            # item['sku'] = pdp.get('id', '')  #########
            item['sku'] = str(pdp.get('id', '') or '')

            item['stock_status'] = 'Out of Stock' if pdp.get('flags', {}).get('outOfStock') else 'In Stock'
            try:
                selling_price = pdp.get('price', {}).get('discounted', 0)
                if selling_price:
                    selling_price = float(selling_price)
            except:
                selling_price = 0
            # item['selling_price'] = selling_price    #########
            item['selling_price'] = float(selling_price or 0)

            try:
                Ingredients = pdp.get('articleAttributes', {}).get('Key Ingredients', '')
                if not Ingredients:
                    Ingredients = pdp.get('articleAttributes', {}).get('Ingredients', '')
                if Ingredients:
                    item['key_ingredients'] = c_replace(Ingredients)
                else:
                    item['key_ingredients'] = ""
            except:
                item['key_ingredients'] = ""
                print("error in Ingredients")

            try:
                Review = pdp.get('ratings').get('totalCount')
                if not Review:
                    Review = 0
                item['rating_counts'] = int(Review)
            except:
                item['rating_counts'] = ''

            # item['key_ingredients'] = c_replace(pdp.get('articleAttributes', {}).get('Key Ingredients', ''))
            # ------------------ Product Description ------------------
            product_dec, sizeDimension, key_f = '', '', ''

            for detail in pdp.get('productDetails', []):
                title, desc = detail.get('title', ''), detail.get('description', '')
                if title == 'Product Details':
                    product_dec = c_replace(desc).replace('<b>', '') if desc else ''
                    if 'Features:' in product_dec:
                        key_f = product_dec.split('Features:')[-1]
                elif title == 'SIZE & FIT':
                    if desc and 'Dimension:' in desc:
                        sizeDimension = c_replace(desc).replace('Dimension:', '')
                    elif all(x in desc for x in ['Height', 'Width', 'Length']):
                        height = re.search(r'Height:\s*([\d.]+)', desc)
                        width = re.search(r'Width:\s*([\d.]+)', desc)
                        length = re.search(r'Length:\s*([\d.]+)', desc)
                        if height and width and length:
                            sizeDimension = f"{length.group(1)}x{width.group(1)}x{height.group(1)}"

            try:
                product_dec = ".".join([f"{item['description']}" for item in pdp.get('productDetails', [])])
            except:
                product_dec = ''
            if product_dec:
                product_dec = product_dec.strip()

            item['product_description'] = product_dec
            item['about_the_product'] = product_dec
            item['dimensions_(lxbxh)'] = sizeDimension
            item['key_features'] = key_f

            item['seller_information'] = ' | '.join([s.get('sellerName', '') for s in pdp.get('sellers', [])])

            # ------------------ Images ------------------
            images_album = pdp.get('media', {}).get('albums', [])
            default_album = next((album for album in images_album if album.get('name') == 'default'), {})
            images = [img.get('imageURL') for img in default_album.get('images', [])]
            item['main_images'] = ' || '.join(images)

            # ------------------ Variants ------------------
            color = pdp.get('baseColour', '')
            item['color'] = color

            warehouses, variant_list = [], []
            sizes = pdp.get('sizes', [])
            first_size_value = sizes[0].get('label', '') if sizes else ''
            for s in sizes:
                s_id = s.get('styleId', '')
                s_sku = s.get('skuId', '')
                s_size = s.get('label', '')
                s_varient_list = []

                if first_size_value and first_size_value == s_size:
                    continue
                counter1 += 1
                size_option = {'option_name':'Size',
                               'option_value':s_size
                               }
                color_option = {'option_name':'Color',
                                'option_value':color
                                }
                s_varient_list.append(size_option)
                s_varient_list.append(color_option)
                try:
                    s_price = (s.get('sizeSellerData', [{}])[0]).get('mrp', 0)
                    if s_price:
                        s_price = float(s_price)
                except:
                    s_price = 0
                try:
                    s_s_price = (s.get('sizeSellerData', [{}])[0]).get('discountedPrice',
                                                                       '')  # 0.sizeSellerData[0].discountedPrice
                    if s_s_price:
                        s_s_price = float(s_s_price)
                except:
                    s_s_price = 0
                s_stock = 'In Stock' if s.get('available') else 'Out Of Stock'
                if 'Out Of Stock' == s_stock:
                    moq = 0
                else:
                    moq = 10

                if s_sku != item['sku']:
                    variant_list.append({
                            'breadcrumbs':' > '.join(breadcrumbs),
                            'title':c_replace(pdp.get('name', '')),
                            'variant_options':s_varient_list,
                            # 'rating': round(pdp.get('ratings', {}).get('averageRating', 0), 1),  #######
                            'rating':float(round(float(pdp.get('ratings', {}).get('averageRating', 0) or 0), 1)),
                            'rating_counts':int(pdp.get('ratings', {}).get('totalCount', 0)),
                            # 'mrp': s_price,  ####
                            'mrp':float(s_price or 0),
                            'size':s_size,
                            'main_images':item['main_images'],
                            # 'selling_price': s_s_price,  #######
                            'selling_price':float(s_s_price or 0),
                            'category_levels':' > '.join(breadcrumbs),
                            # 'sku': s_sku,  #####
                            'sku':str(s_sku or ''),
                            'weight':'',
                            'color':color,
                            'dimensions_(lxbxh)':sizeDimension,
                            'brand':pdp.get('brand', {}).get('name', ''),
                            'product_description':product_dec,
                            'key_benefits':'',
                            'key_features':key_f,
                            'key_ingredients':Ingredients,
                            'how_to_use':'',
                            'about_the_product':product_dec,
                            'seller_information':' | '.join([s.get('sellerName', '') for s in pdp.get('sellers', [])]),
                            'delivery_timelines':'',
                            'stock_status':s_stock,
                            'moq_(maximum_order_quantity)':moq,
                            'product_delivery_sla':'',
                            'location':'110020',
                            'a1_content_with_images':'',
                            'unit_price':'',
                            'source_url_(at_variant_level)':f'https://www.myntra.com/{s_id}?skuId={s_sku}',
                            # 'scrapedat': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
                            })

            # Color Variants
            # colors = pdp.get('colours', [])
            # if colors:
            #     for c in colors:
            #
            #
            #         v_url = f'https://www.myntra.com/{c.get("url", "")}'
            #
            #         if PROXY_TYPE == 'scraperapi':
            #             url = (
            #                 f"{BASE_URL}?api_key={TOKEN}"
            #                 f"&url={v_url}"
            #                 f"&country_code=in&keep_headers=true&pureCookies=true"
            #             )
            #         else:
            #             url = (
            #                 f"{BASE_URL}?token={TOKEN}"
            #                 f"&url={v_url}"
            #                 f"&pureCookies=true"
            #             )
            #
            #
            #             v_response = requests.get(url, headers=headers, cookies=cookies)
            #
            #             v_selector = Selector(text=v_response.text)
            #
            #             try:
            #                 v_product_json = v_selector.xpath("//script[contains(text(),'pdpData')]/text()").get()
            #                 v_product_json = json.loads(v_product_json.split('= ', 1)[-1]) if v_product_json else {}
            #             except Exception:
            #                 v_product_json = {}
            #
            #             v_pdp = v_product_json.get('pdpData', {})
            #
            #             v_images_album = v_pdp.get('media', {}).get('albums', [])
            #             v_default_album = next((v_album for v_album in v_images_album if v_album.get('name') == 'default'), {})
            #             v_images = [v_img.get('imageURL') for v_img in v_default_album.get('images', [])]
            #
            #             # ------------------ Product Description ------------------
            #             v_product_dec, v_sizeDimension, v_key_f = '', '', ''
            #             for v_detail in v_pdp.get('productDetails', []):
            #                 v_title, v_desc = v_detail.get('title', ''), v_detail.get('description', '')
            #                 if v_title == 'Product Details':
            #                     v_product_dec = c_replace(v_desc).replace('<b>', '') if v_desc else ''
            #                     if 'Features:' in v_product_dec:
            #                         key_f = v_product_dec.split('Features:')[-1]
            #                 elif v_title == 'SIZE & FIT':
            #                     if v_desc and 'Dimension:' in v_desc:
            #                         v_sizeDimension = c_replace(v_desc).replace('Dimension:', '')
            #                     elif all(x in v_desc for x in ['Height', 'Width', 'Length']):
            #                         v_height = re.search(r'Height:\s*([\d.]+)', v_desc)
            #                         v_width = re.search(r'Width:\s*([\d.]+)', v_desc)
            #                         v_length = re.search(r'Length:\s*([\d.]+)', v_desc)
            #                         if height and width and length:
            #                             v_sizeDimension = f"{v_length.group(1)}x{v_width.group(1)}x{v_height.group(1)}"
            #
            #             v_brand = v_pdp.get('brand',{}).get('name','')
            #             v_color = v_pdp.get('baseColour', '')
            #             warehouses = []
            #             v_sizes = v_pdp.get('sizes', [])
            #
            #             for v_s in v_sizes:
            #                 v_s_sku = v_s.get('styleId', '')
            #                 v_s_size = v_s.get('label', '')
            #                 try:
            #                     v_s_price = (v_s.get('sizeSellerData', [{}])[0]).get('mrp', '')
            #                 except:
            #                     try:
            #                         v_s_price = v_pdp.get('price','').get('mrp','')
            #                     except:
            #                         v_s_price = ''
            #
            #
            #                 try:
            #                     v_d_price = (v_s.get('sizeSellerData', [{}])[0]).get('discountedPrice', '')
            #                 except:
            #                     try:
            #                         v_d_price = v_pdp.get('price','').get('discounted','')
            #                     except:
            #                         v_d_price = ''
            #
            #                 if v_s_price == v_d_price:
            #                     v_d_price = ''
            #
            #
            #                 v_rating = round(v_pdp.get('ratings', {}).get('averageRating', 0),1)
            #                 v_s_stock = 'In Stock' if v_s.get('available') else 'Out Of Stock'
            #                 if 'Out Of Stock' == v_s_stock:
            #                     v_moq = 0
            #                 else:
            #                     v_moq = 10
            #
            #                 v_data = {
            #                     'breadcrumbs': ' > '.join(breadcrumbs),
            #                     'title': c_replace(v_pdp.get('name', '')),
            #                     'rating': v_rating,
            #                     'mrp': v_s_price,
            #                     'main_images': ' || '.join(v_images),
            #                     'selling_price': v_d_price ,
            #                     'category_levels': ' > '.join(breadcrumbs),
            #                     'SKU': v_s_sku,
            #                     'weight': '',
            #                     'size': v_s_size,
            #                     'color': v_color,
            #                     'dimensions_(lxbxh)': v_sizeDimension,
            #                     'brand': v_brand,
            #                     'product_description': v_product_dec,
            #                     'key_benefits': '',
            #                     'key_features': key_f,
            #                     'key_ingredients': '',
            #                     'how_to_use': '',
            #                     'about_the_product': v_product_dec,
            #                     'seller_information': ' || '.join([v_s.get('sellerName', '') for v_s in pdp.get('sellers', [])]),
            #                     'delivery_timelines': '',
            #                     'stock_status': v_s_stock,
            #                     'moq_(maximum_order_quantity)': v_moq,
            #                     'product_delivery_sla': '',
            #                     'location': '',
            #                     'a1_content_with_images': '',
            #                     'source_url': f'https://www.myntra.com/{v_s_sku}',
            #                     'scrapedat': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
            #                 }
            #                 variant_list.append(v_data)

            # sorted_variant_list  = sorted(variant_list, key=lambda x: x["mrp"])
            fist_option = []

            try:
                first_size = first_size_value
            except:
                first_size = ''
            size_option = {'option_name':'Size',
                           'option_value':first_size
                           }
            color_option = {'option_name':'Color',
                            'option_value':color
                            }
            fist_option.append(size_option)
            fist_option.append(color_option)
            item['size'] = first_size
            item['variant_options'] = fist_option
            if len(sizes) == 1:
                variant_list = []

            sorted_variant_list = sorted(variant_list, key=lambda x:int(x["mrp"]) if x["mrp"] != "" else 0)
            item['variants'] = sorted_variant_list
            item['request_consumed'] = counter1

            dt = datetime.now()
            scraped_time = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
            end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

            # return  item

            # # ------------------ Serviceability API ------------------
            # json_data = json.dumps({
            #     "pincode": "110020",
            #     "consolidationEnabled": False,
            #     "paymentMode": "ALL",
            #     "serviceType": "FORWARD",
            #     "shippingMethod": "ALL",
            #     "items": [{
            #         "procurementTimeInDays": 0,
            #         "availableInWarehouses": warehouses,
            #         "skuId": pdp.get('id', '')
            #     }]
            # })
            #
            # api_headers = {
            #     'Accept': 'application/json',
            #     'Content-Type': 'application/json',
            #     'Origin': 'https://www.myntra.com',
            #     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            #     'Cookie': f'at={cookies.get("at", "")}',
            # }
            #
            # # Retry mechanism
            # shopping_response = None
            # for _ in range(3):
            #     try:
            #         shopping_response = requests.post(
            #             'https://www.myntra.com/gateway/v2/serviceability/check',
            #             headers=api_headers,
            #             data=json_data,
            #             timeout=80
            #         )
            #         request_consumed += 1
            #         if shopping_response.status_code == 200:
            #             break
            #     except Exception:
            #         time.sleep(1)
            #
            # if not shopping_response or shopping_response.status_code != 200:
            #     return None
            #
            # try:
            #     shipping_data = shopping_response.json()
            #     item_entries = shipping_data.get('itemServiceabilityEntries', [])
            # except Exception:
            #     return None
            #
            # formatted_date = ''
            # for item_ in item_entries:
            #     for service in item_.get('serviceabilityEntries', []):
            #         if service.get('promiseDate'):
            #             from datetime import datetime
            #             delivery_datetime = datetime.fromtimestamp(service['promiseDate'] / 1000)
            #             formatted_date = delivery_datetime.strftime("%Y-%m-%d %H:%M:%S")
            #
            #
            # print(json.dumps(item))
            # try:
            #     item['main_page_path'] = page_save_path
            #     item['headers_pages_path'] = ""
            #     item['variant_pages_path'] = ""
            # except:
            #     item['main_page_path'] = ''
            #     item['headers_pages_path'] = ''
            #     item['variant_pages_path'] = ""

            updated = collection.update_one({"Job_Id":job_id, "product_url":product_url, '_id':_id},
                                            {'$set':{"crawling_status":'Done',
                                                     'crawling_time':scraped_at_time,
                                                     "request_count":counter1,
                                                     'myntra_process_time':scraped_time,
                                                     "status_code":200,
                                                     'response_text':str(item),
                                                     "request_count_internal":retry_counter,
                                                     "myntra_process_start_time":start_time,
                                                     "myntra_process_end_time":end_time,
                                                     'response_internal':response_text,
                                                     'main_page_path':page_save_path,
                                                     'headers_pages_path':'',
                                                     'variant_pages_path':''
                                                     }
                                             })
            if updated:
                print('Data Updates...')
                try:
                    with open("log.txt", "a") as f:
                        f.write(f"requestiitititing Myntra...{datetime.now()}" + "" + "\n")
                except:
                    pass
                pass

        # return [item]
        except Exception as e:
            print(e)
            item['request_consumed'] = request_consumed
            item['error_msg'] = "Service unavailable due to structural changes. Don't retry"
            item['error_code'] = 503
            item['source_url'] = product_url
            # item['main_page_path'] = page_save_path if 'page_save_path' in locals() else ''

            dt = datetime.now()
            scraped_time = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")

            updated = collection.update_one({"_id":_id, "Job_Id":job_id, "product_url":product_url},
                                            {'$set':{"crawling_status":'Failed',
                                                     'crawling_time':scraped_time,
                                                     # "request_count":request_consumed,
                                                     # "request_count":item['request_count'],
                                                     "request_count":counter1,
                                                     # "request_count_internal": retry_counter,
                                                     # "request_count_internal": item['request_count_internal'],
                                                     "request_count_internal":retry_counter,
                                                     'myntra_process_time':scraped_time,
                                                     "status_code":503,
                                                     'response_text':json.dumps(item),
                                                     'main_page_path':page_save_path if 'page_save_path' in locals() else '',
                                                     'headers_pages_path':"",
                                                     'variant_pages_path':"",
                                                     }
                                             })  # json.dumps(item)
            if updated:
                print('Data Updated.....')
                try:
                    with open("log.txt", "a") as f:
                        f.write(f"Data Inserted Myntra...{datetime.now()}" + "" + "\n")
                except:
                    pass
                pass

    elif status_code == 400:
        end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        item = {}

        item['request_consumed'] = request_consumed
        item['source_url'] = product_url

        request_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        item['request_consumed'] = request_consumed
        item['scrapedat'] = request_time
        item['error_msg'] = "Invalid URL format. Please verify and try again"
        item['error_code'] = 400
        # item['main_page_path'] = page_save_path if 'page_save_path' in locals() else ''
        dt = datetime.now()
        scraped_time = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
        updated = collection.update_one({"Job_Id":job_id, "product_url":product_url, '_id':_id},
                                        {'$set':{"crawling_status":'Not_Found', 'crawling_time':scraped_at_time,
                                                 "request_count":counter1, "request_count_internal":retry_counter,
                                                 'myntra_process_time':scraped_time,
                                                 "status_code":400, "myntra_process_start_time":start_time,
                                                 "myntra_process_end_time":end_time,
                                                 'response_text':str(item),
                                                 'main_page_path':page_save_path if 'page_save_path' in locals() else '',
                                                 'headers_pages_path':"",
                                                 'variant_pages_path':"",
                                                 }
                                         })
        if updated:
            print('Data Not Found Updates...')
            pass

    elif status_code == 404:
        end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        item = {}

        item['request_consumed'] = request_consumed
        item['source_url'] = product_url

        request_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        item['request_consumed'] = request_consumed
        item['scrapedat'] = request_time
        item['error_msg'] = "The requested URL could not be found. Please verify the URL and try again"
        item['error_code'] = 404
        # item['main_page_path'] = page_save_path if 'page_save_path' in locals() else ''
        dt = datetime.now()
        scraped_time = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
        updated = collection.update_one({"Job_Id":job_id, "product_url":product_url, '_id':_id},
                                        {'$set':{"crawling_status":'Not_Found', 'crawling_time':scraped_at_time,
                                                 "request_count":counter1, "request_count_internal":retry_counter,
                                                 'myntra_process_time':scraped_time,
                                                 "status_code":404, "myntra_process_start_time":start_time,
                                                 "myntra_process_end_time":end_time,
                                                 'response_text':str(item),
                                                 'main_page_path':page_save_path if 'page_save_path' in locals() else '',
                                                 'headers_pages_path':"",
                                                 'variant_pages_path':"",
                                                 }
                                         })
        if updated:
            print('Data Not Found Updates...')
            pass

    elif status_code == 401:
        end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        item = {}
        item['request_consumed'] = request_consumed
        item['source_url'] = product_url
        item[
            'error_msg'] = "The request could not be completed because authentication failed or valid access credentials were not provided."
        item['error_code'] = 401
        request_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        item['request_consumed'] = request_consumed
        item['scrapedat'] = request_time
        # item['main_page_path'] = page_save_path if 'page_save_path' in locals() else ''
        dt = datetime.now()
        scraped_time = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
        updated = collection.update_one({"Job_Id":job_id, "product_url":product_url, '_id':_id},
                                        {'$set':{"crawling_status":'Failed', 'crawling_time':scraped_at_time,
                                                 "request_count":counter1, "request_count_internal":retry_counter,
                                                 'myntra_process_time':scraped_time,
                                                 "status_code":401, "myntra_process_start_time":start_time,
                                                 "myntra_process_end_time":end_time,
                                                 'response_text':str(item),
                                                 'main_page_path':page_save_path if 'page_save_path' in locals() else '',
                                                 'headers_pages_path':"",
                                                 'variant_pages_path':"",
                                                 }
                                         })
        if updated:
            print('Data Not Found Updates...')
            pass
    else:
        item = {}
        end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        item['request_consumed'] = request_consumed
        item['source_url'] = product_url

        request_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        item['request_consumed'] = request_consumed
        item['scrapedat'] = request_time
        item['error_msg'] = "Response timeout. Please retry later. After 30 Minutes"
        item['error_code'] = 503
        # item['main_page_path'] = page_save_path if 'page_save_path' in locals() else ''
        # item['headers_pages_path'] = ""
        # item['variant_pages_path'] = ""
        dt = datetime.now()
        scraped_time = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
        updated = collection.update_one({"Job_Id":job_id, "product_url":product_url, '_id':_id},
                                        {'$set':{"crawling_status":'Failed', 'crawling_time':scraped_at_time,
                                                 "request_count":counter1, "request_count_internal":retry_counter,
                                                 'myntra_process_time':scraped_time,
                                                 "status_code":503, "myntra_process_start_time":start_time,
                                                 "myntra_process_end_time":end_time,
                                                 'response_text':str(item),
                                                 'main_page_path':page_save_path if 'page_save_path' in locals() else '',
                                                 'headers_pages_path':"",
                                                 'variant_pages_path':"",
                                                 }
                                         })
        if updated:
            print('Data Updates...')
            pass

# ========== Main ==========
if __name__ == "__main__":

    data = list(collection.find({"status_code":201, "script_name":"myntra_pdp_web", "crawling_status":"Pending"}))
    try:
        with open("log.txt", "a") as f:
            f.write(f"Got the signal and running Myntra...{datetime.now()}" + "" + "\n")
    except:
        pass
    # data = list(collection.find({"_id": ObjectId("6a0c070aee799670dd0aeab2")}))
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_myntra_product, d) for d in data]
        for future in as_completed(futures):
            try:
                result = future.result()
            except Exception as e:
                print(f"[ERROR] {e}")
