from decimal import Decimal
from io import StringIO
from typing import List
import concurrent.futures
import simplejson as json
import pandas as pd
import logging

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.wait import WebDriverWait 
from webdriver_manager.chrome import ChromeDriverManager
from names_generator import generate_name

from data_utils import clean_keys
from db_utils import get_new_products, put_item_in_dynamo, put_items_in_dynamo, update_product_to_purchased
from openweather_api import get_openweather_api_data, get_roasted_price
from models.ecom_products import RoastedCoffee
from models.green_coffee import GreenCoffeeProduct, Farm, FlavorWheel, Score

MAX_THREADS = 5

service = Service(executable_path=ChromeDriverManager().install())

class ProductWorker:
    user_agent: str="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36"

    def __init__(self, url:str="www.google.com")->None:
        driver = ProductWorker.initialize_driver()
        driver.get(url)
        self.driver = driver

    def __del__(self)->None:
        try:
            self.close()
        except:
            pass

    def close(self)->None:
        self.driver.close() 

    @staticmethod
    def initialize_driver()->webdriver:
        options = webdriver.ChromeOptions()
        options.add_argument("--log-level=1")        
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--ignore-ssl-errors')
        options.add_argument("--headless=new")
        #Some sites require a user agent, this is more of "just in case"
        options.add_argument(f'user-agent={ProductWorker.user_agent}')
        driver = webdriver.Chrome(service=service, options=options)
        return driver

def get_coffee_product(url:str) -> GreenCoffeeProduct:
    try:
        print(f"getting products for url:{url}") 
        link_worker = ProductWorker(url)
        #soup the html and close the driver
        element = WebDriverWait(link_worker.driver, 10).until(lambda x: x.find_element(By.ID, "product.info.specs")) 
        html = link_worker.driver.page_source
        link_worker.close()

        soup = BeautifulSoup(html, features="html.parser")
        #Get the product table, push it into a df and push that into a dict
        full_specs = soup.find("div", id="product.info.specs")
        logging.info(f"specs for {url}: \n {full_specs}")
        df = pd.read_html(StringIO(str(full_specs)))[0]
        product_specs = clean_keys(df.set_index(0).to_dict('dict')[1])
        
        #This could be parallelised
        #with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_THREADS) as executor:
        #    score_task = executor.submit(get_score, soup.find("div", attrs={'data-chart-id' : "cupping-chart"}))
        #    flavor_task = executor.submit(get_flavor_wheel, soup.find("div", attrs={'data-chart-id' : "flavor-chart"}))
        #    farm_task = executor.submit(get_farm, soup)
        #    price_task = executor.submit(get_prices, soup)
        
        flavor_wheel = get_flavor_wheel(soup.find("div", attrs={'data-chart-id' : "flavor-chart"}))
        score = get_score(soup.find("div", attrs={'data-chart-id' : "cupping-chart"}))
        farm = get_farm(soup)

        #We'll process the Magento JSON-LD for prices and SKU
        context = json.loads(soup.find("script", attrs={"type" : "application/ld+json"}).string)
        prices = get_prices(soup, context)
        product_specs["seller"] = "sweetmaria"
        product_specs["sku"] = context["sku"]

        return GreenCoffeeProduct(flavor=flavor_wheel, score=score, farm=farm, prices=prices, **product_specs)
    except TimeoutException as err:
        logging.warning(f"Selenium timeout while scraping {url}")
    except Exception as err:
        logging.fatal(f"Unexpected {err=}, {type(err)=} while scraping {url}") 

#We're going to assume a lot here. 1) no two sizes have the same price, 2) there is always a price
def get_prices(soup:BeautifulSoup, context:dict)->set:
    magento_prices = context["offers"]["offers"]
    sizes = [size.get_text() for size in soup.findAll("option", attrs={"value": lambda value: value.isnumeric()})]
    prices = set()
    for offer in magento_prices:
        prices.add(offer["price"])
    return prices if not sizes else set(zip(prices, sizes))

def get_farm(soup:BeautifulSoup)->Farm:
    try:
        farm_location = soup.find("div", "map-circle")
        farm_location = {"lat": farm_location.get('data-lat'), "lng": farm_location.get('data-lng')}
        farm_description = soup.find("div", "column-right").get_text()
        return Farm(
            location= farm_location,
            description= farm_description
        )
    except:
        logging.warning("No Farm Information Found")
        return Farm()

def get_score(soup:BeautifulSoup)->Score:
    try:
        s = {k:float(v) for k,v in map(lambda i: i.split(':'), soup.get("data-chart-value").split(","))}
        cc = int(soup.get("data-cupper-correction"))
        return Score(cupper_correction=cc, **clean_keys(s))
    except:
        return Score()

def get_flavor_wheel(soup:BeautifulSoup)->FlavorWheel:
    try:
        flavor = {k:v for k,v in map(lambda i: i.split(':'), soup.get("data-chart-value").split(","))}
        return FlavorWheel(**clean_keys(flavor))
    except:
        return FlavorWheel()

def scrape_sm_products(search_type:str=None)->None:
    logging.info("scraping sweet maria's for new green coffees")
    match search_type:
        case "light":
            url = "https://www.sweetmarias.com/green-coffee.html?sm_flavor_profile=102&sm_status=1"
        case "dark":
            url = "https://www.sweetmarias.com/green-coffee.html?sm_flavor_profile=111&sm_status=1"
        case "espresso":
            url = "https://www.sweetmarias.com/green-coffee.html?sm_flavor_profile=2058&sm_status=1"
        case _:
            url = "https://www.sweetmarias.com/green-coffee.html"
    scraper = ProductWorker(url)
    product_links = scraper.driver.find_elements(By.CSS_SELECTOR, 'tr.product.product-item a.product-item-link')
    links = [product.get_attribute("href") for product in product_links]
    #Once we have the links we should be able to close one webdriver for more proc power
    scraper.close()

    threads = min(MAX_THREADS, len(links))
    coffee:List[GreenCoffeeProduct] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        for result in executor.map(get_coffee_product, links, timeout=30):
            try:
                coffee.append(result.to_dynamodb())
            except Exception as err:
                logging.warning(f"Thread failed with Error: {err}")
    put_items_in_dynamo("GreenCoffeeProduct", coffee)

def parse_ecom_product(product)-> RoastedCoffee:
    name = generate_name(style='capital')
    price = get_roasted_price(product["prices"][0])
    roast_level = "City+"
    green_key = product["id"]
    return RoastedCoffee(name, price, roast_level, green_key)

def normalize_products_for_ecom():
    new_products = get_new_products()
    for product in new_products:
        product["weather"] = get_openweather_api_data(api="weather", product=product)
        product["aq"] = get_openweather_api_data(api="aq", product=product)
        put_item_in_dynamo("GreenCoffeeProduct", product)

def update_with_roasted_product():
    new_products = get_new_products()
    ecom_products = []
    for product in new_products:
        ecom_products.append(parse_ecom_product(product).to_dynamo())
        put_items_in_dynamo("RoastedCoffeeProduct", ecom_products)
        update_product_to_purchased(product["id"])

