from decimal import Decimal
import logging
import requests
import botocore 
import botocore.session
import simplejson as json
from db_utils import get_new_products
from aws_secretsmanager_caching import SecretCache, SecretCacheConfig 

#client = botocore.session.get_session().create_client('secretsmanager')
#cache_config = SecretCacheConfig()
#cache = SecretCache( config = cache_config, client = client)
#api_key = cache.get_secret_string('/openweather_api/api_key')

weather_url = "http://api.openweathermap.org/data/2.5/weather"
geocoding_url = "http://api.openweathermap.org/geo/1.0/direct"
aq_url = "http://api.openweathermap.org/data/2.5/air_pollution"
math_url = "https://api.mathjs.org/v4/"

def get_openweather_api_data(product, api="weather"):
    api_url = weather_url
    match api:
        case "aq":
            api_url = aq_url
    if product["farm"]["location"] is None:
        lat, lng = geocode_name(product["region"])       
        params = {
            'lat': lat,
            'lon': lng,
            'appid': api_key,
        }
    else:         
        params = {
            'lat': product["farm"]["location"]["lat"],
            'lon': product["farm"]["location"]["lng"],
            'units': "metric",
            'appid': api_key,
        }
    response = requests.get(api_url, params=params)
    if response.status_code == 200:
        return extract_response(api=api, response=response)
    else:
        # Handle errors
        pass

def extract_response(response, api="weather"):
    body = response.json()
    match api:
        case "weather":
            return body["main"]
        case "aq":
            return body["list"][0] 

def geocode_name(location: str):
    lat = "38.907"
    lng = "-77.037"
    params = {
        "q": location,
        "appid": api_key,
        "limit": 1
    }
    response = requests.get(geocoding_url, params=params)
    if response.status_code == 200:
        geo = response.json()
        lat = geo[0]["lat"]
        lat = geo[0]["lon"]
    return {"lat":lat, "lng":lng}

def get_roasted_price(purchase_price:str)->str:
    params = {
        'expr':f"{purchase_price}+3*sqrt(7)",
        "precision":4
    }
    response = requests.get(math_url, params=params)
    if response.status_code == 200:
        try:
            return response.content.decode()
        except:
            logging.fatal("unable to parse decimal response from math")
    else:
        raise Exception(f"Error in GET: {response.status_code}, {response.content}")

