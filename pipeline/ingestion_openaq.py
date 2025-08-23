# About code: Getting the first 100 results of the latest Air Quality results for LA
# Author: Winnie Mutunga

import pandas as pd
import requests
from datetime import datetime as dt, timedelta as td, timezone as tz
# import pprint as pp
import time
import os 
BASE = 'https://api.openaq.org/v3'

from dotenv import load_dotenv # loading api key from .env file
load_dotenv()
xapikey = os.getenv("APIKEY") # storing apikey

def timestamps(d):
    now = dt.now()
    delta = now - td(days=d)
    return delta, now

def getOpenAQSensors():
    # lists for appending data 
    id = [] # sensor IDs
    first = [] # first seen
    last = [] # last seen
    latitude = []
    longitude = []
    name = [] # name of sensor

    url = f'{BASE}/locations'  
    headers = {
        "X-API-Key": xapikey,
        'Content-Type': 'application/json'
    }
    params = {
        "coordinates": '34.0522,-118.2437', # LA
        "radius": 5000,  # in meters (5km search radius)
        "limit": 100, # page size
        "countries_id":155, # USA
        "sort": "desc" # ordering by ID desc
    }
    try: 
        response = requests.get(url, headers=headers, params=params) # get request to OpenAQ
        r = response.json() # returning as parseable json

        for results in r['results']:
            sensors = results['sensors']
            for sensor in sensors:
                id.append(sensor.get('id', ''))
                latitude.append((results.get('coordinates') or {}).get('latitude', ''))
                longitude.append((results.get('coordinates') or {}).get('longitude', ''))
                first.append((results.get('datetimeFirst') or {}).get('utc', ''))
                last.append((results.get('datetimeLast') or {}).get('utc', ''))
                name.append(results.get('name', ''))

        # appending to dataframe
        df = pd.DataFrame({
            'Sensor ID': id,
            'Latitude': latitude,
            'Longitude': longitude,
            'Station Name': name,
            'First Seen (UTC)': first,
            'Last Seen (UTC)': last,
        })
        today_utc = pd.Timestamp(dt.now(tz.utc))
        oneday_utc = today_utc - pd.Timedelta(days=1)
        df['Last Seen (UTC)'] = pd.to_datetime(df['Last Seen (UTC)'], utc=True, errors='coerce')

        df1 = df[(df['Last Seen (UTC)'] >= oneday_utc) & (df['Last Seen (UTC)'] <= today_utc)] # getting specifically sensors only found in the past day
    except requests.exceptions.RequestException as reqErr:
        print(f'Request error occurred: {reqErr}')
    except ValueError as jsonErr:
        print(f'Request error occurred: {jsonErr}')
    except KeyError as keyErr:
        print(f'Request error occurred: {keyErr}')
    except Exception as e:
        print(f'Request error occurred: {e}')

    # print("x-ratelimit-used:", response.headers.get("x-ratelimit-used"))
    # print("x-ratelimit-reset:", response.headers.get("x-ratelimit-reset"))
    # print("x-ratelimit-limit:", response.headers.get("x-ratelimit-limit"))
    # print("x-ratelimit-remaining:", response.headers.get("x-ratelimit-remaining"))
    return df1

def getHourlyAQData(sensorList):
    ids = sensorList['Sensor ID'].reset_index(drop=True).tolist()
    tfrom, tto = timestamps(1)
    headers = {
        "X-API-Key": xapikey,
        'Content-Type': 'application/json'
    }
    params = {
        "datetime_from": tfrom,
        "datetime_to": tto,
        "limit": 100 # max number of results per page
    }

    allRows = []
    for id in ids: 
        page = 1 # defaulting and resetting to 1 once while-break is reached 
        while True:
            params['page'] = page
            url = f'{BASE}/sensors/{id}/hours'
            print(url) # sanity check
            try:
                response = requests.get(url, headers=headers, params=params)
                data = response.json()
                # print(data['meta']['found'])
                results = data.get("results", [])
                allRows.extend(results) # extending all found results to list to avoid list of lists 
                page += 1 # increasing page if exists

                # verifying rate limits for sanity
                print("x-ratelimit-used:", response.headers.get("x-ratelimit-used"))
                print("x-ratelimit-reset:", response.headers.get("x-ratelimit-reset"))
                print("x-ratelimit-limit:", response.headers.get("x-ratelimit-limit"))
                print("x-ratelimit-remaining:", response.headers.get("x-ratelimit-remaining"))
                if len(results) == 0:
                    time.sleep(10)  # safe pause
                    print(f"-------------------------------------SLEEPING 10s-------------------------------------")
                    break
                used = int(response.headers.get("x-ratelimit-used", 50)) # defaulting to 50 if it skips
                remaining = int(response.headers.get("x-ratelimit-remaining", 1)) # defaulting to minimum (1) if it skips
                reset = int(response.headers.get("x-ratelimit-reset", 60)) # defaulting to maximum (60) if it skips
                if remaining <= 10 or used >= 50: # creating buffer around ratelimits used and remaining so as to not get banned 🥲 
                    print(f"Rate limit reached, sleeping for {reset} seconds")
                    print(f"-------------------------------------SLEEPING {reset}s-------------------------------------")
                    time.sleep(reset) # defaults to 60s
                else:
                    time.sleep(10)  # safety pause
                    print(f"-------------------------------------SLEEPING 10s-------------------------------------")
            except requests.exceptions.RequestException as reqErr:
                print(f'Request error occurred: {reqErr}')
            except ValueError as jsonErr:
                print(f'Request error occurred: {jsonErr}')
            except KeyError as keyErr:
                print(f'Request error occurred: {keyErr}')
            except Exception as e:
                print(f'Request error occurred: {e}')

    df = pd.json_normalize(allRows) # normalizing extended list to dataframe 
    return df