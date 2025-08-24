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
XAPIKEY = os.getenv("APIKEY") # storing apikey

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
    city = [] # city name list
    timezone = [] # timezone of sensor

    url = f'{BASE}/locations'  
    headers = {
        "X-API-Key": XAPIKEY,
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
        # pp.pprint(r)

        for results in r['results']:
            sensors = results['sensors']
            for sensor in sensors:
                id.append(sensor.get('id', ''))
                latitude.append((results.get('coordinates') or {}).get('latitude', ''))
                longitude.append((results.get('coordinates') or {}).get('longitude', ''))
                first.append((results.get('datetimeFirst') or {}).get('utc', ''))
                last.append((results.get('datetimeLast') or {}).get('utc', ''))
                name.append(results.get('name', ''))
                timezone.append(results.get('timezone', ''))
                city.append('Los Angeles') # adding city name for clarification

        # appending to dataframe
        df = pd.DataFrame({
            'Sensor ID': id,
            'Latitude': latitude,
            'Longitude': longitude,
            'City': city,
            'Station Name': name,
            'First Seen (UTC)': first,
            'Last Seen (UTC)': last,
            'Timezone': timezone,
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
    tfrom, tto = timestamps(2)
    headers = {
        "X-API-Key": XAPIKEY,
        'Content-Type': 'application/json'
    }
    params = {
        "datetime_from": tfrom,
        "datetime_to": tto,
        "limit": 100 # max number of results per page
    }

    allRows = []
    for idx, sensor_id in enumerate(ids): 
        # grab metadata for this sensor row
        sensorMetadata = sensorList.iloc[idx].to_dict()

        page = 1 
        while True:
            params['page'] = page
            url = f'{BASE}/sensors/{sensor_id}/hours'
            print(url) # sanity check
            try:
                response = requests.get(url, headers=headers, params=params)
                data = response.json()
                results = data.get("results", [])

                # attach metadata to each hourly record
                for r in results:
                    row = {**r, **sensorMetadata}  # merge hourly data + sensor metadata, with r winning out if found
                    allRows.append(row)

                page += 1 

                # verify rate limits for sanity
                print("x-ratelimit-used:", response.headers.get("x-ratelimit-used"))
                print("x-ratelimit-reset:", response.headers.get("x-ratelimit-reset"))
                print("x-ratelimit-limit:", response.headers.get("x-ratelimit-limit"))
                print("x-ratelimit-remaining:", response.headers.get("x-ratelimit-remaining"))
                if len(results) == 0:
                    print(f"-------------------------------------SLEEPING 10s-------------------------------------")
                    time.sleep(10)  
                    break

                used = int(response.headers.get("x-ratelimit-used", 50))
                remaining = int(response.headers.get("x-ratelimit-remaining", 1))
                reset = int(response.headers.get("x-ratelimit-reset", 60))
                if remaining <= 10 or used >= 50:
                    print(f"-------------------------------------SLEEPING {reset}s-------------------------------------")
                    print(f"Rate limit reached, sleeping for {reset} seconds")
                    time.sleep(reset)
                else:
                    time.sleep(10)  
                    print(f"-------------------------------------SLEEPING 10s-------------------------------------")
            except requests.exceptions.RequestException as reqErr:
                print(f'Request error occurred: {reqErr}')
            except ValueError as jsonErr:
                print(f'Request error occurred: {jsonErr}')
            except KeyError as keyErr:
                print(f'Request error occurred: {keyErr}')
            except Exception as e:
                print(f'Request error occurred: {e}')

    # normalize to dataframe (now includes Sensor ID + metadata!)
    df = pd.json_normalize(allRows)
    return df