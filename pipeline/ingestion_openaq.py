# About code: Getting the first 100 results of the latest Air Quality results for LA
# Author: james Mutunga

import pandas as pd
import requests
from datetime import datetime as dt, timedelta as td, timezone as tz
import time
import os 
BASE = 'https://api.openaq.org/v3'

from dotenv import load_dotenv # loading api key from .env file
load_dotenv()
XAPIKEY = os.getenv("APIKEY") # storing apikey

def timestamps(d):
    today = dt.now().replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - td(days=1)
    delta = yesterday - td(days=d)
    # print(delta, yesterday)
    return delta, yesterday

def getOpenAQSensors():
    cities = [
        {"City": "Los Angeles", "Latitude": 34.0522, "Longitude": -118.2437, "Timezone": "America/Los_Angeles"},
        {"City": "New York", "Latitude": 40.7128, "Longitude": -74.0060, "Timezone": "America/New_York"},
        {"City": "Chicago", "Latitude": 41.8781, "Longitude": -87.6298, "Timezone": "America/Chicago"},
        {"City": "Houston", "Latitude": 29.7604, "Longitude": -95.3698, "Timezone": "America/Chicago"},
    ]
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
    for info in cities:
        c = info['City']
        lat = info['Latitude']
        long = info['Longitude']
        tzone = info['Timezone']
        coordinates = str(lat) + ","+ str(long)
        print(c, " ", coordinates)
        params = {
            "coordinates": coordinates, # LA
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
                    city.append(c) # adding city name for clarification

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
            yesterday = today_utc - pd.Timedelta(days=1)
            week_ago = yesterday - pd.Timedelta(days=7) # getting time delta as of yesterday - 7d ago
            # sanity check
            print(yesterday)
            print(week_ago)
            df['Last Seen (UTC)'] = pd.to_datetime(df['Last Seen (UTC)'], utc=True, errors='coerce')
            df1 = df[(df['Last Seen (UTC)'] >= yesterday) & (df['Last Seen (UTC)'] <= today_utc)] # getting specifically sensors only found in the past day
        except requests.exceptions.RequestException as reqErr:
            print(f'Request error occurred: {reqErr}')
        except ValueError as jsonErr:
            print(f'Request error occurred: {jsonErr}')
        except KeyError as keyErr:
            print(f'Request error occurred: {keyErr}')
        except Exception as e:
            print(f'Request error occurred: {e}')
    return df1

def getHourlyAQData(sensorList):
    ids = sensorList['Sensor ID'].reset_index(drop=True).tolist()
    tfrom, tto = timestamps(7) # getting 7 days of data (delta = yesterday-7)
    headers = {
        "X-API-Key": XAPIKEY,
        'Content-Type': 'application/json'
    }
    params = {
        "datetime_from": tfrom,
        "datetime_to": tto,
        "limit": 100 # max number of results per page
    }
    counter = 0 # counter for total number of requests made
    allRows = []
    for idx, sensor_id in enumerate(ids): 
        # grab metadata for this sensor row
        sensorMetadata = sensorList.iloc[idx].to_dict()

        page = 1 
        while True:
            counter += 1
            params['page'] = page
            url = f'{BASE}/sensors/{sensor_id}/hours'
            print(f"CALL # {counter} ----- ID {sensor_id} ----- URL {url}") # sanity check... also keeping track of how many calls in the run so that I don't overrun!
            try:
                response = requests.get(url, headers=headers, params=params)
                data = response.json()
                results = data.get("results", [])

                # attach metadata to each hourly record
                for r in results:
                    row = {**r, **sensorMetadata}  # merge hourly data + sensor metadata, with r winning out if found. Really trying not to get banned.
                    allRows.append(row)
                page += 1 
                # verify rate limits for sanity
                print("x-ratelimit-used:", response.headers.get("x-ratelimit-used"))
                print("x-ratelimit-reset:", response.headers.get("x-ratelimit-reset"))
                print("x-ratelimit-limit:", response.headers.get("x-ratelimit-limit"))
                print("x-ratelimit-remaining:", response.headers.get("x-ratelimit-remaining"))
                if len(results) == 0:
                    print(f"No more calls for {sensor_id}, so breaking out of loop")
                    print("-------------------------------------")
                    time.sleep(5)
                    break

                used = int(response.headers.get("x-ratelimit-used", 50))
                remaining = int(response.headers.get("x-ratelimit-remaining", 1))
                reset = int(response.headers.get("x-ratelimit-reset", 60))
                if remaining <= 10 or used >= 50:
                    print(f"Rate limit reached, sleeping for {reset} seconds")
                    print(f"-------------------------------------SLEEPING {reset}s-------------------------------------")
                    time.sleep(reset)
                else:
                    print("-------------Sleeping 5s before next request-------------")
                    time.sleep(5)  
            except requests.exceptions.RequestException as reqErr:
                print(f'Request error occurred: {reqErr}')
                break
            except ValueError as jsonErr:
                print(f'Request error occurred: {jsonErr}')
                break
            except KeyError as keyErr:
                print(f'Request error occurred: {keyErr}')
                break
            except Exception as e:
                print(f'Request error occurred: {e}')
                break

    # normalize to dataframe (now includes Sensor ID + metadata!)
    df = pd.json_normalize(allRows)
    return df
# aqSensors = getOpenAQSensors()
# display(getHourlyAQData(aqSensors))