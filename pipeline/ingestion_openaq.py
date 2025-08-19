# About code: Getting the first 100 results of the latest Air Quality results for LA
# Author: Winnie Mutunga

import pandas as pd
import requests
from datetime import datetime as dt
# import json
import pprint as pp
import os

from dotenv import load_dotenv # loading api key from .env file
load_dotenv()
apikey = os.getenv("APIKEY") # storing apikey

def getAQLocations():
    url = 'https://api.openaq.org/v3/locations'
    headers = {
    "X-API-Key": apikey
    }
    params = {
    "coordinates": '34.0522,-118.2437',
    "radius": 5000,  # in meters (5km search radius)
    "limit": 100,
    "sort": "desc",
    "countries_id":155
}

    response = requests.get(url, headers=headers, params=params)
    r = response.json()
    li = []
    first = []
    last = []
    # pp.pprint(r)
    for results in r['results']:
        sensors = results['sensors']
        for sensor in sensors:
            # print(results['datetimeLast']['local'])
            first.append((results.get('datetimeFirst') or {}).get('local', ''))
            last.append((results.get('datetimeLast') or {}).get('local', ''))
            li.append(sensor)

    df1 = pd.DataFrame({
        'First Seen': first,
        'Last Seen': last,
    })
    df2 = pd.json_normalize(li)
    df = pd.concat([df1, df2], axis=1)
    return df


def toCSV(df, name):
    df.to_csv(f'/path/to/folder/{name}.csv', index=False)
    print(f'Completed {name} to CSV')
# toCSV(getAQData(), 'LA_AQData')