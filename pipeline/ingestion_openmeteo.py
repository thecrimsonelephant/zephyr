import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry

# API setup from https://open-meteo.com/en/docs selection
def getOpenMeteoData():
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 34.0522,
        "longitude": -118.2437,
        "daily": ["temperature_2m_mean", "apparent_temperature_mean", "sunset", "sunrise", "weather_code"],
        "hourly": ["temperature_2m", "apparent_temperature", "dew_point_2m", "relative_humidity_2m", "precipitation", "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m", "cloud_cover", "shortwave_radiation", "snow_depth", "surface_pressure", "pressure_msl", "uv_index"],
        "timezone": "America/Los_Angeles",
        "past_days": 7,
        "forecast_days": 1,
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation: {response.Elevation()} m asl")
    print(f"Timezone: {response.Timezone()}{response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_apparent_temperature = hourly.Variables(1).ValuesAsNumpy()
    hourly_dew_point_2m = hourly.Variables(2).ValuesAsNumpy()
    hourly_relative_humidity_2m = hourly.Variables(3).ValuesAsNumpy()
    hourly_precipitation = hourly.Variables(4).ValuesAsNumpy()
    hourly_wind_speed_10m = hourly.Variables(5).ValuesAsNumpy()
    hourly_wind_direction_10m = hourly.Variables(6).ValuesAsNumpy()
    hourly_wind_gusts_10m = hourly.Variables(7).ValuesAsNumpy()
    hourly_cloud_cover = hourly.Variables(8).ValuesAsNumpy()
    hourly_shortwave_radiation = hourly.Variables(9).ValuesAsNumpy()
    hourly_snow_depth = hourly.Variables(10).ValuesAsNumpy()
    hourly_surface_pressure = hourly.Variables(11).ValuesAsNumpy()
    hourly_pressure_msl = hourly.Variables(12).ValuesAsNumpy()
    hourly_uv_index = hourly.Variables(13).ValuesAsNumpy()

    hourly_data = {"date": pd.date_range(
        start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
        end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = hourly.Interval()),
        inclusive = "left"
    )}

    hourly_data["temperature_2m"] = hourly_temperature_2m
    hourly_data["apparent_temperature"] = hourly_apparent_temperature
    hourly_data["dew_point_2m"] = hourly_dew_point_2m
    hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
    hourly_data["precipitation"] = hourly_precipitation
    hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
    hourly_data["wind_direction_10m"] = hourly_wind_direction_10m
    hourly_data["wind_gusts_10m"] = hourly_wind_gusts_10m
    hourly_data["cloud_cover"] = hourly_cloud_cover
    hourly_data["shortwave_radiation"] = hourly_shortwave_radiation
    hourly_data["snow_depth"] = hourly_snow_depth
    hourly_data["surface_pressure"] = hourly_surface_pressure
    hourly_data["pressure_msl"] = hourly_pressure_msl
    hourly_data["uv_index"] = hourly_uv_index

    hourly_dataframe = pd.DataFrame(data = hourly_data)

    # Process daily data. The order of variables needs to be the same as requested.
    daily = response.Daily()
    daily_temperature_2m_mean = daily.Variables(0).ValuesAsNumpy()
    daily_apparent_temperature_mean = daily.Variables(1).ValuesAsNumpy()
    daily_sunset = daily.Variables(2).ValuesInt64AsNumpy()
    daily_sunrise = daily.Variables(3).ValuesInt64AsNumpy()
    daily_weather_code = daily.Variables(4).ValuesAsNumpy()

    daily_data = {"date": pd.date_range(
        start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
        end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = daily.Interval()),
        inclusive = "left"
    )}

    daily_data["temperature_2m_mean"] = daily_temperature_2m_mean
    daily_data["apparent_temperature_mean"] = daily_apparent_temperature_mean
    daily_data["sunset"] = daily_sunset
    daily_data["sunrise"] = daily_sunrise
    daily_data["weather_code"] = daily_weather_code

    daily_dataframe = pd.DataFrame(data = daily_data)
    return daily_dataframe, hourly_dataframe

def mergeDataframes(daily, hourly):
    hourly['hourly_datetime'] = pd.to_datetime(hourly['date']) # preserving hourly datetime data for matching against openaq
    hourly['date'] = pd.to_datetime(hourly['date']).dt.date
    daily['date'] = pd.to_datetime(daily['date']).dt.date  # only keep date part

    # merge on dates
    df = pd.merge(
        hourly,
        daily,
        on='date',
        how='left'  # keep all hourly rows even if daily info missing
    )

    df.drop(columns=['date'], inplace=True) # dropping plain date col but might add it back jic, we'll see
    df = df[sorted(df.columns)] # sorting because my brain breaks if not
    return df