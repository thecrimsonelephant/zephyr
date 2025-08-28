import pandas as pd
import hashlib

def cleaned_data(openAQ, meteo):
    openAQ.columns = openAQ.columns.str.replace('.', '_')

    openAQ.rename(columns={
        'period_datetimeFrom_utc':'datetimeFrom_utc',
        'period_datetimeFrom_local':'datetimeFrom_pdt',
        'period_datetimeTo_utc':'datetimeTo_utc',
        'period_datetimeTo_local':'datetimeTo_pdt',
        'Latitude':'sensor_latitude',
        'Longitude':'sensor_longitude',
        'Sensor ID':'sensor_id',
        'Station Name':'sensor_station_name',
        'City':'sensor_city',

    }, inplace=True)

    meteo['hourly_datetime_utc'] = pd.to_datetime(meteo['hourly_datetime (UTC)'], utc=True, errors='coerce')
    openAQ['datetimeFrom_utc'] = pd.to_datetime(openAQ['datetimeFrom_utc'], utc=True, errors='coerce')
    openAQ['datetimeTo_utc'] = pd.to_datetime(openAQ['datetimeTo_utc'], utc=True, errors='coerce')
    
    openAQ_hourly = openAQ.groupby(['parameter_name','datetimeFrom_utc', 'sensor_city']).agg({
        'datetimeTo_utc': 'last',
        'sensor_id': 'first',
        'sensor_station_name': 'first',
        'sensor_latitude': 'first',
        'sensor_longitude': 'first',
        'summary_avg': 'mean',
        'parameter_units': 'first',
        'value': 'mean',
    }).reset_index()

    cleaned = pd.merge(meteo, openAQ_hourly, how='inner', left_on='hourly_datetime_utc', right_on='datetimeFrom_utc')

    cleaned["row_id"] = cleaned.index
    cleaned['gen_timestamp'] = pd.Timestamp.now() # creating generated timestamp for unique key creation
    cleaned['composite_key'] = cleaned['row_id'].astype(str) + cleaned['sensor_city'].astype(str) + cleaned['parameter_name'].astype(str) + cleaned['hourly_datetime (UTC)'].astype(str) # rather than lambda which slows down as datasets get larger
    cleaned['unique_id'] = [hashlib.sha256(x.encode()).hexdigest()[:12] for x in cleaned['composite_key']] # created sha256 (shortened to 12char)
    cleaned = cleaned.drop(columns=['hourly_datetime (UTC)', 'City', 'composite_key', 'row_id'])
    cleaned = cleaned[sorted(cleaned.columns)]
    return cleaned