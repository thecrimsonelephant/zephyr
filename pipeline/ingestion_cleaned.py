import pandas as pd

def cleaned_data(openAQ, meteo):
    openAQ.columns = openAQ.columns.str.replace('.', '_') # flattening dataframes with .json_normalize produces cols with . operators which are incompatible with SQL

    # renaming nearly all columns in openAQ
    openAQ.rename(columns={
        'period_datetimeFrom_utc':'datetimeFrom (UTC)',
        'period_datetimeFrom_local':'datetimeFrom (PDT)',
        'period_datetimeTo_utc':'datetimeTo (UTC)',
        'period_datetimeTo_local':'datetimeTo (PDT)',
        'Latitude':'sensor_latitude',
        'Longitude':'sensor_longitude',
        'Sensor ID':'sensor_id',
        'Station Name':'sensor_station_name'

    }, inplace=True)

    meteo['hourly_datetime (UTC)'] = pd.to_datetime(meteo['hourly_datetime (UTC)'], utc=True, errors='coerce')
    openAQ['datetimeFrom (UTC)'] = pd.to_datetime(openAQ['datetimeFrom (UTC)'], utc=True, errors='coerce')
    openAQ['datetimeTo (UTC)'] = pd.to_datetime(openAQ['datetimeTo (UTC)'], utc=True, errors='coerce')
    
    # using renamed columns, and adding aggregation by meteo City
    openAQ_hourly = openAQ.groupby(['parameter_name','datetimeFrom (UTC)', 'City']).agg({
        'datetimeTo (UTC)': 'last',
        'sensor_id': 'first',
        'sensor_station_name': 'first',
        'sensor_latitude': 'first',
        'sensor_longitude': 'first',
        'summary_avg': 'mean',
        'parameter_units': 'first',
        'value': 'mean',
    }).reset_index()
    
    cleaned = pd.merge(meteo, openAQ_hourly, how='inner', left_on='hourly_datetime (UTC)', right_on='datetimeFrom (UTC)')
    cleaned = cleaned.drop(columns=['City_x']) # dropping duplicate (and not quite right) city for openAQ city use
    # renaming openAQ City for clarity
    cleaned.rename(columns={
        'City_y':'sensor_city'}, inplace=True)
    
    cleaned = cleaned[sorted(cleaned.columns)]
    return cleaned