import pandas as pd

def cleaned_data(openAQ, meteo):
    openAQ.columns = openAQ.columns.str.replace('.', '_')

    openAQ.rename(columns={
        'period_datetimeFrom_utc':'datetimeFrom (UTC)',
        'period_datetimeFrom_local':'datetimeFrom (PDT)',
        'period_datetimeTo_utc':'datetimeTo (UTC)',
        'period_datetimeTo_local':'datetimeTo (PDT)'
    }, inplace=True)
    
    meteo['hourly_datetime (UTC)'] = pd.to_datetime(meteo['hourly_datetime (UTC)'], utc=True, errors='coerce')
    openAQ['datetimeFrom (UTC)'] = pd.to_datetime(openAQ['datetimeFrom (UTC)'], utc=True, errors='coerce')
    openAQ['datetimeTo (UTC)'] = pd.to_datetime(openAQ['datetimeTo (UTC)'], utc=True, errors='coerce')
    
    openAQ_hourly = openAQ.groupby(['parameter_name','datetimeFrom (UTC)']).agg({
        'datetimeTo (UTC)': 'last',
        'Sensor ID': 'first',
        'Station Name': 'first',
        'Latitude': 'first',
        'Longitude': 'first',
        'City': 'first',
        'summary_avg': 'mean',
        'parameter_units': 'first',
        'value': 'mean',
    }).reset_index()
    
    cleaned = pd.merge(meteo, openAQ_hourly, how='inner', left_on='hourly_datetime (UTC)', right_on='datetimeFrom (UTC)')
    return cleaned