from pipeline import getOpenAQSensors, getHourlyAQData, getOpenMeteoData, mergeDataframes, cleaned_data

def main():
    # it's later now. Callinga ll of those imported functions
    sensorList = getOpenAQSensors()
    openAQ = getHourlyAQData(sensorList)
    daily, hourly = getOpenMeteoData()
    meteo = mergeDataframes(daily, hourly)
    dailyWeather = cleaned_data(openAQ, meteo)
    return dailyWeather

if __name__ == "__main__":
    df = main()
    print(df.head())  # optional: just to confirm it runs standalone (sanity check #???)