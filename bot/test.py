import asyncio
from weatherapi_async import WeatherAPI
from openweathermapapi import OpenWeatherClient
WEATHERAPI_KEY = '50584232288b426091292309251405'
OWM_API = 'aca7e62658558133eae4c3f77f5d20ff'
LAT, LON = 55.759931, 37.643032



async def main():
    w = WeatherAPI(
        api_key=WEATHERAPI_KEY,
        lat=LAT,
        lon=LON,
        cache_ttl=180
    )

    print(await w.format_current())
    print(await w.format_forecast(hours=range(18, 24), short=True))


    # client = OpenWeatherClient(OWM_API, LAT, LON)

    # data = await client.get_current_weather()
    # text = client.format_current_weather(data)

    # forecast = await client.get_hourly_forecast()
    # forecast_text = client.format_hourly_forecast(forecast)

    # print(text)
    # print(forecast_text)



asyncio.run(main())