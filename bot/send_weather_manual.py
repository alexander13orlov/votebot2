# bot/send_weather_manual.py

import asyncio
from aiogram import Bot

from .weatherapi_async import WeatherAPI
from .config import BOT_TOKEN, WEATHERAPI_KEY, LAT, LON, root_chat_id
from .weather_auto import send_weather


async def main():
    bot = Bot(token=BOT_TOKEN)
    weather_client = WeatherAPI(
        api_key=WEATHERAPI_KEY,
        lat=LAT,
        lon=LON,
        cache_ttl=300
    )

    

    await send_weather(bot, root_chat_id, weather_client)

    await bot.session.close()  # корректно закрываем сессию


if __name__ == "__main__":
    asyncio.run(main())
