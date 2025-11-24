# weather_client.py
import aiohttp
from datetime import datetime, timedelta, timezone


# Иконки OpenWeather → Emoji
WEATHER_ICONS = {
    "01d": "☀️", "01n": "🌑",
    "02d": "🌤", "02n": "🌤",
    "03d": "⛅",  "03n": "☁️",
    "04d": "☁️", "04n": "☁️",
    "09d": "🌧", "09n": "🌧",
    "10d": "🌦", "10n": "🌧",
    "11d": "⛈", "11n": "⛈",
    "13d": "❄️", "13n": "❄️",
    "50d": "🌫", "50n": "🌫",
}


class OpenWeatherClient:

    BASE_URL = "https://api.openweathermap.org/data/2.5"

    def __init__(self, api_key: str, lat: float, lon: float):
        self.api_key = api_key
        self.lat = lat
        self.lon = lon

        # ---- КЕШ ----
        self._cache_current = None
        self._cache_current_time = None

        self._cache_forecast = None
        self._cache_forecast_time = None

    # ------------------------
    # 🔧 Базовый GET
    # ------------------------
    async def _get_json(self, url: str):
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                resp.raise_for_status()
                return await resp.json()

    # ------------------------
    # 🌡 Текущая погода
    # ------------------------
    async def get_current_weather(self):
        # --- кеш 5 минут ---
        if (
            self._cache_current
            and datetime.now() - self._cache_current_time < timedelta(minutes=5)
        ):
            return self._cache_current

        url = (
            f"{self.BASE_URL}/weather?"
            f"lat={self.lat}&lon={self.lon}&appid={self.api_key}&units=metric&lang=ru"
        )

        data = await self._get_json(url)

        weather = data["weather"][0]
        main = data["main"]

        result = {
            "description": weather["description"],
            "icon": weather["icon"],
            "temp": main["temp"],
            "feels_like": main["feels_like"],
            "pressure_mm": int(main["pressure"] * 0.750062),
            "humidity": main["humidity"],
            "wind_speed": data["wind"]["speed"],
            "timestamp": data["dt"],
            "timezone_shift": data["timezone"],
        }

        # сохраняем кеш
        self._cache_current = result
        self._cache_current_time = datetime.now()

        return result

    # ------------------------
    # 🕒 Почасовой прогноз
    # ------------------------
    async def get_hourly_forecast(self):
        # --- кеш 30 минут ---
        if (
            self._cache_forecast
            and datetime.now() - self._cache_forecast_time < timedelta(minutes=30)
        ):
            return self._cache_forecast

        url = (
            f"{self.BASE_URL}/forecast?"
            f"lat={self.lat}&lon={self.lon}&appid={self.api_key}&units=metric&lang=ru"
        )

        data = await self._get_json(url)

        forecast = []
        tz_shift = data["city"]["timezone"]
        now = datetime.utcnow() + timedelta(seconds=tz_shift)
        today = now.date()

        for item in data.get("list", []):
            utc_dt = datetime.fromtimestamp(item["dt"], tz=timezone.utc)
            local_dt = utc_dt + timedelta(seconds=tz_shift)

            if local_dt.date() != today:
                continue
            if local_dt.hour < now.hour:
                continue

            w = item["weather"][0]

            entry = {
                "time": local_dt.strftime("%H:%M"),
                "description": w["description"],
                "icon": w["icon"],
                "temp": item["main"]["temp"],
                "feels_like": item["main"]["feels_like"],
                "pop": round(item.get("pop", 0) * 100),
                "wind_speed": item["wind"]["speed"],
                "humidity": item["main"]["humidity"],
            }

            forecast.append(entry)

        # сохраняем кеш
        self._cache_forecast = forecast
        self._cache_forecast_time = datetime.now()

        return forecast

    # ------------------------
    # 🎨 Форматированный вывод
    # ------------------------
    def format_current_weather(self, data: dict) -> str:
        icon = WEATHER_ICONS.get(data["icon"], "🌡")

        return (
            f"{icon} <b>Сейчас</b>\n"
            f"Температура: <b>{data['temp']}°C</b> (ощущается как {data['feels_like']}°C)\n"
            f"{data['description'].capitalize()}\n"
            f"💨 Ветер: {data['wind_speed']} м/с\n"
            f"💧 Влажность: {data['humidity']}%\n"
            f"🔽 Давление: {data['pressure_mm']} мм рт. ст."
        )

    def format_hourly_forecast(self, forecast: list) -> str:
        if not forecast:
            return "Нет данных на сегодня."

        lines = ["📅 <b>Почасовой прогноз на сегодня:</b>"]

        for f in forecast:
            icon = WEATHER_ICONS.get(f["icon"], "🌡")
            lines.append(
                f"{f['time']} — {icon} {f['temp']}°C (ощущ. {f['feels_like']}°C), "
                f"{f['description']}, 💧 {f['humidity']}%, 🌧 {f['pop']}%"
            )

        return "\n".join(lines)
