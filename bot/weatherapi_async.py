# weatherapi_async.py
import aiohttp
import time
from typing import Dict, Optional, Iterable, List
from datetime import datetime, timezone, timedelta


WEATHERAPI_CODE_MAP = {
    1000: '☀️', 1003: '⛅️', 1006: '☁️', 1009: '☁️', 1030: '🌫️',
    1063: '🌦️', 1066: '❄️', 1069: '❄️', 1072: '🌫️', 1087: '⛈️',
    1114: '❄️', 1117: '❄️', 1135: '🌫️', 1147: '🌫️', 1150: '🌦️',
    1153: '🌦️', 1168: '🌦️', 1171: '⛈️', 1180: '🌧️', 1183: '🌧️',
    1186: '🌧️', 1189: '🌧️', 1192: '🌧️', 1195: '🌧️', 1198: '🌧️',
    1201: '🌧️', 1204: '🌨️', 1207: '🌨️', 1210: '🌨️', 1213: '🌨️',
    1216: '🌨️', 1219: '🌨️', 1222: '❄️', 1225: '❄️', 1237: '🌨️',
    1240: '🌦️', 1243: '🌧️', 1246: '🌧️', 1249: '🌨️', 1252: '🌨️',
    1255: '🌨️', 1258: '🌨️', 1261: '🌨️', 1264: '🌨️', 1273: '⛈️',
    1276: '⛈️', 1279: '🌨️', 1282: '🌨️'
}


def hpa_to_mmhg(hpa: float) -> float:
    """Перевод давления hPa → мм рт. ст."""
    return hpa * 0.75006


# ---------------------------------------------------------
#                     MAIN CLASS
# ---------------------------------------------------------
class WeatherAPI:
    def __init__(
        self,
        api_key: str,
        lat: float,
        lon: float,
        cache_ttl: int = 300  # 5 минут
    ):
        self.api_key = api_key
        self.lat = lat
        self.lon = lon
        self.cache_ttl = cache_ttl

        self._cache_current = None
        self._cache_forecast = None
        self._cache_time_current = 0
        self._cache_time_forecast = 0

    # -----------------------------
    # LOW LEVEL FETCHER
    # -----------------------------
    async def _fetch_json(self, url: str) -> Dict:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as resp:
                if resp.status != 200:
                    raise ValueError(f"WeatherAPI returned status {resp.status}")
                return await resp.json()

    # -----------------------------
    # CURRENT WEATHER
    # -----------------------------
    async def get_current(self) -> Dict:
        now = time.time()

        # CACHED
        if self._cache_current and (now - self._cache_time_current < self.cache_ttl):
            return self._cache_current

        url = (
            f"http://api.weatherapi.com/v1/current.json?"
            f"key={self.api_key}&q={self.lat},{self.lon}&lang=ru"
        )

        r = await self._fetch_json(url)
        cur = r["current"]
        code = cur["condition"]["code"]

        data = {
            "icon": WEATHERAPI_CODE_MAP.get(code, ""),
            "text": cur["condition"]["text"],
            "temp_c": cur["temp_c"],
            "feels_c": cur["feelslike_c"],
            "humidity": cur["humidity"],
            "wind_m_s": cur["wind_kph"] / 3.6,
            "pressure_mmhg": round(hpa_to_mmhg(cur["pressure_mb"]), 1),
            "raw": cur
        }

        self._cache_current = data
        self._cache_time_current = now
        return data

    async def format_current(self) -> str:
        d = await self.get_current()
        now = datetime.now(timezone.utc) + timedelta(hours=3)
        return (
            f"🌤 <b>Текущая погода</b>\n"
            f"🕒 Обновлено: {now.strftime('%Y-%m-%d %H:%M')}\n"
            f"{d['icon']} {d['text']}\n"
            f"🌡 Темп: {d['temp_c']}°C (ощущается {d['feels_c']}°C)\n"
            f"💧 Влажность: {d['humidity']}%\n"
            f"💨 Ветер: {d['wind_m_s']:.1f} м/с\n"
            f"🧭 Давление: {d['pressure_mmhg']} мм рт. ст."
        )

    # -----------------------------
    # FORECAST WEATHER
    # -----------------------------
    async def get_forecast(self, days: int = 1) -> Dict:
        now = time.time()

        # CACHED
        if (
            self._cache_forecast and
            (now - self._cache_time_forecast < self.cache_ttl)
        ):
            return self._cache_forecast

        url = (
            f"http://api.weatherapi.com/v1/forecast.json?"
            f"key={self.api_key}&q={self.lat},{self.lon}"
            f"&days={days}&aqi=no&alerts=no&lang=ru"
        )

        r = await self._fetch_json(url)

        self._cache_forecast = r
        self._cache_time_forecast = now
        return r

    async def format_forecast(
        self,
        hours: Optional[Iterable[int]] = None,
        short: bool = False
    ) -> str:
        """
        hours — iterable: например range(8, 22)
        short=True — короткий режим (короткое описание)
        """
        r = await self.get_forecast()
        fday = r["forecast"]["forecastday"][0]
        hours_data = fday["hour"]

        lines: List[str] = ["📅 <b>Прогноз на сегодня</b>"]

        for h in hours_data:
            hour = int(h["time"].split(" ")[1].split(":")[0])

            if hours and hour not in hours:
                continue

            code = h["condition"]["code"]
            icon = WEATHERAPI_CODE_MAP.get(code, "")

            if short:
                # Короткий режим с расшифровкой, осадками и ощущается
                lines.append(
                    f"{hour:02d}:00 {icon} ({h['condition']['text']}) "
                    f"{h['temp_c']}°C (ощущается {h['feelslike_c']}°C), "
                    f"💧 {h.get('chance_of_rain', 0)}% осадков"
                )
            else:
                # Полный режим
                lines.append(
                    f"<b>{hour:02d}:00</b> — {icon} {h['condition']['text']}\n"
                    f"🌡 Темп: {h['temp_c']}°C (ощущается {h['feelslike_c']}°C)\n"
                    f"💨 Ветер: {h['wind_kph']/3.6:.1f} м/с\n"
                    f"💧 Влажность: {h['humidity']}%"
                )

        return "\n".join(lines)


# ---------------------------------------------------------
# Пример использования
# ---------------------------------------------------------
# async def main():
#     w = WeatherAPI(
#         api_key="YOUR_KEY",
#         lat=55.75,
#         lon=37.61,
#         cache_ttl=180
#     )
#
#     print(await w.format_current())
#     print(await w.format_forecast(hours=range(9, 21), short=True))
#
# asyncio.run(main())
