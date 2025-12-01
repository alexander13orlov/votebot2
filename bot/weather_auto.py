# weather_auto.py
import json
from datetime import datetime, date, timedelta
from pathlib import Path
import asyncio
import logging

from aiogram import Bot
from .config import LOCAL_TZ  # ваш локальный часовой пояс

logger = logging.getLogger(__name__)

# === настройки обновления ===
UPDATE_INTERVAL_MIN = 10
STOP_UPDATE_HOUR = 23

# === файл для хранения сообщений ===
WEATHER_FILE = Path("weather_messages.json")

# структура: chat_id -> {message_id, created_date, last_text}
weather_messages = {}


# =============================
#     ЗАГРУЗКА / СОХРАНЕНИЕ
# =============================
def load_weather_messages():
    global weather_messages
    if WEATHER_FILE.exists():
        try:
            weather_messages = json.loads(WEATHER_FILE.read_text(encoding="utf-8"))
        except Exception:
            logger.warning("Failed to load weather_messages.json, starting empty")
            weather_messages = {}
    else:
        weather_messages = {}


def save_weather_messages():
    try:
        WEATHER_FILE.write_text(
            json.dumps(weather_messages, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )
    except Exception as e:
        logger.error(f"Failed to write weather_messages.json: {e}")


# =============================
#     ОТПРАВКА ПОГОДЫ
# =============================
async def send_weather(bot: Bot, chat_id: int, weather_client):
    """
    Отправляет новый прогноз + сохраняет сообщение.
    """
    now = datetime.now(LOCAL_TZ)
    # hours_range = range(now.hour, 24)
    hours_range = range(19, 24)

    current_weather = await weather_client.format_current()
    forecast_text = await weather_client.format_forecast(hours=hours_range, short=True)
    text = f"{current_weather}\n\n{forecast_text}"

    try:
        msg = await bot.send_message(chat_id, text, parse_mode="HTML")
    except Exception as e:
        logger.warning(f"Failed to send weather to chat {chat_id}: {e}")
        return None

    weather_messages[chat_id] = {
        "message_id": msg.message_id,
        "created_date": now.date().isoformat(),
        "last_text": text
    }
    save_weather_messages()

    return msg


# =============================
#     ОБНОВЛЕНИЕ ПОГОДЫ
# =============================
async def weather_updater(bot: Bot, weather_client):
    """
    Обновляет погоду каждые N минут, удаляет записи при ошибке,
    очищает историю в полночь.
    """
    logger.info("Weather updater started")

    last_cleanup = date.today()

    while True:
        try:
            now = datetime.now(LOCAL_TZ)

            # ---- полуночная очистка ----
            if now.date() != last_cleanup:
                weather_messages.clear()
                save_weather_messages()
                last_cleanup = now.date()
                logger.info("[weather_updater] Midnight cleanup complete")

            # ---- остановка обновлений после 23:00 ----
            if now.hour >= STOP_UPDATE_HOUR:
                await asyncio.sleep(60)
                continue

            # ---- обновление сообщений ----
            for chat_id, info in list(weather_messages.items()):
                created_date = date.fromisoformat(info["created_date"])

                # обновлять только сегодняшние сообщения
                if created_date != now.date():
                    continue

                message_id = info["message_id"]
                last_text = info.get("last_text", "")

                # формирование свежего текста
                # hours_range = range(now.hour, 24)
                hours_range = range(19, 24)
                current_weather = await weather_client.format_current()
                forecast_text = await weather_client.format_forecast(hours=hours_range, short=True)
                new_text = f"{current_weather}\n\n{forecast_text}"

                # нет изменений
                if new_text == last_text:
                    continue

                try:
                    await bot.edit_message_text(
                        new_text,
                        chat_id=chat_id,
                        message_id=message_id,
                        parse_mode="HTML"
                    )
                except Exception as e:
                    logger.warning(
                        f"[weather_updater] Failed to edit message chat={chat_id}: {e} — removing entry"
                    )
                    weather_messages.pop(chat_id, None)
                    save_weather_messages()
                    continue

                # сохраняем обновлённый текст
                weather_messages[chat_id]["last_text"] = new_text
                save_weather_messages()

        except Exception as e:
            logger.exception(f"[weather_updater] Unexpected error: {e}")

        await asyncio.sleep(UPDATE_INTERVAL_MIN * 60)
