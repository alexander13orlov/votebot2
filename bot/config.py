# bot/config.py
import os
from dotenv import load_dotenv
from datetime import timezone, timedelta
from pathlib import Path
load_dotenv()  # загружает переменные из .env

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("Не найден BOT_TOKEN в .env")

# список ID админов (строками или числами)
ADMIN_IDS = [
    "84324980",   # твой id
    "566078997",  # ещё админ
]

WEATHERAPI_KEY = os.getenv("WEATHERAPI_KEY")
if not WEATHERAPI_KEY:
    raise ValueError("Не найден WEATHERAPI_KEY в .env")

# ===== Локальный часовой пояс UTC+3 =====
LOCAL_TZ = timezone(timedelta(hours=3))
# ===== Геопозиция =====
LAT, LON = 55.759931, 37.643032

# ===== Пути к файлам =====
DATA_DIR = Path(__file__).parent
SETTINGS_PATH = DATA_DIR / "settings.json"
HISTORY_PATH = DATA_DIR / "polls_history.json"