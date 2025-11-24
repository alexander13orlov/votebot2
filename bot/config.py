# bot/config.py
import os
from dotenv import load_dotenv

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