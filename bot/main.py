# -*- coding: utf-8 -*-
import asyncio
import json
import logging
from datetime import datetime, timedelta, time, date
from pathlib import Path
from typing import Optional

from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.exceptions import TelegramBadRequest

from .config import BOT_TOKEN

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent
SETTINGS_PATH = DATA_DIR / "settings.json"

with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
    SETTINGS = json.load(f)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

active_poll = {}
last_autocreate = {}


def parse_time_str(t: str) -> time:
    h, m, s = [int(x) for x in t.split(":")]
    return time(hour=h, minute=m, second=s)


def user_display_name(user: types.User) -> str:
    if user.username:
        return f"@{user.username} ({user.full_name})"
    return f"{user.full_name}"


async def edit_poll_message_old(chat_id: int, message_id: int, question: str, participants):
    lines = [question, ""]
    if participants:
        for idx, p in enumerate(participants, start=1):
            uid, username, fullname = p
            if username:
                # lines.append(f"{idx}. @{username} — {fullname}")
                lines.append(f"{idx}. @{username}")
            else:
                lines.append(f"{idx}. {fullname}")
    else:
        lines.append("Пока нет участников.")
    text = "\n".join(lines)
    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text)
    except TelegramBadRequest:
        pass

async def edit_poll_message(chat_id: int, message_id: int, question: str, participants):
    total = len(participants)
    lines = [f"[{total}]", question, ""]
    if participants:
        for idx, p in enumerate(participants, start=1):
            uid, username, fullname = p
            if username:
                # lines.append(f"{idx}. @{username} — {fullname}")
                lines.append(f"@{username}")
            else:
                lines.append(f"{fullname}")
    else:
        lines.append("Пока нет участников.")
    text = "\n".join(lines)
    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text)
    except TelegramBadRequest:
        pass



def find_command_settings(chat_id: int, command_name: str):
    chat_conf = SETTINGS["chats"].get(str(chat_id))
    if not chat_conf:
        return None
    topics = chat_conf.get("topics", {})
    topic = topics.get("root", {})
    commands = topic.get("commands", {})
    return commands.get(command_name)


async def create_poll(chat_id: int, command_name: str, *, by_auto=False, schedule_entry: Optional[dict] = None):
    if chat_id in active_poll:
        logger.info("Active poll exists in chat %s, skipping creation of %s", chat_id, command_name)
        return None

    cmd_settings = find_command_settings(chat_id, command_name)
    if not cmd_settings:
        return None

    question = cmd_settings.get("question", f"Опрос: {command_name}")

    pinned = False
    unpin = False
    if by_auto:
        aps = cmd_settings.get("autopollsettings", {})
        pin = aps.get("pin", "false").lower() == "true"
        unpin = aps.get("unpin", "false").lower() == "true"
        deactivatemsg = schedule_entry.get("deactivatemsg")
        deact_time = parse_time_str(deactivatemsg)
        today = date.today()
        expires_at = datetime.combine(today, deact_time)
    else:
        mps = cmd_settings.get("manualpollsettings", {})
        pin = mps.get("pin", "false").lower() == "true"
        unpin = mps.get("unpin", "false").lower() == "true"
        ttl_minutes = int(mps.get("timetolife", 480))  # теперь значение в минутах, по умолчанию 480 мин = 8 часов
        expires_at = datetime.utcnow() + timedelta(minutes=ttl_minutes)


    text = f"{question}\n\nПока нет участников."
    sent = await bot.send_message(chat_id, text)
    message_id = sent.message_id

    if pin:
        try:
            await bot.pin_chat_message(chat_id, message_id)
            pinned = True
        except Exception as e:
            logger.warning("Pin failed: %s", e)

    active_poll[chat_id] = {
        "command": command_name,
        "message_id": message_id,
        "expires_at": expires_at,
        "pinned": pinned,
        "participants": [],
        "unpin": unpin,
    }

    logger.info("Created poll %s in chat %s, expires at %s", command_name, chat_id, expires_at.isoformat())
    return active_poll[chat_id]


async def deactivate_poll(chat_id: int, reason="manual"):
    info = active_poll.get(chat_id)
    if not info:
        return False
    message_id = info["message_id"]
    pinned = info.get("pinned", False)
    unpin = info.get("unpin", False)
    if pinned and unpin:
        try:
            await bot.unpin_chat_message(chat_id=str(chat_id), message_id=message_id)
        except Exception as e:
            logger.warning("Unpin failed: %s", e)

    question = find_command_settings(chat_id, info["command"]).get("question", "Опрос завершён")
    participants = info["participants"]
    lines = [f"{question} (ЗАКРЫТ)", ""]
    if participants:
        for idx, p in enumerate(participants, start=1):
            uid, username, fullname = p
            if username:
                lines.append(f"{idx}. @{username} — {fullname}")
            else:
                lines.append(f"{idx}. {fullname}")
    else:
        lines.append("Никто не записался.")
    try:
        await bot.edit_message_text("\n".join(lines), chat_id, message_id)
    except Exception:
        pass

    del active_poll[chat_id]
    logger.info("Deactivated poll in %s (%s)", chat_id, reason)
    return True


# --- Handlers --- #

# Команды сабля
@dp.message(Command(commands=["saber"]))
async def saber_cmd(message: Message):
    chat_id = message.chat.id
    await create_poll(chat_id, "saber")
    # await message.reply("Создан опрос: сабля")


# Команды рапира
@dp.message(Command(commands=["rapier"]))
async def rapier_cmd(message: Message):
    chat_id = message.chat.id
    await create_poll(chat_id, "rapier")
    # await message.reply("Создан опрос: рапира")


# Деактивация
@dp.message(Command(commands=["deactivate"]))
async def deactivate_cmd(message: Message):
    chat_id = message.chat.id
    res = await deactivate_poll(chat_id)
    if res:
        await message.reply("Активный опрос деактивирован.")
    else:
        await message.reply("Активных опросов нет.")


# Плюс/минус для записи/удаления
@dp.message(F.text.in_({"+", "-"}))
async def plus_minus_handler(message: Message):
    chat_id = message.chat.id
    text = message.text.strip()
    info = active_poll.get(chat_id)
    if not info:
        return
    if datetime.utcnow() >= info["expires_at"]:
        await deactivate_poll(chat_id, reason="expired")
        return
    uid = message.from_user.id
    username = message.from_user.username
    fullname = message.from_user.full_name
    participants = info["participants"]

    if text == "+":
        if not any(p[0] == uid for p in participants):
            participants.append((uid, username, fullname))
            # await message.reply("Вы добавлены в список.")
            cmd_settings = find_command_settings(chat_id, info["command"])
            await edit_poll_message(chat_id, info["message_id"], cmd_settings["question"], participants)
    else:
        if any(p[0] == uid for p in participants):
            participants[:] = [p for p in participants if p[0] != uid]
            # await message.reply("Вы удалены из списка.")
            cmd_settings = find_command_settings(chat_id, info["command"])
            await edit_poll_message(chat_id, info["message_id"], cmd_settings["question"], participants)


async def autopoll_scheduler():
    logger.info("Autopoll scheduler started")
    while True:
        try:
            now = datetime.utcnow()
            for chat_id_str, chat_conf in SETTINGS["chats"].items():
                chat_id = int(chat_id_str)
                topics = chat_conf.get("topics", {})
                topic = topics.get("root", {})
                commands = topic.get("commands", {})
                for cmd_name, cmd_conf in commands.items():
                    if cmd_conf.get("autopoll", "false").lower() != "true":
                        continue
                    aps = cmd_conf.get("autopollsettings", {})
                    schedule_list = aps.get("schedule_autopoll", [])
                    for sched in schedule_list:
                        day = sched.get("day", "").strip().lower()[:3]
                        create_time = parse_time_str(sched.get("createmsg"))
                        today_weekday = now.strftime("%a").lower()[:3]
                        if day != today_weekday:
                            continue
                        sched_dt = datetime.combine(date.today(), create_time)
                        key = (chat_id, cmd_name, day, sched.get("createmsg"))
                        already = last_autocreate.get(key)
                        if sched_dt <= now < (sched_dt + timedelta(seconds=60)):
                            if already == date.today():
                                continue
                            if chat_id in active_poll:
                                last_autocreate[key] = date.today()
                                continue
                            await create_poll(chat_id, cmd_name, by_auto=True, schedule_entry=sched)
                            last_autocreate[key] = date.today()
            for cid, info in list(active_poll.items()):
                if datetime.utcnow() >= info["expires_at"]:
                    await deactivate_poll(cid, reason="expired")
        except Exception as e:
            logger.exception("Error in autopoll scheduler: %s", e)
        await asyncio.sleep(30)


async def main():
    asyncio.create_task(autopoll_scheduler())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
