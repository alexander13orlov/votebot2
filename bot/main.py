# -*- coding: utf-8 -*-
import asyncio
import json
import logging
from datetime import datetime, timedelta, time, date, timezone
from dateutil import parser
LOCAL_TZ = timezone(timedelta(hours=3))  # локальный часовой пояс (UTC+3)

from pathlib import Path
from typing import Optional, List, Dict, Any

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.exceptions import TelegramBadRequest

from .config import BOT_TOKEN  # оставлено как есть (config.py в пакете bot)

logging.basicConfig(level=logging.DEBUG, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent
SETTINGS_PATH = DATA_DIR / "settings.json"
HISTORY_PATH = DATA_DIR / "polls_history.json"  # файл для хранения последних 100 опросов

with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
    SETTINGS = json.load(f)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# В памяти — активный опрос на каждом чате (поддерживается не больше одного активного опроса глобально)
# active_poll: { chat_id: { "command": str, "message_id": int, "expires_at": datetime, "pinned": bool, "unpin": bool, "participants": [ (uid, username, fullname), ... ] } }
active_poll: Dict[int, Dict[str, Any]] = {}

# Для предотвращения повторного автозапуска одного и того же расписания в один день
last_autocreate: Dict[tuple, date] = {}
last_autodeactivate = {}
# История — список последних опросов (новейшие в начале)
history: List[Dict[str, Any]] = []


def parse_time_str(t: str) -> time:
    h, m, s = [int(x) for x in t.split(":")]
    return time(hour=h, minute=m, second=s)


def user_display_name(user: types.User) -> str:
    if user.username:
        return f"@{user.username} ({user.full_name})"
    return f"{user.full_name}"


def _serialize_participants(participants: List[tuple]) -> List[Dict[str, Any]]:
    return [{"uid": p[0], "username": p[1], "fullname": p[2]} for p in participants]


def _deserialize_participants(data: List[Dict[str, Any]]) -> List[tuple]:
    return [(d["uid"], d.get("username"), d.get("fullname")) for d in data]


def load_history():
    global history, active_poll
    if HISTORY_PATH.exists():
        try:
            with open(HISTORY_PATH, "r", encoding="utf-8") as f:
                history = json.load(f)
            # Найдём активные записи и восстановим только последнюю активную (восстанавливаем только один активный)
            active_entries = [h for h in history if h.get("active")]
            if active_entries:
                # возьмём наиболее позднюю по created_at
                active_entries.sort(key=lambda x: x.get("created_at", ""), reverse=True)
                entry = active_entries[0]
                chat_id = int(entry["chat_id"])
                # Восстановим в active_poll
                active_poll.clear()  # гарантируем, что только одна активная запись
                active_poll[chat_id] = {
                    "command": entry["command"],
                    "message_id": int(entry["message_id"]),
                    "expires_at": datetime.fromisoformat(entry["expires_at"]) if entry.get("expires_at") else None,
                    "pinned": bool(entry.get("pinned", False)),
                    "unpin": bool(entry.get("unpin", False)),
                    "participants": _deserialize_participants(entry.get("participants", [])),
                }
                logger.info("Restored active poll from history: chat=%s message=%s command=%s",
                            chat_id, entry["message_id"], entry["command"])
            else:
                active_poll.clear()
        except Exception as e:
            logger.exception("Failed to load history: %s", e)
            history = []
            active_poll.clear()
    else:
        history = []
        active_poll.clear()


def save_history():
    try:
        with open(HISTORY_PATH, "w", encoding="utf-8") as f:
            json.dump(history[:100], f, ensure_ascii=False, indent=2)
        logger.info("Saved history: %d entries -> %s", len(history[:100]), HISTORY_PATH)
    except Exception as e:
        logger.exception("Failed to save history: %s", e)


def add_history_entry(entry: Dict[str, Any]):
    """
    Добавляет новую запись в историю (в начало списка), держит максимум 100 элементов.
    """
    history.insert(0, entry)
    # Обрезаем до 100
    if len(history) > 100:
        del history[100:]
    save_history()


def update_history_entry(chat_id: int, message_id: int, **updates):
    """
    Находит запись по chat_id и message_id и обновляет её полями updates.
    Если не найдено — логируем предупреждение.
    """
    found = False
    for h in history:
        try:
            if int(h.get("chat_id")) == int(chat_id) and int(h.get("message_id")) == int(message_id):
                h.update(updates)
                found = True
                break
        except Exception:
            # если в данных что-то необычное — пропускаем запись
            continue

    if found:
        save_history()
        logger.info("Updated history entry: chat=%s message=%s updates=%s", chat_id, message_id, list(updates.keys()))
    else:
        logger.warning("History entry not found for update: chat=%s message=%s updates=%s", chat_id, message_id, updates)



async def edit_poll_message(chat_id: int, message_id: int, question: str, participants: List[tuple]):
    total = len(participants)
    lines = [f"[{total}]", question, ""]
    if participants:
        for p in participants:
            uid, username, fullname = p
            if username:
                lines.append(f"@{username}")
            else:
                lines.append(f"{fullname}")
    else:
        lines.append("Пока нет участников.")
    text = "\n".join(lines)
    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text)
    except TelegramBadRequest as e:
        logger.warning("Failed to edit poll message chat=%s message=%s: %s", chat_id, message_id, e)


def find_command_settings(chat_id: int, command_name: str) -> Optional[dict]:
    chat_conf = SETTINGS["chats"].get(str(chat_id))
    if not chat_conf:
        return None
    topics = chat_conf.get("topics", {})
    topic = topics.get("root", {})
    commands = topic.get("commands", {})
    return commands.get(command_name)


async def create_poll(chat_id: int, command_name: str, *, by_auto=False, schedule_entry: Optional[dict] = None):
    # Если есть активный опрос в любом чате — пропускаем (требование: максимум один активный глобально)
    if active_poll:
        logger.info("There is already an active poll, skipping creation of %s", command_name)
        return None

    cmd_settings = find_command_settings(chat_id, command_name)
    if not cmd_settings:
        logger.info("Command settings not found for %s in chat %s", command_name, chat_id)
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
        # timetolife хранится в минутaх в config — если это часы прежде, нужно менять в config
        ttl_minutes = int(mps.get("timetolife", 480))
        expires_at = datetime.utcnow() + timedelta(minutes=ttl_minutes)

    # Создаём СОВСЕМ НОВОЕ сообщение (никогда не переиспользуем старое)
    text = f"{question}\n\nПока нет участников."
    sent = await bot.send_message(chat_id, text)
    message_id = sent.message_id

    if pin:
        try:
            await bot.pin_chat_message(chat_id, message_id)
            pinned = True
        except Exception as e:
            logger.warning("Pin failed: %s", e)

    # Запомним активный опрос в памяти
    active_poll.clear()
    active_poll[chat_id] = {
        "command": command_name,
        "message_id": message_id,
        "expires_at": expires_at,
        "pinned": pinned,
        "participants": [],
        "unpin": unpin,
        "created_at": datetime.utcnow().isoformat(),
    }

    # Добавим запись в историю (active=True)
    entry = {
        "chat_id": str(chat_id),
        "message_id": str(message_id),
        "command": command_name,
        "participants": _serialize_participants([]),
        "created_at": active_poll[chat_id]["created_at"],
        "expires_at": expires_at.isoformat() if expires_at else None,
        "active": True,
        "pinned": pinned,
        "unpin": unpin,
    }
    add_history_entry(entry)

    logger.info("Created poll %s in chat %s, message_id=%s expires_at=%s", command_name, chat_id, message_id, expires_at.isoformat())
    return active_poll[chat_id]


async def deactivate_poll(chat_id: int, reason="manual"):
    info = active_poll.get(chat_id)
    if not info:
        logger.info("No active poll in chat %s to deactivate", chat_id)
        return False

    message_id = info["message_id"]
    pinned = info.get("pinned", False)
    unpin = info.get("unpin", False)

    unpin_success = False
    # Попытка открепить, если это нужно
    if pinned and unpin:
        try:
            await bot.unpin_chat_message(chat_id=str(chat_id), message_id=message_id)
            unpin_success = True
            info["pinned"] = False
            logger.info("Successfully unpinned message %s in chat %s", message_id, chat_id)
        except Exception as e:
            logger.warning("Unpin failed: %s", e)

    # Построим итоговый текст с пометкой "ЗАКРЫТ"
    question = find_command_settings(chat_id, info["command"]).get("question", "Опрос завершён")
    participants = info.get("participants", [])
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

    # Пытаемся обновить текст сообщения (именованные аргументы + chat_id как строка)
    try:
        await bot.edit_message_text(text="\n".join(lines), chat_id=str(chat_id), message_id=message_id)
        edit_ok = True
    except Exception as e:
        edit_ok = False
        logger.warning("Failed to edit message when closing poll chat=%s message=%s: %s", chat_id, message_id, e)

    # Решаем финальное значение pinned в истории: если we successfully unpinned -> False, иначе сохраняем текущее info['pinned']
    pinned_value = False if unpin_success else bool(info.get("pinned", False))

    # Обновим историю: пометим запись как inactive и установим pinned_value и финальный список участников
    update_history_entry(chat_id, message_id,
                         active=False,
                         pinned=pinned_value,
                         participants=_serialize_participants(participants))

    logger.info("History updated for chat=%s message=%s active=False pinned=%s edit_ok=%s",
                chat_id, message_id, pinned_value, edit_ok)

    # Удалим активный опрос из памяти — после деактивации он не должен более использоваться
    try:
        del active_poll[chat_id]
    except KeyError:
        pass

    logger.info("Deactivated poll in %s (%s). unpin_success=%s pinned_value=%s", chat_id, reason, unpin_success, pinned_value)
    return True

# --- Handlers --- #

@dp.message(Command(commands=["saber"]))
async def saber_cmd(message: Message):
    chat_id = message.chat.id
    await create_poll(chat_id, "saber")


@dp.message(Command(commands=["rapier"]))
async def rapier_cmd(message: Message):
    chat_id = message.chat.id
    await create_poll(chat_id, "rapier")


@dp.message(Command(commands=["deactivate"]))
async def deactivate_cmd(message: Message):
    chat_id = message.chat.id
    res = await deactivate_poll(chat_id, reason=f"manual by {message.from_user.id}")
    if res:
        await message.reply("Активный опрос деактивирован.")
    else:
        await message.reply("Активных опросов нет.")


@dp.message(F.text.in_({"+", "-"}))
async def plus_minus_handler(message: Message):
    chat_id = message.chat.id
    text = message.text.strip()
    info = active_poll.get(chat_id)
    if not info:
        return
    # проверяем expiry
    if info.get("expires_at") and datetime.utcnow() >= info["expires_at"]:
        await deactivate_poll(chat_id, reason="expired")
        return
    uid = message.from_user.id
    username = message.from_user.username
    fullname = message.from_user.full_name
    participants = info["participants"]

    if text == "+":
        if not any(p[0] == uid for p in participants):
            participants.append((uid, username, fullname))
            # обновляем сообщение
            cmd_settings = find_command_settings(chat_id, info["command"])
            await edit_poll_message(chat_id, info["message_id"], cmd_settings["question"], participants)
            # Обновим историю с новым списком участников
            update_history_entry(chat_id, info["message_id"], participants=_serialize_participants(participants))
    else:  # "-"
        if any(p[0] == uid for p in participants):
            participants[:] = [p for p in participants if p[0] != uid]
            cmd_settings = find_command_settings(chat_id, info["command"])
            await edit_poll_message(chat_id, info["message_id"], cmd_settings["question"], participants)
            update_history_entry(chat_id, info["message_id"], participants=_serialize_participants(participants))


async def autopoll_scheduler():
    logger.info("Autopoll scheduler started")
    while True:
        try:
            now_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
            now_local = now_utc.astimezone(LOCAL_TZ)
            logger.debug(f"[autopoll] Tick at {now_local.isoformat()} (weekday={now_local.strftime('%a').lower()[:3]})")

            # Проверка и деактивация активных опросов
            for cid, info in list(active_poll.items()):
                expires_at = info.get("expires_at")
                if expires_at:
                    # Приводим expires_at к timezone-aware, если нужно
                    if expires_at.tzinfo is None:
                        expires_at = expires_at.replace(tzinfo=LOCAL_TZ)
                    key_deact = (cid,)
                    already_deact = last_autodeactivate.get(key_deact)
                    if now_local >= expires_at and already_deact != date.today():
                        logger.info(f"[autopoll] Deactivating poll {cid} due to expiration")
                        await deactivate_poll(cid, reason="expired by scheduler")
                        last_autodeactivate[key_deact] = date.today()

            # Если уже есть активный опрос, пропускаем создание новых
            if active_poll:
                await asyncio.sleep(30)
                continue

            # Проходим по чатам и командам
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
                        today_weekday = now_local.strftime("%a").lower()[:3]

                        if day != today_weekday:
                            continue

                        sched_dt = datetime.combine(date.today(), create_time).replace(tzinfo=LOCAL_TZ)
                        key = (chat_id, cmd_name, day, sched.get("createmsg"))
                        already = last_autocreate.get(key)

                        logger.debug(
                            f"[autopoll] Check schedule: cmd={cmd_name}, day={day}, target={sched.get('createmsg')}, "
                            f"today={today_weekday}, sched_dt={sched_dt.isoformat()}, now_local={now_local.isoformat()}"
                        )

                        # Проверяем окно запуска (60 секунд)
                        if sched_dt <= now_local < (sched_dt + timedelta(seconds=60)):
                            if already == date.today():
                                logger.debug(f"[autopoll] Already executed today for {cmd_name}")
                                continue
                            if active_poll:
                                logger.debug(f"[autopoll] Active poll exists, skip creating new {cmd_name}")
                                last_autocreate[key] = date.today()
                                continue

                            logger.info(f"[autopoll] Triggering scheduled autopoll for {cmd_name} (chat {chat_id})")
                            await create_poll(chat_id, cmd_name, by_auto=True, schedule_entry=sched)
                            last_autocreate[key] = date.today()

        except Exception as e:
            logger.exception("Error in autopoll scheduler: %s", e)

        await asyncio.sleep(30)



async def main():
    # Загрузим историю и восстановим (если есть) активный опрос
    load_history()
    # Запустим автопланировщик
    asyncio.create_task(autopoll_scheduler())
    # Запуск Polling
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
