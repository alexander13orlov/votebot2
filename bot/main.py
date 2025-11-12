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
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.exceptions import TelegramBadRequest

from .config import BOT_TOKEN, ADMIN_IDS

import csv
import io
from datetime import datetime

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.filters import Command
import html
import re
from urllib.parse import urlparse

logging.basicConfig(level=logging.DEBUG, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent
SETTINGS_PATH = DATA_DIR / "settings.json"
HISTORY_PATH = DATA_DIR / "polls_history.json"  # файл для хранения последних 100 опросов

edit_sessions = {}  # {admin_id: session_data}
edit_waiting_for_link = {}  # {admin_id: True/False}


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


def build_poll_keyboard() -> InlineKeyboardMarkup:
    """Создает клавиатуру для опроса"""
    keyboard = [
        [
            InlineKeyboardButton(text="✅ Участвую", callback_data="poll_join"),
            InlineKeyboardButton(text="🔄 Пас", callback_data="poll_leave")  # Изменено на "Пас"
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)








































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

            # Найдём активные записи и восстановим последнюю активную
            active_entries = [h for h in history if h.get("active")]
            if active_entries:
                active_entries.sort(key=lambda x: x.get("created_at", ""), reverse=True)
                entry = active_entries[0]
                chat_id = int(entry["chat_id"])

                # Восстанавливаем expires_at с корректной TZ
                expires_at = None
                if entry.get("expires_at"):
                    try:
                        dt = datetime.fromisoformat(entry["expires_at"])
                        if dt.tzinfo is None:
                            # если часовой пояс не указан — считаем, что это локальное время (например, Москва)
                            dt = dt.replace(tzinfo=LOCAL_TZ)
                        else:
                            # приводим к локальному
                            dt = dt.astimezone(LOCAL_TZ)
                        expires_at = dt
                    except Exception as e:
                        logger.warning("Invalid expires_at format in history: %s", e)

                active_poll.clear()
                active_poll[chat_id] = {
                    "command": entry["command"],
                    "message_id": int(entry["message_id"]),
                    "expires_at": expires_at,
                    "pinned": bool(entry.get("pinned", False)),
                    "unpin": bool(entry.get("unpin", False)),
                    "participants": _deserialize_participants(entry.get("participants", [])),
                }

                logger.info(
                    "Restored active poll from history: chat=%s message=%s command=%s expires_at=%s",
                    chat_id, entry["message_id"], entry["command"], expires_at
                )
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




def build_poll_text_with_timer(question: str, participants: List[tuple], expires_at: datetime) -> str:
    """
    Формирует текст опроса с моноширинным форматированием через HTML
    """
    total = len(participants)
    now_utc = datetime.now(timezone.utc)
    remaining = expires_at - now_utc

    if remaining.total_seconds() <= 0:
        remaining_str = "0ч0м"
    else:
        hours, remainder = divmod(int(remaining.total_seconds()), 3600)
        minutes, _ = divmod(remainder, 60)
        remaining_str = f"{hours}ч{minutes}м"

    # Экранируем для HTML
    question_escaped = html.escape(question)
    
    # Формируем текст с моноширинным шрифтом через <code> тег
    lines = []
    lines.append(f"<b>{question_escaped}</b>")
    lines.append(f"⏰ Осталось: <code>{html.escape(remaining_str)}</code>")
    lines.append(f"Участники: <code>[{total}]</code>")
    lines.append("")
    
    if participants:
        for idx, p in enumerate(participants, start=1):
            uid, username, fullname = p
            fullname_escaped = html.escape(fullname)
            
            if username:
                username_escaped = html.escape(username)
                # Обернем всю строку в <code> для моноширинного шрифта
                lines.append(f"<code>{idx:2d}. @{username_escaped} - {fullname_escaped}</code>")
            else:
                lines.append(f"<code>{idx:2d}. {fullname_escaped}</code>")
    else:
        lines.append("<code>┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄</code>")
        lines.append("<code>Пока нет участников</code>")

    return "\n".join(lines)


async def active_poll_updater():
    """
    Фоновый цикл, который каждые 30 секунд обновляет все активные опросы с таймером.
    """
    while True:
        try:
            for chat_id, info in list(active_poll.items()):
                message_id = info["message_id"]
                expires_at = info["expires_at"]
                participants = info.get("participants", [])

                cmd_settings = find_command_settings(chat_id, info["command"])
                question = cmd_settings.get("question", info["command"]) if cmd_settings else info["command"]

                text = build_poll_text_with_timer(question, participants, expires_at)

                last_text = info.get("last_text")
                if text != last_text:
                    try:
                        await bot.edit_message_text(
                            chat_id=chat_id, 
                            message_id=message_id, 
                            text=text,
                            reply_markup=build_poll_keyboard(),
                            parse_mode="HTML"
                        )
                        info["last_text"] = text
                    except TelegramBadRequest as e:
                        if "message is not modified" in str(e):
                            pass
                        elif "message to edit not found" in str(e):
                            logger.warning(f"Message not found in updater: chat_id={chat_id}, message_id={message_id}")
                            if chat_id in active_poll:
                                del active_poll[chat_id]
                        elif "query is too old" in str(e):
                            logger.warning(f"Old callback query during updater: {e}")
                        else:
                            logger.warning(
                                "Failed to update poll message with timer chat=%s message=%s: %s",
                                chat_id, message_id, e
                            )
        except Exception as e:
            logger.exception("Error in active_poll_updater: %s", e)

        await asyncio.sleep(30)




async def edit_poll_message(chat_id, message_id, question, participants, expires_at):
    if chat_id not in active_poll:
        return
        
    text = build_poll_text_with_timer(question, participants, expires_at)
    last_text = active_poll[chat_id].get("last_text")
    if text == last_text:
        return
        
    try:
        await bot.edit_message_text(
            chat_id=chat_id, 
            message_id=message_id, 
            text=text,
            reply_markup=build_poll_keyboard(),
            parse_mode="HTML"
        )
        active_poll[chat_id]["last_text"] = text
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            pass
        elif "message to edit not found" in str(e):
            logger.warning(f"Message not found in edit_poll_message: chat_id={chat_id}, message_id={message_id}")
            if chat_id in active_poll:
                del active_poll[chat_id]
        elif "query is too old" in str(e):
            logger.warning(f"Old callback query during message edit: {e}")
        else:
            logger.warning(
                "Failed to edit poll message chat=%s message=%s: %s", chat_id, message_id, e
            )




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
        # local_dt — дата+время в LOCAL_TZ (UTC+3)
        local_dt = datetime.combine(date.today(), deact_time).replace(tzinfo=LOCAL_TZ)
        # expires_at — в UTC (храним/сравниваем в UTC)
        expires_at = local_dt.astimezone(timezone.utc).replace(microsecond=0) 
        logger.debug("Auto poll: local_dt=%s expires_at(utc)=%s", local_dt.isoformat(), expires_at.isoformat())
    else:
        mps = cmd_settings.get("manualpollsettings", {})
        pin = mps.get("pin", "false").lower() == "true"
        unpin = mps.get("unpin", "false").lower() == "true"

        # Новый способ: берём schedule_autopoll
        aps = cmd_settings.get("autopollsettings", {})
        schedule_list = aps.get("schedule_autopoll", [])

        now_local = datetime.now(timezone.utc).astimezone(LOCAL_TZ)
        soonest_dt = None

        for sched in schedule_list:
            day_str = sched.get("day", "").strip().lower()[:3]  # "mon", "tue", ...
            deactivatemsg = sched.get("deactivatemsg")
            if not deactivatemsg:
                continue
            deact_time = parse_time_str(deactivatemsg)

            # Переводим день в число (0=Mon ... 6=Sun)
            weekday_map = {"mon":0,"tue":1,"wed":2,"thu":3,"fri":4,"sat":5,"sun":6}
            target_wd = weekday_map.get(day_str)
            if target_wd is None:
                continue

            # Вычисляем дату ближайшего target_wd после now_local
            days_ahead = (target_wd - now_local.weekday() + 7) % 7
            candidate_date = now_local.date() + timedelta(days=days_ahead)
            candidate_dt = datetime.combine(candidate_date, deact_time).replace(tzinfo=LOCAL_TZ)

            # Если время уже прошло сегодня, идём на следующую неделю
            if candidate_dt <= now_local:
                candidate_dt += timedelta(days=7)

            if soonest_dt is None or candidate_dt < soonest_dt:
                soonest_dt = candidate_dt

        if soonest_dt is None:
            # fallback: 8 часов по UTC, на случай, если расписание пустое
            expires_at = (datetime.now(timezone.utc) + timedelta(hours=8)).replace(microsecond=0)
        else:
            expires_at = soonest_dt.astimezone(timezone.utc).replace(microsecond=0)

        logger.debug("Manual poll: expires_at(utc)=%s", expires_at.isoformat())

    # Создаём СОВСЕМ НОВОЕ сообщение (никогда не переиспользуем старое)
    text = build_poll_text_with_timer(
        question,
        participants=[],
        expires_at=expires_at
    )
    
    # Отправляем сообщение с инлайн-клавиатурой
    sent = await bot.send_message(
        chat_id, 
        text, 
        reply_markup=build_poll_keyboard(),
        parse_mode="HTML"  # Добавляем parse_mode
    )
    message_id = sent.message_id

    if pin:
        try:
            await bot.pin_chat_message(chat_id, message_id, disable_notification=True)
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
        "created_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
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

    if pinned and unpin:
        try:
            await bot.unpin_chat_message(chat_id=str(chat_id), message_id=message_id)
            unpin_success = True
            info["pinned"] = False
            logger.info("Successfully unpinned message %s in chat %s", message_id, chat_id)
        except Exception as e:
            logger.warning("Unpin failed: %s", e)

    question = find_command_settings(chat_id, info["command"]).get("question", "Опрос завершён")
    participants = info.get("participants", [])
    total = len(participants)
    
    # Экранируем для HTML
    question_escaped = html.escape(question)
    
    # Формируем текст для завершенного опроса
    lines = []
    lines.append(f"<b>{question_escaped} - ЗАКРЫТ</b>")
    lines.append(f"Участники: <code>[{total}]</code>")
    lines.append("")
    
    if participants:
        for idx, p in enumerate(participants, start=1):
            uid, username, fullname = p
            fullname_escaped = html.escape(fullname)
            
            if username:
                username_escaped = html.escape(username)
                lines.append(f"<code>{idx:2d}. @{username_escaped} - {fullname_escaped}</code>")
            else:
                lines.append(f"<code>{idx:2d}. {fullname_escaped}</code>")
    else:
        lines.append("<code>┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄</code>")
        lines.append("<code>Никто не записался</code>")

    new_text = "\n".join(lines)
    
    last_text = info.get("last_text")
    if new_text != last_text:
        try:
            await bot.edit_message_text(
                chat_id=str(chat_id), 
                message_id=message_id, 
                text=new_text,
                reply_markup=None,
                parse_mode="HTML"
            )
            info["last_text"] = new_text
            edit_ok = True
        except TelegramBadRequest as e:
            if "message is not modified" in str(e):
                edit_ok = True
            elif "message to edit not found" in str(e):
                logger.warning(f"Message not found when closing poll: chat_id={chat_id}, message_id={message_id}")
                edit_ok = False
            elif "query is too old" in str(e):
                logger.warning(f"Old callback query during deactivation: {e}")
                edit_ok = False
            else:
                edit_ok = False
                logger.warning(
                    "Failed to edit message when closing poll chat=%s message=%s: %s", chat_id, message_id, e
                )
    else:
        edit_ok = True

    pinned_value = False if unpin_success else bool(info.get("pinned", False))
    update_history_entry(chat_id, message_id,
                         active=False,
                         pinned=pinned_value,
                         participants=_serialize_participants(participants))

    logger.info("History updated for chat=%s message=%s active=False pinned=%s edit_ok=%s",
                chat_id, message_id, pinned_value, edit_ok)

    try:
        del active_poll[chat_id]
    except KeyError:
        pass

    logger.info("Deactivated poll in %s (%s). unpin_success=%s pinned_value=%s", chat_id, reason, unpin_success, pinned_value)
    return True




# Функция для извлечения chat_id и message_id из ссылки
def extract_ids_from_link(link: str) -> tuple[Optional[int], Optional[int]]:
    logger.debug(f"🔍 Extracting IDs from link: {link}")
    try:
        parsed = urlparse(link)
        path_parts = parsed.path.split('/')
        
        if '/c/' in link:
            c_index = path_parts.index('c')
            if len(path_parts) > c_index + 3:
                chat_id = int(path_parts[c_index + 1])
                message_id = int(path_parts[c_index + 3])
                
                # Определяем тип чата по длине chat_id
                # Обычно супергруппы имеют 10-значные ID в ссылках
                if len(str(chat_id)) == 10:
                    # Это супергруппа - добавляем префикс -100
                    chat_id_with_prefix = int(f"-100{chat_id}")
                else:
                    # Это обычная группа или канал
                    chat_id_with_prefix = chat_id
                
                logger.debug(f"✅ Successfully extracted: chat_id={chat_id_with_prefix}, message_id={message_id}")
                return chat_id_with_prefix, message_id
                
    except (ValueError, IndexError, AttributeError) as e:
        logger.error(f"❌ Error extracting IDs from link '{link}': {e}")
    
    return None, None





# Функция для поиска опроса в истории
def find_poll_in_history(chat_id: int, message_id: int) -> Optional[dict]:
    logger.debug(f"🔍 Searching in history: {len(history)} entries")
    for idx, entry in enumerate(history):
        entry_chat_id = int(entry.get("chat_id"))
        entry_message_id = int(entry.get("message_id"))
        logger.debug(f"  Entry {idx}: chat_id={entry_chat_id}, message_id={entry_message_id}")
        
        if entry_chat_id == chat_id and entry_message_id == message_id:
            logger.debug(f"✅ Found match at index {idx}")
            return entry
    
    logger.debug(f"❌ No match found for chat_id={chat_id}, message_id={message_id}")
    return None

# Функция для получения уникальных пользователей из истории
def get_unique_users_from_history() -> List[tuple]:
    unique_users = {}
    for entry in history:
        for participant in entry.get("participants", []):
            uid = participant.get("uid")
            if uid and uid not in unique_users:
                unique_users[uid] = (
                    uid,
                    participant.get("username"),
                    participant.get("fullname", "")
                )
    return list(unique_users.values())

# Функция для построения клавиатуры редактирования
def build_edit_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton(text="➕ Добавить", callback_data="edit_add"),
            InlineKeyboardButton(text="➖ Удалить", callback_data="edit_remove"),
        ],
        [
            InlineKeyboardButton(text="✅ Завершить", callback_data="edit_finish")
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# Функция для построения клавиатуры с пользователями для удаления
def build_remove_user_keyboard(participants: List[tuple]) -> InlineKeyboardMarkup:
    keyboard = []
    for uid, username, fullname in participants:
        display_name = f"@{username}" if username else fullname
        if len(display_name) > 30:
            display_name = display_name[:30] + "..."
        keyboard.append([
            InlineKeyboardButton(
                text=display_name, 
                callback_data=f"edit_remove_{uid}"
            )
        ])
    keyboard.append([
        InlineKeyboardButton(text="↩️ Назад", callback_data="edit_back")
    ])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# Функция для построения клавиатуры с пользователями для добавления
def build_add_user_keyboard(available_users: List[tuple]) -> InlineKeyboardMarkup:
    keyboard = []
    for uid, username, fullname in available_users:
        display_name = f"@{username}" if username else fullname
        if len(display_name) > 30:
            display_name = display_name[:30] + "..."
        keyboard.append([
            InlineKeyboardButton(
                text=display_name, 
                callback_data=f"edit_add_{uid}"
            )
        ])
    keyboard.append([
        InlineKeyboardButton(text="↩️ Назад", callback_data="edit_back")
    ])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


# Таймер сессии редактирования (1 минута)
async def edit_session_timer(admin_id: int):
    await asyncio.sleep(60)  # 1 минута
    
    if admin_id in edit_sessions:
        session = edit_sessions[admin_id]
        time_since_last_action = datetime.now(timezone.utc) - session["last_action_time"]
        
        if time_since_last_action.total_seconds() >= 60:
            try:
                await bot.edit_message_text(
                    chat_id=admin_id,
                    message_id=session["private_message_id"],
                    text="Сессия редактирования завершена по таймауту.",
                    reply_markup=None
                )
            except TelegramBadRequest as e:
                if "query is too old" in str(e) or "message to edit not found" in str(e):
                    pass
                else:
                    logger.warning(f"Failed to edit message in session timer: {e}")
            
            del edit_sessions[admin_id]

# Обновляем время действия в сессии
def update_session_time(admin_id: int):
    if admin_id in edit_sessions:
        edit_sessions[admin_id]["last_action_time"] = datetime.now(timezone.utc)



# Функция для обновления сообщения опроса
async def update_poll_message(chat_id: int, message_id: int, poll_entry: dict, participants: List[tuple]) -> bool:
    try:
        # Определяем, активен ли опрос
        is_active = poll_entry.get("active", False)
        command = poll_entry.get("command", "")
        question = find_command_settings(chat_id, command).get("question", command) if find_command_settings(chat_id, command) else command
        
        if is_active:
            # Активный опрос - используем формат с таймером
            expires_at_str = poll_entry.get("expires_at")
            if expires_at_str:
                expires_at = datetime.fromisoformat(expires_at_str)
            else:
                expires_at = datetime.now(timezone.utc) + timedelta(hours=1)  # fallback
            
            text = build_poll_text_with_timer(question, participants, expires_at)
            
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                reply_markup=build_poll_keyboard(),
                parse_mode="HTML"
            )
        else:
            # Закрытый опрос
            text = build_closed_poll_text(question, participants)
            
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                parse_mode="HTML"
            )
        
        return True
        
    except TelegramBadRequest as e:
        if "message to edit not found" in str(e) or "message is not modified" in str(e):
            return False
        elif "query is too old" in str(e):
            return False
        else:
            logger.warning(f"Failed to update poll message during edit: {e}")
            return False

# Функция для построения текста закрытого опроса
def build_closed_poll_text(question: str, participants: List[tuple]) -> str:
    total = len(participants)
    
    # Экранируем для HTML
    question_escaped = html.escape(question)
    
    lines = []
    lines.append(f"<b>{question_escaped} - ЗАКРЫТ</b>")
    lines.append(f"Участники: <code>[{total}]</code>")
    lines.append("")
    
    if participants:
        for idx, p in enumerate(participants, start=1):
            uid, username, fullname = p
            fullname_escaped = html.escape(fullname)
            
            if username:
                username_escaped = html.escape(username)
                lines.append(f"<code>{idx:2d}. @{username_escaped} - {fullname_escaped}</code>")
            else:
                lines.append(f"<code>{idx:2d}. {fullname_escaped}</code>")
    else:
        lines.append("<code>┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄</code>")
        lines.append("<code>Никто не записался</code>")

    return "\n".join(lines)

def build_edit_poll_text(question: str, participants: List[tuple]) -> str:
    """
    Формирует текст для интерфейса редактирования опроса
    """
    total = len(participants)
    
    # Экранируем для HTML
    question_escaped = html.escape(question)
    
    lines = []
    lines.append(f"<b>Редактирование опроса: {question_escaped}</b>")
    lines.append(f"Участников: <code>[{total}]</code>")
    lines.append("")
    
    if participants:
        for idx, p in enumerate(participants, start=1):
            uid, username, fullname = p
            fullname_escaped = html.escape(fullname)
            
            if username:
                username_escaped = html.escape(username)
                lines.append(f"<code>{idx:2d}. @{username_escaped} - {fullname_escaped}</code>")
            else:
                lines.append(f"<code>{idx:2d}. {fullname_escaped}</code>")
    else:
        lines.append("<code>┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄</code>")
        lines.append("<code>Пока нет участников</code>")

    lines.append("")
    lines.append("Выберите действие:")
    
    return "\n".join(lines)







# ---------------------------------------------------- Handlers ------------------------------------------------------ #


# Глобальная переменная для хранения состояния
stat_waiting_username = {}
# Добавим словарь для отслеживания последнего callback от пользователя
user_last_callback = {}



# Обработчик callback'ов редактирования
@dp.callback_query(F.data.startswith("edit_"))
async def edit_callback_handler(callback: CallbackQuery):
    admin_id = callback.from_user.id
    
    # Проверяем активную сессию
    if admin_id not in edit_sessions:
        try:
            await callback.answer("Сессия редактирования завершена.", show_alert=True)
        except TelegramBadRequest as e:
            if "query is too old" in str(e):
                return
            else:
                raise
        return

    update_session_time(admin_id)
    data = callback.data
    session = edit_sessions[admin_id]
    poll_entry = session["poll_entry"]
    participants = _deserialize_participants(poll_entry.get("participants", []))

    if data == "edit_finish":
        # Завершаем сессию
        try:
            await callback.message.edit_text(
                "Редактирование завершено.",
                reply_markup=None
            )
        except TelegramBadRequest as e:
            if "query is too old" in str(e):
                pass
            else:
                logger.warning(f"Failed to edit message in edit finish: {e}")
        
        del edit_sessions[admin_id]
        await callback.answer()
        return

    elif data == "edit_back":
        # Возвращаемся к основному меню
        participants = _deserialize_participants(poll_entry.get("participants", []))
        question = poll_entry.get("command", "Опрос")
        text = build_edit_poll_text(question, participants)
        
        try:
            await callback.message.edit_text(
                text,
                reply_markup=build_edit_keyboard(),
                parse_mode="HTML"
            )
        except TelegramBadRequest as e:
            if "query is too old" in str(e):
                pass
            else:
                logger.warning(f"Failed to edit message in edit back: {e}")
        
        await callback.answer()
        return

    elif data == "edit_remove":
        # Показываем список участников для удаления
        if not participants:
            try:
                await callback.answer("В опросе нет участников для удаления.", show_alert=True)
            except TelegramBadRequest as e:
                if "query is too old" in str(e):
                    return
                else:
                    raise
            return

        try:
            await callback.message.edit_reply_markup(
                reply_markup=build_remove_user_keyboard(participants)
            )
        except TelegramBadRequest as e:
            if "query is too old" in str(e):
                pass
            else:
                logger.warning(f"Failed to edit message in edit remove: {e}")
        
        await callback.answer()
        return

    elif data == "edit_add":
        # Показываем список пользователей для добавления
        all_users = get_unique_users_from_history()
        current_uids = [p[0] for p in participants]
        available_users = [user for user in all_users if user[0] not in current_uids]
        
        if not available_users:
            try:
                await callback.answer("Нет доступных пользователей для добавления.", show_alert=True)
            except TelegramBadRequest as e:
                if "query is too old" in str(e):
                    return
                else:
                    raise
            return

        try:
            await callback.message.edit_reply_markup(
                reply_markup=build_add_user_keyboard(available_users)
            )
        except TelegramBadRequest as e:
            if "query is too old" in str(e):
                pass
            else:
                logger.warning(f"Failed to edit message in edit add: {e}")
        
        await callback.answer()
        return

    elif data.startswith("edit_remove_"):
        # Удаляем пользователя
        uid = int(data.split("_")[2])
        
        # Находим пользователя
        user_to_remove = None
        for user in participants:
            if user[0] == uid:
                user_to_remove = user
                break
        
        if not user_to_remove:
            try:
                await callback.answer("Пользователь не найден в опросе.", show_alert=True)
            except TelegramBadRequest as e:
                if "query is too old" in str(e):
                    return
                else:
                    raise
            return

        # Удаляем пользователя из опроса
        new_participants = [p for p in participants if p[0] != uid]
        poll_entry["participants"] = _serialize_participants(new_participants)
        
        # ОБНОВЛЯЕМ АКТИВНЫЙ ОПРОС В ПАМЯТИ (если он активен)
        chat_id = session["chat_id"]
        message_id = session["message_id"]
        if chat_id in active_poll and active_poll[chat_id]["message_id"] == message_id:
            active_poll[chat_id]["participants"] = new_participants
            # Сбрасываем last_text, чтобы принудительно обновить сообщение
            if "last_text" in active_poll[chat_id]:
                del active_poll[chat_id]["last_text"]
            logger.info(f"✅ Updated active poll in memory for chat {chat_id}")
        
        # Обновляем сообщение в чате (если возможно)
        success = await update_poll_message(
            session["chat_id"], 
            session["message_id"], 
            poll_entry, 
            new_participants
        )
        
        # Обновляем историю
        update_history_entry(
            session["chat_id"], 
            session["message_id"],
            participants=_serialize_participants(new_participants)
        )
    
            
        # Возвращаемся к основному меню
        participants_count = len(new_participants)
        question = poll_entry.get("command", "Опрос")
        # Формируем текст с обновленным списком участников
        text = build_edit_poll_text(question, new_participants)
        
 
        if success:
            text += f"\n\n✅ Пользователь удален. Сообщение в чате обновлено."
        else:
            text += f"\n\n✅ Пользователь удален из истории. Не удалось обновить сообщение в чате (прошло более 48 часов)."
        
        text += "\n\nВыберите действие:"
        
        try:
            await callback.message.edit_text(
                text,
                reply_markup=build_edit_keyboard(),
                parse_mode="HTML"
            )
        except TelegramBadRequest as e:
            if "query is too old" in str(e):
                pass
            else:
                logger.warning(f"Failed to edit message in remove user: {e}")
        
        await callback.answer()
        return

    elif data.startswith("edit_add_"):
        # Добавляем пользователя
        uid = int(data.split("_")[2])
        
        # Находим пользователя в истории
        user_to_add = None
        all_users = get_unique_users_from_history()
        for user in all_users:
            if user[0] == uid:
                user_to_add = user
                break
        
        if not user_to_add:
            try:
                await callback.answer("Пользователь не найден в истории.", show_alert=True)
            except TelegramBadRequest as e:
                if "query is too old" in str(e):
                    return
                else:
                    raise
            return

        # Добавляем пользователя в опрос
        new_participants = participants + [user_to_add]
        poll_entry["participants"] = _serialize_participants(new_participants)
        
        # ОБНОВЛЯЕМ АКТИВНЫЙ ОПРОС В ПАМЯТИ (если он активен)
        chat_id = session["chat_id"]
        message_id = session["message_id"]
        if chat_id in active_poll and active_poll[chat_id]["message_id"] == message_id:
            active_poll[chat_id]["participants"] = new_participants
            # Сбрасываем last_text, чтобы принудительно обновить сообщение
            if "last_text" in active_poll[chat_id]:
                del active_poll[chat_id]["last_text"]
            logger.info(f"✅ Updated active poll in memory for chat {chat_id}")
        
        # Обновляем сообщение в чате (если возможно)
        success = await update_poll_message(
            session["chat_id"], 
            session["message_id"], 
            poll_entry, 
            new_participants
        )
        
        # Обновляем историю
        update_history_entry(
            session["chat_id"], 
            session["message_id"],
            participants=_serialize_participants(new_participants)
        )
        
        # ... остальной код ...
        
        # Возвращаемся к основному меню
        participants_count = len(new_participants)
        question = poll_entry.get("command", "Опрос")
        # Формируем текст с обновленным списком участников
        text = build_edit_poll_text(question, new_participants)
        
        if success:
            text += f"\n\n✅ Пользователь добавлен. Сообщение в чате обновлено."
        else:
            text += f"\n\n✅ Пользователь добавлен в историю. Не удалось обновить сообщение в чате (прошло более 48 часов)."
        
        text += "\n\nВыберите действие:"
        
        try:
            await callback.message.edit_text(
                text,
                reply_markup=build_edit_keyboard(),
                parse_mode="HTML"
            )
        except TelegramBadRequest as e:
            if "query is too old" in str(e):
                pass
            else:
                logger.warning(f"Failed to edit message in add user: {e}")
        
        await callback.answer()
        return


@dp.message(Command(commands=["edit"]))
async def edit_cmd(message: Message):
    user_id = message.from_user.id
    logger.info(f"🎯 Edit command received from user {user_id}")
    
    # Проверяем, что команда в личном чате
    if message.chat.type != "private":
        logger.warning(f"❌ Edit command used in non-private chat: {message.chat.type}")
        try:
            await message.reply("Эта команда доступна только в личном чате с ботом.")
        except Exception as e:
            logger.error(f"Failed to send private chat warning: {e}")
        return

    # Проверяем права админа
    user_id_str = str(user_id)
    if user_id_str not in ADMIN_IDS:
        logger.warning(f"❌ User {user_id} is not in ADMIN_IDS")
        try:
            await message.reply("У вас нет прав для использования этой команды.")
        except Exception as e:
            logger.error(f"Failed to send admin rights warning: {e}")
        return

    # Показываем доступные опросы для отладки
    logger.info(f"📊 Available polls in history:")
    for idx, entry in enumerate(history[:5]):  # Показываем первые 5
        logger.info(f"  {idx}: chat_id={entry.get('chat_id')}, message_id={entry.get('message_id')}, command={entry.get('command')}")

    # Устанавливаем состояние ожидания ссылки
    edit_waiting_for_link[user_id] = True
    logger.info(f"✅ Set waiting_for_link=True for user {user_id}")
    
    try:
        await message.reply("Пришлите ссылку на опрос. Пример: https://t.me/c/1570728084/1/3110")
        logger.info(f"📤 Sent link request to user {user_id}")
    except Exception as e:
        logger.error(f"❌ Failed to send link request to user {user_id}: {e}")


# Улучшенный обработчик инлайн-кнопок опроса
@dp.callback_query(F.data.startswith("poll_"))
async def poll_button_handler(callback: CallbackQuery):
    chat_id = callback.message.chat.id
    user = callback.from_user
    uid = user.id
    username = user.username
    fullname = user.full_name
    
    # Защита от спама нажатий - игнорируем частые нажатия от одного пользователя
    current_time = datetime.now(timezone.utc).timestamp()
    last_callback_time = user_last_callback.get(uid, 0)
    if current_time - last_callback_time < 1:  # Не чаще 1 раза в секунду
        try:
            await callback.answer("Подождите немного перед следующим действием", show_alert=False)
        except TelegramBadRequest as e:
            if "query is too old" in str(e):
                return  # Игнорируем устаревшие запросы
            else:
                raise
        return
    
    user_last_callback[uid] = current_time
    
    # Проверяем, есть ли активный опрос
    info = active_poll.get(chat_id)
    if not info:
        try:
            await callback.answer("Опрос не активен", show_alert=True)
        except TelegramBadRequest as e:
            if "query is too old" in str(e):
                return  # Игнорируем устаревшие запросы
            else:
                raise
        return
        
    # Проверяем, не истек ли опрос
    expires_at = info.get("expires_at")
    if expires_at and datetime.now(timezone.utc) >= expires_at:
        try:
            await callback.answer("Опрос уже завершен", show_alert=True)
        except TelegramBadRequest as e:
            if "query is too old" in str(e):
                return  # Игнорируем устаревшие запросы
            else:
                raise
        return
    
    participants = info.get("participants", [])
    user_in_list = any(p[0] == uid for p in participants)
    changed = False
    action_performed = False
    
    if callback.data == "poll_join":
        if not user_in_list:
            participants.append((uid, username, fullname))
            changed = True
            action_performed = True
            try:
                await callback.answer("Вы добавлены в список участников")
            except TelegramBadRequest as e:
                if "query is too old" in str(e):
                    # Откатываем изменения, если запрос устарел
                    participants.remove((uid, username, fullname))
                    return
                else:
                    raise
        else:
            try:
                await callback.answer("Вы уже в списке участников")
            except TelegramBadRequest as e:
                if "query is too old" in str(e):
                    return
                else:
                    raise
            
    elif callback.data == "poll_leave":
        if user_in_list:
            # Сохраняем участника для возможного отката
            participant_to_remove = next(p for p in participants if p[0] == uid)
            participants[:] = [p for p in participants if p[0] != uid]
            changed = True
            action_performed = True
            try:
                await callback.answer("Вы удалены из списка участников")
            except TelegramBadRequest as e:
                if "query is too old" in str(e):
                    # Откатываем изменения, если запрос устарел
                    participants.append(participant_to_remove)
                    return
                else:
                    raise
        else:
            try:
                await callback.answer("Вас нет в списке участников")
            except TelegramBadRequest as e:
                if "query is too old" in str(e):
                    return
                else:
                    raise
    
    # Обновляем сообщение только если произошли реальные изменения
    if changed and action_performed:
        # Обновляем сообщение опроса
        cmd_settings = find_command_settings(chat_id, info["command"])
        question = cmd_settings.get("question", info["command"]) if cmd_settings else info["command"]
        
        # Формируем новый текст
        new_text = build_poll_text_with_timer(question, participants, expires_at)
        
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=info["message_id"],
                text=new_text,
                reply_markup=build_poll_keyboard(),
                parse_mode="HTML"
            )
            # Сохраняем новый текст
            if "last_text" not in info or info["last_text"] != new_text:
                info["last_text"] = new_text
                
        except TelegramBadRequest as e:
            if "message is not modified" in str(e):
                pass
            elif "message to edit not found" in str(e):
                logger.warning(f"Message not found: chat_id={chat_id}, message_id={info['message_id']}")
            elif "query is too old" in str(e):
                logger.warning(f"Old callback query during message edit: {e}")
            else:
                logger.warning(f"Failed to update poll message: {e}")
        
        # Обновляем историю
        update_history_entry(chat_id, info["message_id"], participants=_serialize_participants(participants))


@dp.message(Command(commands=["stat"]))
async def stat_cmd(message: Message):
    # Проверяем, что команда вызвана в личном чате
    if message.chat.type != "private":
        try:
            await message.reply("Эта команда доступна только в личном чате с ботом.")
        except TelegramBadRequest as e:
            if "query is too old" in str(e):
                return
            else:
                raise
        return

    # Проверяем права админа
    user_id = str(message.from_user.id)
    if user_id not in ADMIN_IDS:
        try:
            await message.reply("У вас нет прав для использования этой команды.")
        except TelegramBadRequest as e:
            if "query is too old" in str(e):
                return
            else:
                raise
        return

    # Собираем уникальные uid и соответствующие данные из истории
    user_data = {}
    for entry in history:
        for participant in entry.get("participants", []):
            uid = participant.get("uid")
            username = participant.get("username")
            fullname = participant.get("fullname", "")
            
            if uid:  # используем uid вместо username
                # Если uid уже есть, сохраняем самые актуальные данные (из последней записи)
                if uid not in user_data:
                    user_data[uid] = {
                        "username": username,
                        "fullname": fullname
                    }
                # Если в текущей записи есть username, а в сохраненных данных нет - обновляем
                elif username and not user_data[uid]["username"]:
                    user_data[uid]["username"] = username
                    user_data[uid]["fullname"] = fullname

    if not user_data:
        try:
            await message.reply("В истории опросов нет участников.")
        except TelegramBadRequest as e:
            if "query is too old" in str(e):
                return
            else:
                raise
        return

    # Создаем инлайн-клавиатуру с кнопками
    keyboard = []
    
    # Кнопка "ВСЕ" в начале
    keyboard.append([InlineKeyboardButton(text="👥 ВСЕ", callback_data="stat_ALL")])
    
    # Кнопки с данными пользователей
    for uid, data in sorted(user_data.items()):
        username = data["username"]
        fullname = data["fullname"]
        
        # Формируем текст кнопки: username + fullname, или только fullname если username нет
        if username:
            button_text = f"@{username} {fullname}"
        else:
            button_text = fullname
        
        # Обрезаем если слишком длинный
        max_button_length = 30
        if len(button_text) > max_button_length:
            button_text = button_text[:max_button_length] + "..."
        
        # Используем uid в callback_data
        keyboard.append([InlineKeyboardButton(text=button_text, callback_data=f"stat_{uid}")])
    
    # Кнопка "Отмена" в конце
    keyboard.append([InlineKeyboardButton(text="❌ Отмена", callback_data="stat_cancel")])

    reply_markup = InlineKeyboardMarkup(inline_keyboard=keyboard)

    # Сохраняем состояние ожидания выбора
    stat_waiting_username[message.from_user.id] = True

    try:
        await message.reply("Выберите пользователя для фильтрации статистики:", reply_markup=reply_markup)
    except TelegramBadRequest as e:
        if "query is too old" in str(e):
            return
        else:
            raise


# Обработчик нажатий на кнопки инлайн-клавиатуры
@dp.callback_query(F.data.startswith("stat_"))
async def stat_callback_handler(callback: CallbackQuery):
    user_id = callback.from_user.id
    
    # Проверяем, что пользователь ожидает выбора
    if user_id not in stat_waiting_username:
        try:
            await callback.answer("Сессия устарела. Вызовите /stat снова.", show_alert=True)
        except TelegramBadRequest as e:
            if "query is too old" in str(e):
                return  # Игнорируем устаревшие запросы
            else:
                raise
        return

    # Извлекаем выбранный uid из callback_data
    callback_data = callback.data
    selected_uid = callback_data[5:]  # Убираем "stat_"

    # Обработка кнопки "Отмена"
    if selected_uid == "cancel":
        del stat_waiting_username[user_id]
        try:
            await callback.message.edit_text("Операция отменена.")
            await callback.answer()
        except TelegramBadRequest as e:
            if "query is too old" in str(e):
                return  # Игнорируем устаревшие запросы
            else:
                raise
        return

    # Удаляем состояние ожидания
    del stat_waiting_username[user_id]

    # Собираем данные из истории
    data = []
    for entry in history:
        expires_at_str = entry.get("expires_at")
        if not expires_at_str:
            continue
            
        try:
            # Парсим дату и извлекаем только дату
            expires_dt = datetime.fromisoformat(expires_at_str)
            expires_date = expires_dt.date()
        except ValueError:
            continue

        command = entry.get("command", "")
        
        for participant in entry.get("participants", []):
            uid = participant.get("uid")
            fullname = participant.get("fullname", "")
            username = participant.get("username", "")
            
            # Фильтруем по выбранному uid, если не выбрано "ВСЕ"
            if selected_uid != "ALL" and str(uid) != selected_uid:
                continue
                
            data.append({
                "uid": uid,
                "fullname": fullname,
                "username": username,
                "expires_at": expires_date,
                "command": command
            })

    if not data:
        try:
            await callback.message.edit_text("Нет данных для выбранного фильтра.")
            await callback.answer()
        except TelegramBadRequest as e:
            if "query is too old" in str(e):
                return  # Игнорируем устаревшие запросы
            else:
                raise
        return

    # Сортируем данные по expires_at, затем по command
    data.sort(key=lambda x: (x["expires_at"], x["command"]))

    # Создаем CSV файл в памяти
    output = io.StringIO()
    fieldnames = ["uid", "fullname", "username", "expires_at", "command"]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    
    for row in data:
        writer.writerow(row)

    # Подготавливаем файл для отправки
    csv_data = output.getvalue().encode('utf-8')
    output.close()

    # Определяем имя файла в зависимости от выбора
    if selected_uid == "ALL":
        filename = "poll_statistics_all.csv"
        display_name = "всех пользователей"
    else:
        # Находим данные выбранного пользователя для красивого имени файла
        user_info = None
        for entry in history:
            for participant in entry.get("participants", []):
                if str(participant.get("uid")) == selected_uid:
                    user_info = participant
                    break
            if user_info:
                break
        
        if user_info:
            username = user_info.get("username")
            fullname = user_info.get("fullname", "")
            if username:
                display_name = f"@{username}"
            else:
                display_name = fullname
            filename = f"poll_statistics_{display_name.replace(' ', '_')}.csv"
        else:
            display_name = f"uid_{selected_uid}"
            filename = f"poll_statistics_{selected_uid}.csv"
    
    # Редактируем сообщение с клавиатурой и отправляем файл
    try:
        await callback.message.edit_text(f"Статистика для: {display_name}")
        
        await callback.message.answer_document(
            types.BufferedInputFile(csv_data, filename=filename),
            caption=f"Статистика опросов - {display_name}"
        )
        
        await callback.answer()
    except TelegramBadRequest as e:
        if "query is too old" in str(e):
            return  # Игнорируем устаревшие запросы
        else:
            raise


@dp.message(Command(commands=["deactivate"]))
async def deactivate_cmd(message: Message):
    chat_id = message.chat.id
    res = await deactivate_poll(chat_id, reason=f"manual by {message.from_user.id}")
    try:
        if res:
            await message.reply("Опрос закрыт.")
        else:
            await message.reply("Активных опросов нет.")
    except TelegramBadRequest as e:
        if "query is too old" in str(e):
            return
        else:
            raise



async def autopoll_scheduler():
    logger.info("Autopoll scheduler started")
    while True:
        try:
            # Всегда работаем в локальном времени (UTC+3)
            now_local = datetime.now(LOCAL_TZ)
            logger.debug(f"[autopoll] Tick at {now_local.isoformat()} (weekday={now_local.strftime('%a').lower()[:3]})")

            # --- Проверка и авто-деактивация активных опросов ---
            for cid, info in list(active_poll.items()):
                expires_at = info.get("expires_at")

                if expires_at:
                    # Если expires_at хранится в UTC — переведём в локальное
                    if expires_at.tzinfo == timezone.utc:
                        expires_at = expires_at.astimezone(LOCAL_TZ)
                    elif expires_at.tzinfo is None:
                        expires_at = expires_at.replace(tzinfo=LOCAL_TZ)

                    key_deact = (cid,)
                    already_deact = last_autodeactivate.get(key_deact)

                    if now_local >= expires_at and already_deact != date.today():
                        logger.info(f"[autopoll] Deactivating poll {cid} due to expiration (now={now_local}, expires_at={expires_at})")
                        await deactivate_poll(cid, reason="expired by scheduler")
                        last_autodeactivate[key_deact] = date.today()

            # Если уже есть активный опрос — ждём и не создаём новый
            if active_poll:
                await asyncio.sleep(30)
                continue

            # --- Автоматическое создание новых опросов ---
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

                        # время создания — в локальном часовом поясе
                        sched_dt = datetime.combine(date.today(), create_time).replace(tzinfo=LOCAL_TZ)
                        key = (chat_id, cmd_name, day, sched.get("createmsg"))
                        already = last_autocreate.get(key)

                        logger.debug(
                            f"[autopoll] Check schedule: cmd={cmd_name}, day={day}, target={sched_dt.isoformat()}, "
                            f"now_local={now_local.isoformat()}"
                        )

                        # Проверяем окно запуска (±60 сек)
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

def build_help_text():
    lines = [
        "🤖 *Бот для управления опросами*\n",
        "*Основные команды:*"
    ]

    for chat_id_str, chat_conf in SETTINGS.get("chats", {}).items():
        lines.append(f"\n*Чат:* `{chat_id_str}`")
        topics = chat_conf.get("topics", {})
        topic = topics.get("root", {})
        commands = topic.get("commands", {})

        for cmd_name, cmd_conf in commands.items():
            question = cmd_conf.get("question", cmd_name)
            lines.append(f"/{cmd_name} - Создать опрос: \"{question}\"")

            # Автопрос
            if cmd_conf.get("autopoll", "false").lower() == "true":
                lines.append(f"   - Автопрос включён")
                aps = cmd_conf.get("autopollsettings", {})
                schedule_list = aps.get("schedule_autopoll", [])
                for sched in schedule_list:
                    day = sched.get("day", "").capitalize()
                    create_time = sched.get("createmsg")
                    deactivate_time = sched.get("deactivatemsg")
                    lines.append(f"     • {day}: создаётся в {create_time}, закрывается в {deactivate_time}")

            # Настройки ручного опроса
            mps = cmd_conf.get("manualpollsettings", {})
            pin = mps.get("pin", "false").lower() == "true"
            unpin = mps.get("unpin", "false").lower() == "true"
            lines.append(f"   - Pin: {pin}, Unpin: {unpin}")

    lines.append("\n*Участие в опросе:*")
    lines.append("- Используйте кнопки \"✅ Участвую\" и \"🔄 Пас\" под сообщением опроса")
    lines.append("- Нажмите \"✅ Участвую\" чтобы добавиться в список")
    lines.append("- Нажмите \"🔄 Пас\" чтобы удалиться из списка")
    lines.append("- Можно нажимать кнопки многократно - бот корректно обработает все действия")
    lines.append("\n*Закрытие опроса:*")
    lines.append("- /deactivate - закрыть активный опрос в этом чате")
    lines.append("\n*Статистика:*")
    lines.append("- /stat - получить статистику по опросам (только для админов)")
    lines.append("- /top5 - топ-5 самых активных участников")
    lines.append("\n*История:*")
    lines.append("- Бот хранит последние 100 опросов и восстанавливает активный при перезапуске")
    return "\n".join(lines)

@dp.message(Command(commands=["help"]))
async def help_cmd(message: types.Message):
    text = build_help_text()
    try:
        sent = await message.answer(text, parse_mode="Markdown")
        await asyncio.sleep(100)
        try:
            await sent.delete()
        except TelegramBadRequest as e:
            if "query is too old" in str(e):
                return
            else:
                raise
    except TelegramBadRequest as e:
        if "query is too old" in str(e):
            return
        else:
            raise


# Список команд, для которых есть отдельные хэндлеры
EXCLUDE_COMMANDS = {"help", "deactivate", "stat", "top5", "edit"}



@dp.message(Command(commands=["top5"]))
async def top5_cmd(message: Message):
    chat_id = message.chat.id
    
    # Проверяем, что команда вызвана в групповом чате
    if message.chat.type not in ["group", "supergroup"]:
        try:
            await message.answer("Эта команда доступна только в групповых чатах.")
        except TelegramBadRequest as e:
            if "query is too old" in str(e):
                return
            else:
                raise
        return
    
    # Проверяем права админа
    user_id = str(message.from_user.id)
    if user_id not in ADMIN_IDS:
        try:
            await message.answer("У вас нет прав для использования этой команды.")
        except TelegramBadRequest as e:
            if "query is too old" in str(e):
                return
            else:
                raise
        return
    
    # Вычисляем дату два месяца назад
    now = datetime.now(timezone.utc)
    two_months_ago = now - timedelta(days=60)
    
    # Собираем статистику тренировок
    training_days = {}  # {date: set(participant_uids)}
    user_stats = {}     # {uid: {"count": int, "latest_name": str}}
    
    for entry in history:
        try:
            # Пропускаем активные опросы
            if entry.get("active", False):
                continue
                
            # Получаем дату тренировки из expires_at
            expires_at_str = entry.get("expires_at")
            if not expires_at_str:
                continue
                
            # Парсим дату
            expires_dt = datetime.fromisoformat(expires_at_str)
            
            # Фильтруем по времени (последние 2 месяца)
            if expires_dt < two_months_ago:
                continue
                
            # Получаем участников
            participants = entry.get("participants", [])
            
            # Пропускаем тренировки с менее чем 4 участниками
            if len(participants) < 4:
                continue
                
            # Дата тренировки (без времени)
            training_date = expires_dt.date()
            
            # Добавляем участников в статистику по дате
            for participant in participants:
                uid = participant.get("uid")
                username = participant.get("username")
                fullname = participant.get("fullname", "")
                
                if not uid:
                    continue
                    
                # Обновляем информацию о пользователе
                if uid not in user_stats:
                    user_stats[uid] = {"count": 0, "latest_name": ""}
                
                # Обновляем имя (используем последнее доступное)
                display_name = f"@{username}" if username else fullname
                if display_name:
                    user_stats[uid]["latest_name"] = display_name
                
                # Увеличиваем счетчик тренировок
                if training_date not in training_days:
                    training_days[training_date] = set()
                
                # Если пользователь еще не учтен за эту дату
                if uid not in training_days[training_date]:
                    training_days[training_date].add(uid)
                    user_stats[uid]["count"] += 1
                    
        except Exception as e:
            logger.warning(f"Error processing history entry for top5: {e}")
            continue
    
    # Формируем топ-5 участников
    top_users = []
    for uid, stats in user_stats.items():
        if stats["count"] > 0 and stats["latest_name"]:
            top_users.append({
                "name": stats["latest_name"],
                "count": stats["count"]
            })
    
    # Сортируем по количеству тренировок (по убыванию)
    top_users.sort(key=lambda x: x["count"], reverse=True)
    top_5 = top_users[:5]
    
    # Формируем сообщение с результатами
    if not top_5:
        try:
            await message.answer("За последние 2 месяцев нет данных о тренировках с 4+ участниками.")
        except TelegramBadRequest as e:
            if "query is too old" in str(e):
                return
            else:
                raise
        return
    
    lines = ["🏆 ТОП-5 самых активных участников за последние 2 месяца:\n"]
    
    for i, user in enumerate(top_5, 1):
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "🔸"
        lines.append(f"{medal} {user['name']} - {user['count']} тренировок")
    
    lines.append(f"\nУсловия: учитываются только тренировки с 4+ участниками")
    lines.append(f"Период: последние 2 месяца")
    lines.append(f"Всего тренировок учтено: {len(training_days)}")
    
    result_text = "\n".join(lines)
    
    # Просто отправляем сообщение
    try:
        await message.answer(result_text)
    except TelegramBadRequest as e:
        if "query is too old" in str(e):
            return
        else:
            raise



# --- Универсальный хэндлер для ручных опросов --- #
@dp.message(F.text.startswith("/"))
async def universal_command_handler(message: types.Message):
    chat_id = message.chat.id
    text = message.text.strip()
    
    # Игнорируем + и -
    if text in {"+", "-"}:
        return

    # Берём имя команды без /
    cmd_name = text[1:].split()[0]  # /rapier@bot → rapier@bot
    
    # Убираем @username, если есть
    if "@" in cmd_name:
        cmd_name = cmd_name.split("@")[0].lower()
    else:
        cmd_name = cmd_name.lower()
    
    # Пропускаем команды с отдельными хэндлерами
    if cmd_name in EXCLUDE_COMMANDS:
        return
    
    # Получаем настройки команды
    cmd_settings = find_command_settings(chat_id, cmd_name)
    if not cmd_settings:
        # Исправляем получение username бота
        try:
            bot_info = await bot.get_me()
            logger.info("No settings for command %s@%s in chat %s", cmd_name, bot_info.username, chat_id)
        except TelegramBadRequest as e:
            if "query is too old" in str(e):
                return
            else:
                raise
        return

    # Создаём опрос вручную
    try:
        await create_poll(chat_id, cmd_name)
    except TelegramBadRequest as e:
        if "query is too old" in str(e):
            return
        else:
            raise


@dp.message(F.text)
async def handle_edit_link(message: Message):
    user_id = message.from_user.id
    logger.info(f"📨 Received text message from user {user_id} in chat {message.chat.type}: '{message.text}'")
    
    # Проверяем, что сообщение в личном чате И пользователь ожидает ссылку
    if message.chat.type != "private":
        logger.debug(f"Message not in private chat, ignoring. Chat type: {message.chat.type}")
        return
        
    if user_id not in edit_waiting_for_link or not edit_waiting_for_link[user_id]:
        logger.debug(f"User {user_id} is not waiting for link, ignoring message")
        return

    logger.info(f"✅ User {user_id} is waiting for link, processing...")
    
    # Сбрасываем состояние ожидания
    edit_waiting_for_link[user_id] = False
    logger.debug(f"Reset waiting state for user {user_id}")

    # Проверяем, что сообщение действительно похоже на ссылку
    link = message.text.strip()
    if not link.startswith(('http://', 'https://', 't.me/')):
        logger.warning(f"Message doesn't look like a link: {link}")
        try:
            await message.reply("Это не похоже на ссылку. Пожалуйста, пришлите ссылку на опрос в формате: https://t.me/c/...")
        except Exception as e:
            logger.error(f"Failed to send error message: {e}")
        return

    logger.info(f"🔗 Processing link: {link}")
    
    # Извлекаем ID из ссылки
    chat_id, message_id = extract_ids_from_link(link)
    logger.info(f"📋 Extracted IDs - chat_id: {chat_id}, message_id: {message_id}")
    
    if not chat_id or not message_id:
        logger.warning(f"❌ Failed to extract IDs from link: {link}")
        try:
            await message.reply("Не удалось извлечь данные из ссылки. Убедитесь, что ссылка правильная. Пример: https://t.me/c/1570728084/1/3110")
        except Exception as e:
            logger.error(f"Failed to send error message: {e}")
        return

    logger.info(f"🔍 Looking for poll in history: chat_id={chat_id}, message_id={message_id}")
    
    # Ищем опрос в истории
    poll_entry = find_poll_in_history(chat_id, message_id)
    if not poll_entry:
        logger.warning(f"❌ Poll not found in history for chat_id={chat_id}, message_id={message_id}")
        try:
            await message.reply("Опрос не найден в истории.")
        except Exception as e:
            logger.error(f"Failed to send 'not found' message: {e}")
        return

    logger.info(f"✅ Poll found: {poll_entry.get('command', 'Unknown')}")
    
    # Создаем сессию редактирования
    edit_sessions[user_id] = {
        "chat_id": chat_id,
        "message_id": message_id,
        "poll_entry": poll_entry,
        "last_action_time": datetime.now(timezone.utc),
        "private_message_id": None
    }
    logger.info(f"📝 Created edit session for user {user_id}")

    # Формируем текст с списком участников
    participants = _deserialize_participants(poll_entry.get("participants", []))
    question = poll_entry.get("command", "Опрос")
    
    text = build_edit_poll_text(question, participants)
    
    try:
        sent_message = await message.reply(text, reply_markup=build_edit_keyboard(), parse_mode="HTML")
        edit_sessions[user_id]["private_message_id"] = sent_message.message_id
        logger.info(f"📤 Sent edit interface to user {user_id}, message_id: {sent_message.message_id}")
        
        # Запускаем таймер сессии
        asyncio.create_task(edit_session_timer(user_id))
        logger.info(f"⏰ Started session timer for user {user_id}")
        
    except Exception as e:
        logger.error(f"❌ Failed to send edit interface to user {user_id}: {e}")


async def main():
    load_history()

    # Запуск фонового таска для живого таймера
    asyncio.create_task(active_poll_updater())

    # Запуск автопланировщика для автопросов
    asyncio.create_task(autopoll_scheduler())

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
