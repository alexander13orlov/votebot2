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

from .config import BOT_TOKEN, ADMIN_IDS

import csv
import io
from datetime import datetime

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.filters import Command


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
    Формирует текст опроса с количеством участников и оставшимся временем до закрытия.
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

    lines = [f"[{total}] {question}", f"Осталось {remaining_str}.", ""]

    if participants:
        for idx, p in enumerate(participants, start=1):
            uid, username, fullname = p
            if username:
                lines.append(f"{idx}. @{username} {fullname}")
            else:
                lines.append(f"{idx}. {fullname}")
    else:
        lines.append("Пока нет участников.")

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
                        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text)
                        info["last_text"] = text
                    except TelegramBadRequest as e:
                        if "message is not modified" in str(e):
                            pass  # текст совпадает, игнорируем
                        else:
                            logger.warning(
                                "Failed to update poll message with timer chat=%s message=%s: %s",
                                chat_id, message_id, e
                            )
        except Exception as e:
            logger.exception("Error in active_poll_updater: %s", e)

        await asyncio.sleep(30)

async def edit_poll_message(chat_id, message_id, question, participants, expires_at):
    text = build_poll_text_with_timer(question, participants, expires_at)
    last_text = active_poll[chat_id].get("last_text")
    if text == last_text:
        return  # текст не изменился, не обновляем
    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text)
        active_poll[chat_id]["last_text"] = text
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            pass
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
    sent = await bot.send_message(chat_id, text)
    message_id = sent.message_id

    if pin:
        try:
            # await bot.pin_chat_message(chat_id, message_id)
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
    lines = [f"[{total}] {question} (ЗАКРЫТ)", ""]
    if participants:
        for idx, p in enumerate(participants, start=1):
            uid, username, fullname = p
            if username:
                lines.append(f"{idx}. @{username} — {fullname}")
            else:
                lines.append(f"{idx}. {fullname}")
    else:
        lines.append("Никто не записался.")

    new_text = "\n".join(lines)
    last_text = info.get("last_text")
    if new_text != last_text:
        try:
            await bot.edit_message_text(chat_id=str(chat_id), message_id=message_id, text=new_text)
            info["last_text"] = new_text
            edit_ok = True
        except TelegramBadRequest as e:
            if "message is not modified" in str(e):
                edit_ok = True  # текст совпадает, считаем, что редактирование прошло успешно
            else:
                edit_ok = False
                logger.warning(
                    "Failed to edit message when closing poll chat=%s message=%s: %s", chat_id, message_id, e
                )
    else:
        edit_ok = True  # текст не изменился, обновлять не нужно — считаем успехом

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


# --- Handlers --- #

# Глобальная переменная для хранения состояния
stat_waiting_username = {}

# Добавьте обработчик команды /stat
@dp.message(Command(commands=["stat"]))
async def stat_cmd(message: Message):
    # Проверяем, что команда вызвана в личном чате
    if message.chat.type != "private":
        await message.reply("Эта команда доступна только в личном чате с ботом.")
        return

    # Проверяем права админа
    user_id = str(message.from_user.id)
    if user_id not in ADMIN_IDS:
        await message.reply("У вас нет прав для использования этой команды.")
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
        await message.reply("В истории опросов нет участников.")
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

    await message.reply("Выберите пользователя для фильтрации статистики:", reply_markup=reply_markup)

# Обработчик нажатий на кнопки инлайн-клавиатуры
@dp.callback_query(F.data.startswith("stat_"))
async def stat_callback_handler(callback: CallbackQuery):
    user_id = callback.from_user.id
    
    # Проверяем, что пользователь ожидает выбора
    if user_id not in stat_waiting_username:
        await callback.answer("Сессия устарела. Вызовите /stat снова.", show_alert=True)
        return

    # Извлекаем выбранный uid из callback_data
    callback_data = callback.data
    selected_uid = callback_data[5:]  # Убираем "stat_"

    # Обработка кнопки "Отмена"
    if selected_uid == "cancel":
        del stat_waiting_username[user_id]
        await callback.message.edit_text("Операция отменена.")
        await callback.answer()
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
        await callback.message.edit_text("Нет данных для выбранного фильтра.")
        await callback.answer()
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
    await callback.message.edit_text(f"Статистика для: {display_name}")
    
    await callback.message.answer_document(
        types.BufferedInputFile(csv_data, filename=filename),
        caption=f"Статистика опросов - {display_name}"
    )
    
    await callback.answer()










# ------------------------------------------

@dp.message(Command(commands=["deactivate"]))
async def deactivate_cmd(message: Message):
    chat_id = message.chat.id
    res = await deactivate_poll(chat_id, reason=f"manual by {message.from_user.id}")
    if res:
        await message.reply("Опрос закрыт.")
    else:
        await message.reply("Активных опросов нет.")


@dp.message(F.text.in_({"+", "-"}))
async def plus_minus_handler(message: Message):
    chat_id = message.chat.id
    text = message.text.strip()

    info = active_poll.get(chat_id)
    if not info:
        # Нет активного опроса — игнорируем
        return

    # Если опрос истёк — игнорируем (деактивация делается scheduler-ом)
    expires_at = info.get("expires_at")
    if expires_at and datetime.now(timezone.utc) >= expires_at:
        return

    uid = message.from_user.id
    username = message.from_user.username
    fullname = message.from_user.full_name
    participants = info.get("participants", [])

    cmd_settings = find_command_settings(chat_id, info["command"])
    delete_pm = False
    if cmd_settings:
        # берём настройку удаления только если есть настройки
        if "autopollsettings" in cmd_settings and info.get("expires_at"):
            delete_pm = cmd_settings.get("autopollsettings", {}).get("deleteplusminus", "false").lower() == "true"
        elif "manualpollsettings" in cmd_settings:
            delete_pm = cmd_settings.get("manualpollsettings", {}).get("deleteplusminus", "false").lower() == "true"

    changed = False
    if text == "+":
        if not any(p[0] == uid for p in participants):
            participants.append((uid, username, fullname))
            changed = True
    elif text == "-":
        if any(p[0] == uid for p in participants):
            participants[:] = [p for p in participants if p[0] != uid]
            changed = True

    if changed:
        question = cmd_settings.get("question", info["command"]) if cmd_settings else info["command"]
        await edit_poll_message(chat_id, info["message_id"], question, participants, info.get("expires_at"))
        update_history_entry(chat_id, info["message_id"], participants=_serialize_participants(participants))

        if delete_pm:
            await asyncio.sleep(4)
            try:
                await message.delete()
            except Exception:
                pass

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
    lines.append("- Чтобы записаться, отправьте `+`")
    lines.append("- Чтобы снять участие, отправьте `-`")
    lines.append("\n*Закрытие опроса:*")
    lines.append("- /deactivate - закрыть активный опрос в этом чате")
    lines.append("\n*История:*")
    lines.append("- Бот хранит последние 100 опросов и восстанавливает активный при перезапуске")
    return "\n".join(lines)

@dp.message(Command(commands=["help"]))
async def help_cmd(message: types.Message):
    text = build_help_text()
    sent = await message.answer(text, parse_mode="Markdown")
    await asyncio.sleep(100)
    try:
        await sent.delete()
    except Exception:
        pass

# --- Универсальный хэндлер для ручных опросов --- #
# Список команд, для которых есть отдельные хэндлеры
EXCLUDE_COMMANDS = {"help", "deactivate"}

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
        logger.info("No settings for command %s@%s in chat %s", cmd_name, bot.username, chat_id)
        return

    # Создаём опрос вручную
    await create_poll(chat_id, cmd_name)



async def main():
    load_history()

    # Запуск фонового таска для живого таймера
    asyncio.create_task(active_poll_updater())

    # Запуск автопланировщика для автопросов
    asyncio.create_task(autopoll_scheduler())

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
