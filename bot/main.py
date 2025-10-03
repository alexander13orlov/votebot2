import json
import logging
import asyncio
from pathlib import Path
from telegram import Update, Poll
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    PollAnswerHandler,
    PollHandler,
)
from .config import BOT_TOKEN

# Пути
BASE_DIR = Path(__file__).resolve().parent
SETTINGS_FILE = BASE_DIR / "settings.json"
POLLS_STATE_FILE = BASE_DIR / "polls_state.json"

# Логирование
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Глобальные переменные
settings = {}
polls_data = {}  # poll_id -> {chat_id, message_id, answers, options, voters}


def load_settings():
    global settings
    with open(SETTINGS_FILE, "r", encoding="utf-8-sig") as f:
        settings = json.load(f)
    logger.info("Настройки загружены")


async def save_polls_state():
    """Сохраняем состояние опросов в файл"""
    try:
        with open(POLLS_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(polls_data, f, ensure_ascii=False, indent=2)
        logger.info("Polls state saved.")
    except Exception as e:
        logger.error(f"Ошибка сохранения состояния опросов: {e}")


async def send_current_results(context: ContextTypes.DEFAULT_TYPE, poll_id: str):
    """Формируем и обновляем сообщение с текущими результатами"""
    if poll_id not in polls_data:
        return

    pdata = polls_data[poll_id]
    chat_id = pdata["chat_id"]
    msg_id = pdata["results_message_id"]
    options = pdata["options"]
    answers = pdata["answers"]

    total_voters = sum(len(voters) for voters in answers.values())

    lines = []
    for idx, option in enumerate(options):
        voters = answers.get(idx, [])
        lines.append(f"[{len(voters)}/{total_voters}] {option}")
        if voters:
            lines.extend(voters)

    text = "\n".join(lines) if lines else "Нет голосов."

    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=msg_id,
            text=text
        )
    except Exception as e:
        logger.warning(f"Не удалось обновить сообщение с результатами: {e}")


async def handle_poll_command(update: Update, context: ContextTypes.DEFAULT_TYPE, cmd: str, conf: dict):
    """Обработка команды, запускающей опрос"""
    logger.info(f"Запущена команда /{cmd} пользователем @{update.effective_user.username}")

    poll_settings = conf.get("settings", {})
    question = poll_settings.get("question", "Вопрос?")
    options = poll_settings.get("options", ["Да", "Нет"])
    is_anonymous = poll_settings.get("is_anonymous", False)
    allows_multiple_answers = poll_settings.get("allows_multiple_answers", False)

    # Отправляем опрос без цитирования
    poll_message = await context.bot.send_poll(
        chat_id=update.effective_chat.id,
        question=question,
        options=options,
        is_anonymous=is_anonymous,
        allows_multiple_answers=allows_multiple_answers
    )

    poll_id = poll_message.poll.id
    polls_data[poll_id] = {
        "chat_id": poll_message.chat_id,
        "message_id": poll_message.message_id,
        "results_message_id": None,
        "options": options,
        "answers": {},
    }

    # Если нужно сразу показывать результаты
    if conf.get("currentresult", False):
        # Отправляем сообщение без цитирования
        result_msg = await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Результаты будут обновляться здесь..."
        )
        polls_data[poll_id]["results_message_id"] = result_msg.message_id

        # Первое обновление
        await send_current_results(context, poll_id)


async def poll_answer_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обновление данных при голосовании"""
    answer = update.poll_answer
    poll_id = answer.poll_id
    user = f"@{answer.user.username}" if answer.user.username else answer.user.first_name

    if poll_id not in polls_data:
        return

    pdata = polls_data[poll_id]

    # Удаляем юзера из всех вариантов
    for voters in pdata["answers"].values():
        if user in voters:
            voters.remove(user)

    # Добавляем голос
    for option_id in answer.option_ids:
        pdata["answers"].setdefault(option_id, []).append(user)

    # Обновляем сообщение с результатами
    if pdata.get("results_message_id"):
        await send_current_results(context, poll_id)


async def poll_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка закрытия опроса"""
    poll = update.poll
    if not poll.is_closed:
        return
    if poll.id in polls_data:
        logger.info(f"Опрос {poll.id} закрыт")
        polls_data.pop(poll.id)


async def get_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для получения настроек"""
    user_id = str(update.effective_user.id)
    chat_id = str(update.effective_chat.id)

    admins = settings["chats"].get(chat_id, {}).get("admins", [])
    if user_id not in admins:
        await context.bot.send_message(chat_id=chat_id, text="Только админы могут получить файл настроек.")
        return

    await context.bot.send_document(chat_id=chat_id, document=SETTINGS_FILE)


def register_dynamic_commands(app: Application):
    """Регистрируем команды из settings.json"""
    for chat_id, chat_conf in settings.get("chats", {}).items():
        for topic, tconf in chat_conf.get("topics", {}).items():
            for cmd, conf in tconf.get("commands", {}).items():
                app.add_handler(CommandHandler(cmd, lambda u, c, cm=cmd, cf=conf: handle_poll_command(u, c, cm, cf)))
                logger.info(f"Зарегистрирована команда /{cmd}")


def main():
    load_settings()
    app = Application.builder().token(BOT_TOKEN).build()

    # Базовые команды
    app.add_handler(CommandHandler("getsettings", get_settings))

    # Обработчики опросов
    app.add_handler(PollAnswerHandler(poll_answer_handler))
    app.add_handler(PollHandler(poll_handler))

    # Динамические команды
    register_dynamic_commands(app)

    # Периодическое сохранение
    app.job_queue.run_repeating(lambda ctx: asyncio.create_task(save_polls_state()), interval=30, first=30)

    logger.info("Бот запущен")
    app.run_polling()


if __name__ == "__main__":
    main()
