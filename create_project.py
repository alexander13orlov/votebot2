import os
from pathlib import Path

# Базовая директория = где лежит сам скрипт
BASE_DIR = Path(__file__).parent.resolve()

# Структура проекта
files_content = {
    ".gitignore": """# Python
__pycache__/
*.pyc
venv/
.env
""",
    "requirements.txt": "",
    "README.md": "# My Telegram Bot\n\nТелеграм-бот на Python.",
    ".env.example": "BOT_TOKEN=your_token_here\nPASSWORD=ultimatum2025",
    "bot/__init__.py": "",
    "bot/config.py": "",
    "bot/utils.py": "",
    "bot/handlers.py": "",
    "bot/main.py": ""  # пустой файл
}

def create_project():
    for filepath, content in files_content.items():
        path = BASE_DIR / filepath
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
    print("✅ Структура проекта создана в:", BASE_DIR)

if __name__ == "__main__":
    create_project()
