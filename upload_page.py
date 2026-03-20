"""
Скрипт для загрузки страницы в Confluence.
Использует данные из JSON-файла, скачанного через download_page.py.
"""

import json
from pathlib import Path

from dotenv import load_dotenv
from atlassian import Confluence

import os


# ==================== НАСТРОЙКИ ====================

# Путь к файлу с данными страницы
PAGE_FILE = "downloads/page_286539561/page_286539561.json"

# Ключ целевого пространства
TARGET_SPACE = "Lanit"

# ID родительской страницы (None или ID)
PARENT_ID = None

# ================================================


def load_confluence_config() -> dict:
    """Загружает конфигурацию из .env файла."""
    env_path = Path(__file__).parent / ".env"
    load_dotenv(dotenv_path=env_path)

    return {
        "target_url": os.getenv("TARGET_CONFLUENCE_URL"),
        "target_username": os.getenv("TARGET_CONFLUENCE_USERNAME"),
        "target_token": os.getenv("TARGET_CONFLUENCE_TOKEN"),
    }


def upload_page(confluence: Confluence, file_path: str, page_data: dict, target_space: str, parent_id: str = None) -> dict:
    """Загружает страницу в Confluence с вложениями."""

    # Создаём страницу
    new_page = confluence.create_page(
        space=target_space,
        title=page_data["title"],
        body=page_data["body"]["storage"],
        parent_id=parent_id,
        representation="storage",
    )

    new_page_id = new_page.get("id")
    print(f"Страница '{page_data['title']}' создана (ID: {new_page_id})")

    # Загружаем вложения
    attachments = page_data.get("attachments", [])
    attachments_dir = os.path.join(os.path.dirname(file_path), page_data.get("attachments_dir", "files"))

    if attachments and os.path.exists(attachments_dir):
        print(f"Загрузка {len(attachments)} вложений...")
        for attachment in attachments:
            filename = attachment.get("filename")
            src_file = os.path.join(attachments_dir, filename)

            if not os.path.exists(src_file):
                print(f"  ⚠ Файл не найден: {filename}")
                continue

            try:
                confluence.attach_file(
                    filename=src_file,
                    name=filename,
                    content_type=attachment.get("mediaType", "application/octet-stream"),
                    page_id=new_page_id,
                )
                print(f"  ✓ {filename}")
            except Exception as e:
                print(f"  ✗ {filename}: {e}")

    return new_page


def main():
    config = load_confluence_config()

    if not config["target_url"]:
        print("Ошибка: не указан TARGET_CONFLUENCE_URL в .env")
        return

    if not config.get("target_username") or not config.get("target_token"):
        print("Ошибка: укажите TARGET_CONFLUENCE_USERNAME и TARGET_CONFLUENCE_TOKEN в .env")
        return

    # Инициализация целевого Confluence
    confluence = Confluence(
        url=config["target_url"],
        username=config["target_username"],
        password=config["target_token"],
        cloud=True,
    )

    # Путь к файлу
    file_path = PAGE_FILE
    if not os.path.exists(file_path):
        print(f"Файл не найден: {file_path}")
        return

    # Загружаем данные
    with open(file_path, "r", encoding="utf-8") as f:
        page_data = json.load(f)

    try:
        upload_page(confluence, file_path, page_data, TARGET_SPACE, PARENT_ID)
    except Exception as e:
        print(f"Ошибка: {e}")


if __name__ == "__main__":
    main()
