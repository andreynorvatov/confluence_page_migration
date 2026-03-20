"""
Скрипт для скачивания страницы из Confluence.
Сохраняет содержимое страницы в JSON для последующей загрузки в другой Confluence.
"""

import json
import sys
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from atlassian import Confluence

import os


def load_confluence_config() -> dict:
    """Загружает конфигурацию Confluence из .env файла."""
    env_path = Path(__file__).parent / ".env"
    load_dotenv(dotenv_path=env_path)
    
    return {
        "url": os.getenv("CONFLUENCE_URL"),
        "cookie": os.getenv("CONFLUENCE_COOKIE"),
    }


def download_page(confluence: Confluence, page_id: str, output_dir: str = "downloads") -> dict:
    """Скачивает страницу из Confluence."""
    page = confluence.get_page_by_id(page_id, expand="body.storage,version,space,ancestors")
    attachments = confluence.get(f"rest/api/content/{page_id}/child/attachment")
    
    page_data = {
        "id": page.get("id"),
        "title": page.get("title"),
        "space": {
            "key": page.get("space", {}).get("key"),
            "name": page.get("space", {}).get("name"),
        },
        "version": page.get("version", {}).get("number"),
        "body": {
            "storage": page.get("body", {}).get("storage", {}).get("value"),
            "representation": "storage",
        },
        "ancestors": [{"id": a.get("id"), "title": a.get("title")} for a in page.get("ancestors", [])],
        "attachments": attachments.get("results", []) if attachments else [],
        "downloaded_at": datetime.now().isoformat(),
    }
    
    os.makedirs(output_dir, exist_ok=True)
    output_file = Path(output_dir) / f"page_{page_id}.json"
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(page_data, f, ensure_ascii=False, indent=2)
    
    print(f"Страница '{page_data['title']}' (ID: {page_id}) успешно скачана")
    print(f"Файл: {output_file}")
    print(f"Вложений: {len(page_data['attachments'])}")
    
    return page_data


def main():
    config = load_confluence_config()
    
    if not all([config["url"], config["cookie"]]):
        print("Ошибка: укажите CONFLUENCE_URL и CONFLUENCE_COOKIE в .env")
        return
    
    confluence = Confluence(url=config["url"], cookies={"seraph.confluence": config["cookie"]}, cloud=False)
    
    # Получение ID страницы
    page_id = sys.argv[1] if len(sys.argv) > 1 else input("Введите ID страницы: ").strip()
    
    if not page_id:
        print("ID страницы не указан")
        return
    
    try:
        download_page(confluence, page_id)
    except Exception as e:
        print(f"Ошибка: {e}")


if __name__ == "__main__":
    main()
