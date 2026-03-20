"""
Скрипт для скачивания страницы из Confluence.
Сохраняет содержимое страницы и вложения для последующей загрузки в другой Confluence.
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


def download_attachment(confluence: Confluence, attachment: dict, attachments_dir: Path) -> str:
    """Скачивает вложение и возвращает относительный путь к файлу."""
    filename = attachment.get("title")
    download_url = attachment.get("_links", {}).get("download")
    
    if not download_url:
        print(f"  ⚠ Нет URL для {filename}")
        return None
    
    # Полный URL
    if not download_url.startswith("http"):
        download_url = confluence.url + download_url
    
    # Скачиваем файл
    response = confluence._session.get(download_url, stream=True)
    if response.status_code != 200:
        print(f"  ⚠ Ошибка загрузки {filename}: {response.status_code}")
        return None
    
    # Сохраняем
    file_path = attachments_dir / filename
    with open(file_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    print(f"  ✓ {filename}")
    return filename


def download_page(confluence: Confluence, page_id: str, output_dir: str = "downloads") -> dict:
    """Скачивает страницу из Confluence со всеми вложениями."""
    page = confluence.get_page_by_id(page_id, expand="body.storage,version,space,ancestors")
    attachments = confluence.get(f"rest/api/content/{page_id}/child/attachment")
    
    # Создаём директорию для страницы
    page_dir = Path(output_dir) / f"page_{page_id}"
    page_dir.mkdir(parents=True, exist_ok=True)
    
    # Создаём директорию для вложений
    attachments_dir = page_dir / "files"
    attachments_dir.mkdir(parents=True, exist_ok=True)
    
    # Скачиваем вложения
    downloaded_attachments = []
    attachment_results = attachments.get("results", []) if attachments else []
    
    if attachment_results:
        print(f"Загрузка {len(attachment_results)} вложений...")
        for attachment in attachment_results:
            saved_file = download_attachment(confluence, attachment, attachments_dir)
            if saved_file:
                downloaded_attachments.append({
                    "title": attachment.get("title"),
                    "filename": saved_file,
                    "mediaType": attachment.get("metadata", {}).get("mediaType"),
                })
    
    page_data = {
        "id": page.get("id"),
        "title": page.get("title"),
        "space": {
            "key": page.get("space", {}).get("key"),
            "name": page.get("space", {}).get("name"),
        },
        "version": page.get("version", {}).get("number"),
        "last_modified": page.get("version", {}).get("when"),
        "body": {
            "storage": page.get("body", {}).get("storage", {}).get("value"),
            "representation": "storage",
        },
        "ancestors": [{"id": a.get("id"), "title": a.get("title")} for a in page.get("ancestors", [])],
        "attachments": downloaded_attachments,
        "attachments_dir": "files",
        "downloaded_at": datetime.now().isoformat(),
    }
    
    # Сохраняем JSON
    output_file = page_dir / f"page_{page_id}.json"
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(page_data, f, ensure_ascii=False, indent=2)
    
    print(f"\nСтраница '{page_data['title']}' (ID: {page_id}) успешно скачана")
    print(f"Папка: {page_dir}")
    print(f"Вложений скачано: {len(downloaded_attachments)}")
    
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
