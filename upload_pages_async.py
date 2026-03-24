"""
Асинхронный скрипт для загрузки страниц в Confluence.
Обрабатывает все страницы и папки из downloads, обновляет БД с last_modified из JSON.
"""

import json
import asyncio
import aiohttp
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from atlassian import Confluence

import os

from db_async import AsyncDatabase, ConfluencePage


# ==================== НАСТРОЙКИ ====================

# Ключ целевого пространства
TARGET_SPACE = "Lanit"

# Папка с загруженными страницами
DOWNLOADS_DIR = "downloads"

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


async def upload_page_async(
    session: aiohttp.ClientSession,
    confluence: Confluence,
    db: AsyncDatabase,
    file_path: str,
    page_data: dict,
    target_space: str,
    parent_id: str = None
) -> dict:
    """
    Асинхронно загружает страницу в Confluence с вложениями.
    
    Если страница с таким заголовком уже существует в пространстве,
    её контент будет обновлён.
    """
    title = page_data["title"]
    page_id_from_json = page_data.get("id")

    print(f"\n{'='*60}")
    print(f"Загрузка: {title} (ID: {page_id_from_json})")
    print(f"{'='*60}")

    try:
        # Проверяем существование страницы
        existing_page = confluence.get_page_by_title(space=target_space, title=title)

        if existing_page:
            # Обновляем существующую страницу
            page_id = existing_page["id"]

            # Определяем родительский ID
            update_parent_id = parent_id
            if not update_parent_id:
                ancestors = existing_page.get("ancestors", [])
                if ancestors:
                    update_parent_id = ancestors[-1].get("id") if isinstance(ancestors[-1], dict) else None

            confluence.update_page(
                page_id=page_id,
                title=title,
                body=page_data["body"]["storage"],
                representation="storage",
                parent_id=update_parent_id,
            )
            print(f"Страница '{title}' обновлена (ID: {page_id})")
            new_page = existing_page
        else:
            # Создаём новую страницу
            new_page = confluence.create_page(
                space=target_space,
                title=title,
                body=page_data["body"]["storage"],
                parent_id=parent_id,
                representation="storage",
            )
            print(f"Страница '{title}' создана (ID: {new_page.get('id')})")

        page_id = new_page.get("id")

        # Загружаем вложения
        attachments = page_data.get("attachments", [])
        attachments_dir = os.path.join(os.path.dirname(file_path), page_data.get("attachments_dir", "files"))

        if attachments and os.path.exists(attachments_dir):
            print(f"Загрузка {len(attachments)} вложений...")
            for attachment in attachments:
                filename = attachment.get("filename") or attachment.get("title")
                src_file = os.path.join(attachments_dir, filename)

                if not os.path.exists(src_file):
                    print(f"  ⚠ Файл не найден: {filename}")
                    continue

                try:
                    confluence.attach_file(
                        filename=src_file,
                        name=filename,
                        content_type=attachment.get("mediaType", "application/octet-stream"),
                        page_id=page_id,
                    )
                    print(f"  ✓ {filename}")
                except Exception as e:
                    print(f"  ✗ {filename}: {e}")

        # Получаем last_modified из JSON для обновления БД
        last_modified = page_data.get("last_modified")

        # Обновляем БД
        db_page = ConfluencePage(
            page_id=page_id_from_json,
            page_title=title,
            last_edited_date=last_modified,
            last_sync_date=datetime.now().isoformat(),
            needs_update=False,
            update_attempts=0,
            last_update_error=None,
            space_key=target_space,
            page_url=f"{confluence.url}/pages/{page_id}",
            is_deleted=False,
        )

        await db.upsert_page(db_page)
        print(f"✓ БД обновлена: last_edited_date={last_modified}")

        return {"success": True, "page_id": page_id, "title": title}

    except Exception as e:
        print(f"✗ Ошибка при загрузке страницы {title}: {e}")

        # Обновляем БД с ошибкой
        if page_id_from_json:
            db_page = ConfluencePage(
                page_id=page_id_from_json,
                page_title=title,
                last_edited_date=page_data.get("last_modified"),
                last_sync_date=datetime.now().isoformat(),
                needs_update=True,
                update_attempts=1,
                last_update_error=str(e),
                space_key=target_space,
                page_url=None,
                is_deleted=False,
            )
            await db.upsert_page(db_page)

        return {"success": False, "error": str(e), "title": title}


async def process_page_file(
    session: aiohttp.ClientSession,
    confluence: Confluence,
    db: AsyncDatabase,
    json_file: Path,
    target_space: str,
    parent_id: str = None
) -> dict:
    """Обрабатывает один JSON-файл страницы."""
    print(f"\n{'='*60}")
    print(f"Обработка файла: {json_file}")
    print(f"{'='*60}")

    try:
        # Загружаем данные
        with open(json_file, "r", encoding="utf-8") as f:
            page_data = json.load(f)

        result = await upload_page_async(
            session,
            confluence,
            db,
            str(json_file),
            page_data,
            target_space,
            parent_id,
        )

        return result

    except Exception as e:
        print(f"✗ Ошибка при чтении файла {json_file}: {e}")
        return {"success": False, "error": str(e), "file": str(json_file)}


def find_all_page_json_files(downloads_dir: Path) -> list[Path]:
    """Находит все JSON-файлы страниц в папке downloads и подпапках."""
    json_files = []

    if not downloads_dir.exists():
        print(f"Папка {downloads_dir} не найдена")
        return json_files

    # Ищем все page_*.json файлы в подпапках
    for pattern in ["page_*/page_*.json", "page_*.json"]:
        json_files.extend(downloads_dir.glob(pattern))

    # Убираем дубликаты и сортируем
    json_files = sorted(set(json_files))

    return json_files


async def main():
    """Основная функция."""
    # Загрузка конфигурации
    config = load_confluence_config()

    if not config["target_url"]:
        print("Ошибка: не указан TARGET_CONFLUENCE_URL в .env")
        return

    if not config.get("target_username") or not config.get("target_token"):
        print("Ошибка: укажите TARGET_CONFLUENCE_USERNAME и TARGET_CONFLUENCE_TOKEN в .env")
        return

    # Инициализация БД
    db = AsyncDatabase()
    await db._init_db()

    # Инициализация целевого Confluence
    confluence = Confluence(
        url=config["target_url"],
        username=config["target_username"],
        password=config["target_token"],
        cloud=True,
    )

    # Находим все JSON-файлы страниц
    downloads_path = Path(__file__).parent / DOWNLOADS_DIR
    json_files = find_all_page_json_files(downloads_path)

    if not json_files:
        print(f"Нет JSON-файлов страниц в папке {DOWNLOADS_DIR}")
        return

    print(f"Найдено страниц для загрузки: {len(json_files)}")

    # Создаем HTTP-сессию для асинхронных операций
    async with aiohttp.ClientSession() as session:
        # Обрабатываем все страницы асинхронно
        tasks = [
            process_page_file(session, confluence, db, json_file, TARGET_SPACE, parent_id=None)
            for json_file in json_files
        ]
        results = await asyncio.gather(*tasks)

    # Вывод результатов
    success_count = sum(1 for r in results if r.get("success"))
    error_count = len(results) - success_count

    print("\n" + "=" * 60)
    print("РЕЗУЛЬТАТЫ")
    print("=" * 60)
    print(f"Всего обработано: {len(results)}")
    print(f"Успешно: {success_count}")
    print(f"С ошибками: {error_count}")

    if error_count > 0:
        print("\nСтраницы с ошибками:")
        for r in results:
            if not r.get("success"):
                error_info = r.get("error", "Неизвестная ошибка")
                title_or_file = r.get("title") or r.get("file")
                print(f"  - {title_or_file}: {error_info}")


if __name__ == "__main__":
    asyncio.run(main())
