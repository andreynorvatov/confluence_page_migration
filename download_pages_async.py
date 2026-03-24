"""
Асинхронный скрипт для скачивания страниц из Confluence, требующих обновления.
Получает список страниц с needs_update==1 из БД, скачивает их локально и обновляет БД.
"""

import json
import asyncio
import aiohttp
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from atlassian import Confluence
import random

import os
import sys

from db_async import AsyncDatabase, ConfluencePage


async def retry_with_backoff(
    func,
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 10.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retryable_exceptions: tuple = (aiohttp.ClientError, asyncio.TimeoutError)
) -> any:
    """
    Выполняет функцию с ретраями и экспоненциальной задержкой.
    
    Args:
        func: Асинхронная функция для выполнения
        max_retries: Максимальное количество попыток
        base_delay: Базовая задержка в секундах
        max_delay: Максимальная задержка в секундах
        exponential_base: База экспоненты для задержки
        jitter: Добавлять случайную задержку для предотвращения thundering herd
        retryable_exceptions: Кортеж исключений, при которых делать retry
    
    Returns:
        Результат выполнения функции
    """
    last_exception = None
    
    for attempt in range(max_retries + 1):
        try:
            return await func()
        except retryable_exceptions as e:
            last_exception = e
            
            if attempt < max_retries:
                # Рассчитываем задержку
                delay = min(base_delay * (exponential_base ** attempt), max_delay)
                if jitter:
                    delay = delay * (0.5 + random.random())
                
                print(f"  ⚠ Ошибка (попытка {attempt + 1}/{max_retries + 1}): {e}")
                print(f"  ⟳ Повтор через {delay:.2f}с...")
                await asyncio.sleep(delay)
            else:
                print(f"  ✗ Превышено количество попыток ({max_retries + 1})")
    
    raise last_exception


def load_confluence_config() -> dict:
    """Загружает конфигурацию Confluence из .env файла."""
    env_path = Path(__file__).parent / ".env"
    load_dotenv(dotenv_path=env_path)

    return {
        "url": os.getenv("CONFLUENCE_URL"),
        "cookie": os.getenv("CONFLUENCE_COOKIE"),
    }


async def download_attachment(
    session: aiohttp.ClientSession,
    confluence_url: str,
    attachment: dict,
    attachments_dir: Path
) -> str:
    """Скачивает вложение и возвращает относительный путь к файлу."""
    filename = attachment.get("title")
    download_url = attachment.get("_links", {}).get("download")

    if not download_url:
        print(f"  ⚠ Нет URL для {filename}")
        return None

    # Полный URL
    if not download_url.startswith("http"):
        download_url = confluence_url + download_url

    async def _download():
        async with session.get(download_url) as response:
            if response.status != 200:
                raise aiohttp.ClientResponseError(
                    response.request_info,
                    response.history,
                    status=response.status,
                    message=f"HTTP {response.status}"
                )
            
            file_path = attachments_dir / filename
            with open(file_path, "wb") as f:
                f.write(await response.read())
        
        return filename

    try:
        result = await retry_with_backoff(
            _download,
            max_retries=3,
            retryable_exceptions=(aiohttp.ClientError, asyncio.TimeoutError)
        )
        print(f"  ✓ {filename}")
        return result
    except Exception as e:
        print(f"  ✗ Ошибка загрузки {filename} после всех попыток: {e}")
        return None


async def download_page(
    session: aiohttp.ClientSession,
    confluence: Confluence,
    page_id: str,
    output_dir: str = "downloads"
) -> dict:
    """Скачивает страницу из Confluence со всеми вложениями."""
    
    async def _get_page():
        return confluence.get_page_by_id(page_id, expand="body.storage,version,space,ancestors")
    
    async def _get_attachments():
        return confluence.get(f"rest/api/content/{page_id}/child/attachment")
    
    # Получаем страницу с ретраями
    page = await retry_with_backoff(
        _get_page,
        max_retries=3,
        retryable_exceptions=(aiohttp.ClientError, asyncio.TimeoutError, Exception)
    )
    
    # Получаем вложения с ретраями
    attachments = await retry_with_backoff(
        _get_attachments,
        max_retries=3,
        retryable_exceptions=(aiohttp.ClientError, asyncio.TimeoutError, Exception)
    )

    # Создаём директорию для страницы
    page_dir = Path(output_dir) / f"page_{page_id}"
    page_dir.mkdir(parents=True, exist_ok=True)

    # Создаём директорию для вложений
    attachments_dir = page_dir / "files"
    attachments_dir.mkdir(parents=True, exist_ok=True)

    # Скачиваем вложения асинхронно
    downloaded_attachments = []
    attachment_results = attachments.get("results", []) if attachments else []

    if attachment_results:
        print(f"Загрузка {len(attachment_results)} вложений...")
        tasks = [
            download_attachment(session, confluence.url, attachment, attachments_dir)
            for attachment in attachment_results
        ]
        saved_files = await asyncio.gather(*tasks)

        for attachment, saved_file in zip(attachment_results, saved_files):
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


async def process_page(
    session: aiohttp.ClientSession,
    confluence: Confluence,
    db: AsyncDatabase,
    page: ConfluencePage,
    output_dir: str
) -> bool:
    """Обрабатывает одну страницу: скачивает и обновляет БД."""
    print(f"\n{'='*60}")
    print(f"Обработка: {page.page_title} (ID: {page.page_id})")
    print(f"{'='*60}")

    try:
        # Скачиваем страницу
        await download_page(session, confluence, page.page_id, output_dir)

        # Обновляем БД
        page.last_sync_date = datetime.now().isoformat()
        page.update_attempts = 0
        page.last_update_error = None
        await db.update_page(page)

        print(f"✓ БД обновлена: last_sync_date={page.last_sync_date}")
        return True

    except Exception as e:
        print(f"✗ Ошибка при обработке страницы {page.page_id}: {e}")

        # Обновляем БД с ошибкой
        page.update_attempts += 1
        page.last_update_error = str(e)
        await db.update_page(page)

        return False


async def main():
    """Основная функция."""
    # Загрузка конфигурации
    config = load_confluence_config()

    if not all([config["url"], config["cookie"]]):
        print("Ошибка: укажите CONFLUENCE_URL и CONFLUENCE_COOKIE в .env")
        return

    # Инициализация БД
    db = AsyncDatabase()
    await db._init_db()

    # Получаем страницы, требующие обновления
    pages = await db.get_pages_needing_update()

    if not pages:
        print("Нет страниц, требующих обновления (needs_update=1)")
        return

    print(f"Найдено страниц для обновления: {len(pages)}")

    # Инициализация Confluence
    confluence = Confluence(
        url=config["url"],
        cookies={"seraph.confluence": config["cookie"]},
        cloud=False
    )

    # Создаем директорию для загрузок
    output_dir = Path(__file__).parent / "downloads"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Асинхронно скачиваем страницы
    async with aiohttp.ClientSession() as session:
        tasks = [
            process_page(session, confluence, db, page, str(output_dir))
            for page in pages
        ]
        results = await asyncio.gather(*tasks)

    # Вывод результатов
    success_count = sum(1 for r in results if r)
    error_count = len(results) - success_count

    print("\n" + "=" * 60)
    print("РЕЗУЛЬТАТЫ")
    print("=" * 60)
    print(f"Всего обработано: {len(pages)}")
    print(f"Успешно: {success_count}")
    print(f"С ошибками: {error_count}")


if __name__ == "__main__":
    asyncio.run(main())
