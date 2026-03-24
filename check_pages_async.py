"""
Асинхронный скрипт проверки страниц Confluence.
Получает данные из Confluence параллельно, проверяет наличие в БД и отмечает страницы, требующие обновления.
"""

import asyncio
import time
import aiohttp
from dotenv import load_dotenv

from db_async import AsyncDatabase, ConfluencePage

import os


# ==================== НАСТРОЙКИ ====================

# Ключ пространства
SPACE_KEY = "ДИТ Москва"

# ID корневой страницы (None для всех страниц пространства)
ROOT_PAGE_ID = "92209311"

# Максимальное количество одновременных запросов (semaphore)
MAX_CONCURRENT_REQUESTS = 20

# ================================================


def load_confluence_config() -> dict:
    """Загружает конфигурацию Confluence из .env файла."""
    from pathlib import Path
    
    env_path = Path(__file__).parent / ".env"
    load_dotenv(dotenv_path=env_path)

    return {
        "url": os.getenv("CONFLUENCE_URL").rstrip('/'),
        "cookie": os.getenv("CONFLUENCE_COOKIE"),
        "username": os.getenv("CONFLUENCE_USERNAME"),
        "token": os.getenv("CONFLUENCE_TOKEN"),
    }


async def fetch_page_and_children(
    session: aiohttp.ClientSession,
    page_id: str,
    base_url: str,
    semaphore: asyncio.Semaphore
) -> tuple:
    """Получает страницу и её дочерние страницы с обработкой пагинации."""
    async with semaphore:
        try:
            # Получаем данные страницы
            async with session.get(f"{base_url}/rest/api/content/{page_id}?expand=version") as resp:
                if resp.status != 200:
                    return None, []
                page_data = await resp.json()

            # Получаем дочерние страницы с обработкой пагинации
            children = []
            start = 0
            limit = 100  # Максимальный лимит для уменьшения количества запросов
            
            while True:
                url = f"{base_url}/rest/api/content/{page_id}/child/page?expand=version&start={start}&limit={limit}"
                async with session.get(url) as resp:
                    if resp.status != 200:
                        break
                    children_data = await resp.json()
                    results = children_data.get("results", [])
                    
                    if not results:
                        break
                    
                    children.extend([
                        {
                            "id": str(child.get("id")),
                            "title": child.get("title"),
                            "last_modified": child.get("version", {}).get("when"),
                            "url": f"{base_url}/pages/viewpage.action?pageId={child.get('id')}",
                        }
                        for child in results
                    ])
                    
                    # Проверяем, есть ли ещё страницы
                    next_link = children_data.get("_links", {}).get("next")
                    if not next_link:
                        break
                    
                    start += limit

            page_info = {
                "id": str(page_data.get("id")),
                "title": page_data.get("title"),
                "last_modified": page_data.get("version", {}).get("when"),
                "url": f"{base_url}/pages/viewpage.action?pageId={page_id}",
            }

            return page_info, children

        except Exception as e:
            print(f"  ⚠ Ошибка получения страницы {page_id}: {e}")
            return None, []


async def get_pages_tree_async(config: dict, space_key: str, root_page_id: str = None) -> list:
    """
    Получает список страниц пространства асинхронно с использованием BFS.
    """
    base_url = config["url"]
    pages = []
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    headers = {"Cookie": f"seraph.confluence={config['cookie']}"} if config.get("cookie") else {}
    
    async with aiohttp.ClientSession(
        headers=headers,
        auth=aiohttp.BasicAuth(config["username"], config["token"]) if config.get("username") and config.get("token") else None
    ) as session:
        
        if root_page_id:
            # BFS обход дерева страниц
            queue = [str(root_page_id)]
            processed = set()

            while queue:
                # Берём текущий уровень
                current_level = queue[:MAX_CONCURRENT_REQUESTS * 2]
                queue = queue[MAX_CONCURRENT_REQUESTS * 2:]

                # Создаём задачи для всех страниц текущего уровня
                tasks = [fetch_page_and_children(session, pid, base_url, semaphore) for pid in current_level]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for page_id, result in zip(current_level, results):
                    # Нормализуем page_id к строке
                    page_id = str(page_id)
                    
                    if page_id in processed:
                        continue
                    processed.add(page_id)

                    # Обработка ошибок
                    if isinstance(result, Exception):
                        continue
                    
                    if result[0] is None:
                        continue

                    page_info, children = result
                    pages.append(page_info)

                    # Добавляем детей в очередь (только необработанные)
                    for child in children:
                        child_id = str(child["id"])
                        if child_id not in processed and child_id not in queue:
                            queue.append(child_id)
        else:
            # Получаем все страницы пространства с обработкой пагинации
            pages = []
            start = 0
            limit = 100
            
            while True:
                url = f"{base_url}/rest/api/content?spaceKey={space_key}&expand=version&start={start}&limit={limit}"
                async with session.get(url) as response:
                    if response.status != 200:
                        break
                    data = await response.json()
                    results = data.get("results", [])
                    
                    if not results:
                        break
                    
                    pages.extend([
                        {
                            "id": str(p.get("id")),
                            "title": p.get("title"),
                            "last_modified": p.get("version", {}).get("when"),
                            "url": f"{base_url}/pages/viewpage.action?pageId={p.get('id')}",
                        }
                        for p in results
                    ])
                    
                    # Проверяем, есть ли ещё страницы
                    next_link = data.get("_links", {}).get("next")
                    if not next_link:
                        break
                    
                    start += limit

    return pages


async def check_pages_async(config: dict, db: AsyncDatabase, space_key: str, root_page_id: str = None):
    """Проверяет страницы Confluence и обновляет базу данных."""

    print(f"Получение страниц из Confluence (пространство: {space_key})...")
    if root_page_id:
        print(f"Корневая страница: {root_page_id}")
    print(f"Максимум одновременных запросов: {MAX_CONCURRENT_REQUESTS}")

    # Получаем страницы из Confluence
    confluence_pages = await get_pages_tree_async(config, space_key, root_page_id)
    print(f"Найдено страниц: {len(confluence_pages)}")

    # Формируем множество актуальных ID страниц (нормализованных к строке)
    current_page_ids = set()

    # Проверяем каждую страницу
    for page_data in confluence_pages:
        # Нормализуем ID к строке
        page_id = str(page_data["id"])
        current_page_ids.add(page_id)

        page = ConfluencePage(
            page_id=page_id,
            page_title=page_data["title"],
            last_edited_date=page_data["last_modified"],
            space_key=space_key,
            page_url=page_data.get("url"),
            last_sync_date=None,
        )
        await db.upsert_page(page)

    # Проверяем, не были ли удалены страницы, которые есть в БД
    all_db_pages = await db.get_all_pages(include_deleted=False)
    for db_page in all_db_pages:
        # Нормализуем сравнение ID
        if str(db_page.page_id) not in current_page_ids:
            await db.mark_page_as_deleted(db_page.page_id)
            print(f"  Удалена страница: {db_page.page_title} (ID: {db_page.page_id})")

    print("\nПроверка завершена")

    # Выводим результат
    all_pages = await db.get_all_pages()
    needs_update_pages = await db.get_pages_needing_update()
    error_pages = await db.get_pages_with_errors()

    db.print_pages_table(all_pages, "Все страницы")

    if needs_update_pages:
        db.print_pages_table(needs_update_pages, "Требуют обновления")

    if error_pages:
        db.print_pages_table(error_pages, "С ошибками")


async def main():
    config = load_confluence_config()

    # Проверка конфигурации
    if not config.get("url"):
        print("Ошибка: не указан CONFLUENCE_URL в .env")
        return

    if not config.get("cookie") and not (config.get("username") and config.get("token")):
        print("Ошибка: укажите CONFLUENCE_COOKIE или CONFLUENCE_USERNAME и CONFLUENCE_TOKEN в .env")
        return

    # Инициализируем базу данных
    db = AsyncDatabase()
    await db._init_db()

    # Замер времени выполнения
    start_time = time.perf_counter()

    try:
        await check_pages_async(config, db, SPACE_KEY.upper(), ROOT_PAGE_ID or None)
    except Exception as e:
        print(f"Ошибка: {e}")

    elapsed_time = time.perf_counter() - start_time
    print(f"\n⏱ Время выполнения: {elapsed_time:.2f} сек")


if __name__ == "__main__":
    asyncio.run(main())
