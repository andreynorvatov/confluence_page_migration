"""
Асинхронный скрипт проверки страниц Confluence.
Получает данные из Confluence параллельно, проверяет наличие в БД и отмечает страницы, требующие обновления.
"""

import asyncio
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
MAX_CONCURRENT_REQUESTS = 10

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


async def get_page_data(session: aiohttp.ClientSession, page_id: str, base_url: str, semaphore: asyncio.Semaphore) -> dict:
    """Получает данные о странице через REST API."""
    async with semaphore:
        url = f"{base_url}/rest/api/content/{page_id}?expand=version"
        async with session.get(url) as response:
            if response.status != 200:
                raise Exception(f"Ошибка получения страницы {page_id}: {response.status}")
            
            data = await response.json()
            
            return {
                "id": data.get("id"),
                "title": data.get("title"),
                "last_modified": data.get("version", {}).get("when"),
                "url": f"{base_url}/pages/viewpage.action?pageId={page_id}",
            }


async def get_child_pages(session: aiohttp.ClientSession, page_id: str, base_url: str, semaphore: asyncio.Semaphore) -> list:
    """Получает дочерние страницы через REST API."""
    async with semaphore:
        url = f"{base_url}/rest/api/content/{page_id}/child/page?expand=version"
        async with session.get(url) as response:
            if response.status != 200:
                return []
            
            data = await response.json()
            results = data.get("results", [])
            
            return [
                {
                    "id": child.get("id"),
                    "title": child.get("title"),
                    "last_modified": child.get("version", {}).get("when"),
                    "url": f"{base_url}/pages/viewpage.action?pageId={child.get('id')}",
                }
                for child in results
            ]


async def get_descendants_recursive(
    session: aiohttp.ClientSession,
    page_id: str,
    base_url: str,
    semaphore: asyncio.Semaphore
) -> list:
    """Рекурсивно получает все дочерние страницы."""
    pages = []
    
    children = await get_child_pages(session, page_id, base_url, semaphore)
    
    for child in children:
        pages.append(child)
        # Рекурсивно получаем потомков
        descendants = await get_descendants_recursive(session, child["id"], base_url, semaphore)
        pages.extend(descendants)
    
    return pages


async def get_pages_tree_async(config: dict, space_key: str, root_page_id: str = None) -> list:
    """
    Получает список страниц пространства асинхронно.
    """
    base_url = config["url"]
    pages = []
    
    # Создаём сессию и semaphore для ограничения одновременных запросов
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    headers = {}
    if config.get("cookie"):
        headers["Cookie"] = f"seraph.confluence={config['cookie']}"
    elif config.get("username") and config.get("token"):
        from aiohttp import BasicAuth
    
    async with aiohttp.ClientSession(
        headers=headers,
        auth=BasicAuth(config["username"], config["token"]) if config.get("username") and config.get("token") else None
    ) as session:
        if root_page_id:
            # Получаем корневую страницу
            root_data = await get_page_data(session, root_page_id, base_url, semaphore)
            pages.append(root_data)
            
            # Получаем всех потомков рекурсивно
            descendants = await get_descendants_recursive(session, root_page_id, base_url, semaphore)
            pages.extend(descendants)
        else:
            # Получаем все страницы пространства
            url = f"{base_url}/rest/api/content?spaceKey={space_key}&expand=version&limit=100"
            
            async with semaphore:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        results = data.get("results", [])
                        
                        pages = [
                            {
                                "id": p.get("id"),
                                "title": p.get("title"),
                                "last_modified": p.get("version", {}).get("when"),
                                "url": f"{base_url}/pages/viewpage.action?pageId={p.get('id')}",
                            }
                            for p in results
                        ]
    
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
    
    # Формируем множество актуальных ID страниц
    current_page_ids = set()
    
    # Проверяем каждую страницу
    for page_data in confluence_pages:
        current_page_ids.add(page_data["id"])
        
        page = ConfluencePage(
            page_id=page_data["id"],
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
        if db_page.page_id not in current_page_ids:
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
    
    try:
        await check_pages_async(config, db, SPACE_KEY.upper(), ROOT_PAGE_ID or None)
    except Exception as e:
        print(f"Ошибка: {e}")


if __name__ == "__main__":
    asyncio.run(main())
