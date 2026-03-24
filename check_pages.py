"""
Скрипт проверки страниц Confluence.
Получает данные из Confluence, проверяет наличие в БД и отмечает страницы, требующие обновления.
"""

import time
from dotenv import load_dotenv
from atlassian import Confluence

from db import Database, ConfluencePage

import os


# ==================== НАСТРОЙКИ ====================

# Ключ пространства
SPACE_KEY = "ДИТ Москва"

# ID корневой страницы (None для всех страниц пространства)
ROOT_PAGE_ID = "92209311"

# ================================================


def load_confluence_config() -> dict:
    """Загружает конфигурацию Confluence из .env файла."""
    from pathlib import Path
    
    env_path = Path(__file__).parent / ".env"
    load_dotenv(dotenv_path=env_path)

    return {
        "url": os.getenv("CONFLUENCE_URL"),
        "cookie": os.getenv("CONFLUENCE_COOKIE"),
        "username": os.getenv("CONFLUENCE_USERNAME"),
        "token": os.getenv("CONFLUENCE_TOKEN"),
    }


def get_pages_tree(confluence: Confluence, space_key: str, root_page_id: str = None) -> list:
    """
    Получает список страниц пространства с указанной корневой страницей.
    """
    pages = []
    base_url = confluence.url.rstrip('/')
    
    if root_page_id:
        def get_descendants(page_id: str):
            """Рекурсивно получает все дочерние страницы."""
            children_data = confluence.get(f"rest/api/content/{page_id}/child/page?expand=version")
            children = children_data.get("results", [])
            
            for child in children:
                page_info = {
                    "id": child.get("id"),
                    "title": child.get("title"),
                    "last_modified": child.get("version", {}).get("when"),
                    "url": f"{base_url}/pages/viewpage.action?pageId={child.get('id')}",
                }
                pages.append(page_info)
                get_descendants(child.get("id"))
        
        # Добавляем саму корневую страницу
        root_page = confluence.get_page_by_id(root_page_id, expand="version")
        if root_page:
            pages.append({
                "id": root_page.get("id"),
                "title": root_page.get("title"),
                "last_modified": root_page.get("version", {}).get("when"),
                "url": f"{base_url}/pages/viewpage.action?pageId={root_page_id}",
            })
        
        get_descendants(root_page_id)
    else:
        all_pages = confluence.get_all_pages_from_space(space_key, expand="version")
        for page in all_pages:
            pages.append({
                "id": page.get("id"),
                "title": page.get("title"),
                "last_modified": page.get("version", {}).get("when"),
                "url": f"{base_url}/pages/viewpage.action?pageId={page.get('id')}",
            })
    
    return pages


def check_pages(confluence: Confluence, db: Database, space_key: str, root_page_id: str = None):
    """Проверяет страницы Confluence и обновляет базу данных."""
    
    # Получаем страницы из Confluence
    print(f"Получение страниц из Confluence (пространство: {space_key})...")
    if root_page_id:
        print(f"Корневая страница: {root_page_id}")
    
    confluence_pages = get_pages_tree(confluence, space_key, root_page_id)
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
            last_sync_date=None,  # check_pages не заполняет это поле
        )
        db.upsert_page(page)
    
    # Проверяем, не были ли удалены страницы, которые есть в БД
    all_db_pages = db.get_all_pages(include_deleted=False)
    for db_page in all_db_pages:
        if db_page.page_id not in current_page_ids:
            db.mark_page_as_deleted(db_page.page_id)
            print(f"  Удалена страница: {db_page.page_title} (ID: {db_page.page_id})")
    
    print("\nПроверка завершена")
    
    # Выводим результат
    all_pages = db.get_all_pages()
    needs_update_pages = db.get_pages_needing_update()
    error_pages = db.get_pages_with_errors()
    
    db.print_pages_table(all_pages, "Все страницы")
    
    if needs_update_pages:
        db.print_pages_table(needs_update_pages, "Требуют обновления")
    
    if error_pages:
        db.print_pages_table(error_pages, "С ошибками")


def main():
    config = load_confluence_config()

    # Определяем тип аутентификации
    if config.get("cookie"):
        if not config["url"]:
            print("Ошибка: не указан CONFLUENCE_URL в .env")
            return

        confluence = Confluence(
            url=config["url"],
            cookies={"seraph.confluence": config["cookie"]},
            cloud=False,
        )
    elif config.get("username") and config.get("token"):
        if not config["url"]:
            print("Ошибка: не указан CONFLUENCE_URL в .env")
            return

        confluence = Confluence(
            url=config["url"],
            username=config["username"],
            password=config["token"],
            cloud=True,
        )
    else:
        print("Ошибка: укажите CONFLUENCE_COOKIE или CONFLUENCE_USERNAME и CONFLUENCE_TOKEN в .env")
        return

    # Инициализируем базу данных
    db = Database()
    
    # Замер времени выполнения
    start_time = time.time()

    try:
        check_pages(confluence, db, SPACE_KEY.upper(), ROOT_PAGE_ID or None)
    except Exception as e:
        print(f"Ошибка: {e}")
    
    elapsed_time = time.time() - start_time
    print(f"\n⏱ Время выполнения: {elapsed_time:.2f} сек")


if __name__ == "__main__":
    main()
