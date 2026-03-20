"""
Скрипт синхронизации страниц Confluence с локальной базой данных.
Получает данные из Confluence, проверяет наличие в БД и отмечает обновлённые страницы.
"""

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
            })
        
        get_descendants(root_page_id)
    else:
        all_pages = confluence.get_all_pages_from_space(space_key, expand="version")
        for page in all_pages:
            pages.append({
                "id": page.get("id"),
                "title": page.get("title"),
                "last_modified": page.get("version", {}).get("when"),
            })
    
    return pages


def sync_pages(confluence: Confluence, db: Database, space_key: str, root_page_id: str = None):
    """Синхронизирует страницы Confluence с базой данных."""
    
    # Получаем страницы из Confluence
    print(f"Получение страниц из Confluence (пространство: {space_key})...")
    if root_page_id:
        print(f"Корневая страница: {root_page_id}")
    
    confluence_pages = get_pages_tree(confluence, space_key, root_page_id)
    print(f"Найдено страниц: {len(confluence_pages)}")
    
    # Сбрасываем флаг updated у всех страниц
    db.mark_all_as_not_updated()
    
    # Синхронизируем каждую страницу
    for page_data in confluence_pages:
        page = ConfluencePage(
            id=page_data["id"],
            title=page_data["title"],
            last_modified=page_data["last_modified"],
        )
        db.upsert_page(page)
    
    print("\nСинхронизация завершена")
    
    # Выводим результат
    all_pages = db.get_all_pages()
    updated_pages = db.get_updated_pages()
    
    db.print_pages_table(all_pages, "Все страницы")
    
    if updated_pages:
        db.print_pages_table(updated_pages, "Обновлённые страницы")


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
    
    try:
        sync_pages(confluence, db, SPACE_KEY.upper(), ROOT_PAGE_ID or None)
    except Exception as e:
        print(f"Ошибка: {e}")


if __name__ == "__main__":
    main()
