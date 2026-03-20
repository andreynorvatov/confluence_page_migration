"""
Скрипт для получения информации о страницах Confluence.
Выводит наименование, ID и дату последнего редактирования страниц.
"""

from datetime import datetime
from dotenv import load_dotenv
from atlassian import Confluence

import os


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
    
    Args:
        confluence: Экземпляр Confluence клиента
        space_key: Ключ пространства
        root_page_id: ID корневой страницы (None для всех страниц пространства)
    
    Returns:
        Список словарей с информацией о страницах
    """
    pages = []
    
    if root_page_id:
        # Получаем страницы, начиная с корневой
        def get_descendants(page_id: str):
            """Рекурсивно получает все дочерние страницы."""
            # Прямой REST API запрос с expand
            children_data = confluence.get(f"rest/api/content/{page_id}/child/page?expand=version")
            children = children_data.get("results", [])
            
            for child in children:
                page_info = {
                    "id": child.get("id"),
                    "title": child.get("title"),
                    "last_modified": child.get("version", {}).get("when"),
                }
                pages.append(page_info)
                
                # Рекурсивно получаем дочерние страницы
                get_descendants(child.get("id"))
        
        # Добавляем саму корневую страницу
        root_page = confluence.get_page_by_id(root_page_id, expand="version")
        if root_page:
            pages.append({
                "id": root_page.get("id"),
                "title": root_page.get("title"),
                "last_modified": root_page.get("version", {}).get("when"),
            })
        
        # Получаем все дочерние страницы
        get_descendants(root_page_id)
    else:
        # Получаем все страницы пространства
        all_pages = confluence.get_all_pages_from_space(space_key, expand="version")
        for page in all_pages:
            pages.append({
                "id": page.get("id"),
                "title": page.get("title"),
                "last_modified": page.get("version", {}).get("when"),
            })
    
    return pages


def format_date(date_string: str) -> str:
    """Форматирует дату в читаемый вид."""
    if not date_string:
        return "N/A"
    
    try:
        # Парсим ISO формат даты
        dt = datetime.fromisoformat(date_string.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, AttributeError):
        return date_string


def print_pages_table(pages: list):
    """Выводит информацию о страницах в виде таблицы."""
    if not pages:
        print("Страницы не найдены")
        return
    
    # Определяем ширину колонок
    id_width = max(len("ID"), max(len(p["id"]) for p in pages))
    title_width = max(len("Наименование"), max(len(p["title"]) for p in pages))
    date_width = len("Дата редактирования")
    
    # Выводим заголовок
    print("\n" + "=" * (id_width + title_width + date_width + 8))
    print(f"{'ID':<{id_width}} | {'Наименование':<{title_width}} | {'Дата редактирования':<{date_width}}")
    print("=" * (id_width + title_width + date_width + 8))
    
    # Выводим страницы
    for page in pages:
        date_formatted = format_date(page.get("last_modified"))
        print(f"{page['id']:<{id_width}} | {page['title']:<{title_width}} | {date_formatted:<{date_width}}")
    
    print("=" * (id_width + title_width + date_width + 8))
    print(f"Всего страниц: {len(pages)}")


# ==================== НАСТРОЙКИ ====================

# Ключ пространства
SPACE_KEY = "ДИТ Москва"

# ID корневой страницы (None для всех страниц пространства)
ROOT_PAGE_ID = "165712109"

# ================================================


def main():
    config = load_confluence_config()

    # Определяем тип аутентификации
    if config.get("cookie"):
        # Server/Data Center с cookie
        if not config["url"]:
            print("Ошибка: не указан CONFLUENCE_URL в .env")
            return
        
        confluence = Confluence(
            url=config["url"],
            cookies={"seraph.confluence": config["cookie"]},
            cloud=False,
        )
    elif config.get("username") and config.get("token"):
        # Cloud с username/token
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

    try:
        print(f"\nПолучение страниц пространства '{SPACE_KEY}'...")
        if ROOT_PAGE_ID:
            print(f"Корневая страница: {ROOT_PAGE_ID}")
        
        pages = get_pages_tree(confluence, SPACE_KEY.upper(), ROOT_PAGE_ID or None)
        print_pages_table(pages)
        
    except Exception as e:
        print(f"Ошибка: {e}")


if __name__ == "__main__":
    main()
