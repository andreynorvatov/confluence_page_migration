"""
ORM для работы с SQLite и хранения информации о страницах Confluence.
"""

import sqlite3
from datetime import datetime
from typing import Optional, List
from contextlib import contextmanager


class ConfluencePage:
    """Модель страницы Confluence."""
    
    def __init__(self, id: str, title: str, last_modified: str, updated: bool = False):
        self.id = id
        self.title = title
        self.last_modified = last_modified
        self.updated = updated
    
    def __repr__(self):
        return f"ConfluencePage(id={self.id}, title={self.title}, updated={self.updated})"


class Database:
    """Класс для работы с базой данных."""
    
    def __init__(self, db_path: str = "confluence_pages.db"):
        self.db_path = db_path
        self._init_db()
    
    @contextmanager
    def get_connection(self):
        """Контекстный менеджер для подключения к БД."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()
    
    def _init_db(self):
        """Инициализирует базу данных и создаёт таблицу."""
        with self.get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pages (
                    id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    last_modified TEXT,
                    updated BOOLEAN DEFAULT FALSE,
                    synced_at TEXT NOT NULL
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_pages_title ON pages(title)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_pages_updated ON pages(updated)")
    
    def get_page(self, page_id: str) -> Optional[ConfluencePage]:
        """Получает страницу по ID."""
        with self.get_connection() as conn:
            row = conn.execute(
                "SELECT id, title, last_modified, updated FROM pages WHERE id = ?",
                (page_id,)
            ).fetchone()
            
            if row:
                return ConfluencePage(
                    id=row["id"],
                    title=row["title"],
                    last_modified=row["last_modified"],
                    updated=bool(row["updated"])
                )
        return None
    
    def get_all_pages(self) -> List[ConfluencePage]:
        """Получает все страницы из базы."""
        with self.get_connection() as conn:
            rows = conn.execute(
                "SELECT id, title, last_modified, updated FROM pages ORDER BY title"
            ).fetchall()
            
            return [
                ConfluencePage(
                    id=row["id"],
                    title=row["title"],
                    last_modified=row["last_modified"],
                    updated=bool(row["updated"])
                )
                for row in rows
            ]
    
    def insert_page(self, page: ConfluencePage):
        """Добавляет новую страницу."""
        with self.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO pages (id, title, last_modified, updated, synced_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (page.id, page.title, page.last_modified, page.updated, datetime.now().isoformat())
            )
    
    def update_page(self, page: ConfluencePage):
        """Обновляет существующую страницу."""
        with self.get_connection() as conn:
            conn.execute(
                """
                UPDATE pages 
                SET title = ?, last_modified = ?, updated = ?, synced_at = ?
                WHERE id = ?
                """,
                (page.title, page.last_modified, page.updated, datetime.now().isoformat(), page.id)
            )
    
    def upsert_page(self, page: ConfluencePage):
        """Добавляет страницу или обновляет, если она существует."""
        existing = self.get_page(page.id)
        
        if existing:
            # Проверяем, изменилась ли дата редактирования
            if existing.last_modified != page.last_modified:
                page.updated = True
                self.update_page(page)
            else:
                page.updated = False
                self.update_page(page)
        else:
            page.updated = False
            self.insert_page(page)
    
    def mark_all_as_not_updated(self):
        """Сбрасывает флаг updated у всех страниц."""
        with self.get_connection() as conn:
            conn.execute("UPDATE pages SET updated = FALSE")
    
    def get_updated_pages(self) -> List[ConfluencePage]:
        """Получает все обновлённые страницы."""
        with self.get_connection() as conn:
            rows = conn.execute(
                "SELECT id, title, last_modified, updated FROM pages WHERE updated = TRUE ORDER BY title"
            ).fetchall()
            
            return [
                ConfluencePage(
                    id=row["id"],
                    title=row["title"],
                    last_modified=row["last_modified"],
                    updated=bool(row["updated"])
                )
                for row in rows
            ]
    
    def print_pages_table(self, pages: List[ConfluencePage], title: str = "Страницы"):
        """Выводит страницы в виде таблицы."""
        if not pages:
            print(f"\n{title}: нет данных")
            return
        
        # Определяем ширину колонок
        id_width = max(len("ID"), max(len(p.id) for p in pages))
        title_width = max(len("Наименование"), max(len(p.title) for p in pages))
        date_width = len("Дата редактирования")
        status_width = len("Обновлена")
        
        # Выводим заголовок
        print("\n" + "=" * (id_width + title_width + date_width + status_width + 11))
        print(f"{'ID':<{id_width}} | {'Наименование':<{title_width}} | {'Дата редактирования':<{date_width}} | {'Обновлена':<{status_width}}")
        print("=" * (id_width + title_width + date_width + status_width + 11))
        
        # Выводим страницы
        for page in pages:
            date_formatted = format_date(page.last_modified)
            status = "✓" if page.updated else ""
            print(f"{page.id:<{id_width}} | {page.title:<{title_width}} | {date_formatted:<{date_width}} | {status:<{status_width}}")
        
        print("=" * (id_width + title_width + date_width + status_width + 11))
        print(f"Всего страниц: {len(pages)}")
        if any(p.updated for p in pages):
            print(f"Обновлённых: {sum(1 for p in pages if p.updated)}")


def format_date(date_string: str) -> str:
    """Форматирует дату в читаемый вид."""
    if not date_string:
        return "N/A"
    
    try:
        dt = datetime.fromisoformat(date_string.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, AttributeError):
        return date_string
