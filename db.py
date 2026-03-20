"""
ORM для работы с SQLite и хранения информации о страницах Confluence.
"""

import sqlite3
from datetime import datetime
from typing import Optional, List
from contextlib import contextmanager


class ConfluencePage:
    """Модель страницы Confluence."""
    
    def __init__(
        self,
        page_id: str,
        page_title: str,
        last_edited_date: str,
        last_check_date: str = None,
        last_sync_date: str = None,
        needs_update: bool = False,
        update_attempts: int = 0,
        last_update_error: str = None,
        space_key: str = None,
        page_url: str = None,
        is_deleted: bool = False,
    ):
        self.page_id = page_id
        self.page_title = page_title
        self.last_edited_date = last_edited_date
        self.last_check_date = last_check_date or datetime.now().isoformat()
        self.last_sync_date = last_sync_date
        self.needs_update = needs_update
        self.update_attempts = update_attempts
        self.last_update_error = last_update_error
        self.space_key = space_key
        self.page_url = page_url
        self.is_deleted = is_deleted
    
    def __repr__(self):
        return f"ConfluencePage(id={self.page_id}, title={self.page_title}, needs_update={self.needs_update})"


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
                    page_id TEXT PRIMARY KEY,
                    page_title TEXT NOT NULL,
                    last_edited_date TEXT,
                    last_check_date TEXT,
                    last_sync_date TEXT,
                    needs_update BOOLEAN DEFAULT FALSE,
                    update_attempts INTEGER DEFAULT 0,
                    last_update_error TEXT,
                    space_key TEXT,
                    page_url TEXT,
                    is_deleted BOOLEAN DEFAULT FALSE
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_pages_title ON pages(page_title)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_pages_needs_update ON pages(needs_update)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_pages_space ON pages(space_key)")
    
    def get_page(self, page_id: str) -> Optional[ConfluencePage]:
        """Получает страницу по ID."""
        with self.get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM pages WHERE page_id = ? AND is_deleted = FALSE",
                (page_id,)
            ).fetchone()
            
            if row:
                return ConfluencePage(
                    page_id=row["page_id"],
                    page_title=row["page_title"],
                    last_edited_date=row["last_edited_date"],
                    last_check_date=row["last_check_date"],
                    last_sync_date=row["last_sync_date"],
                    needs_update=bool(row["needs_update"]),
                    update_attempts=row["update_attempts"],
                    last_update_error=row["last_update_error"],
                    space_key=row["space_key"],
                    page_url=row["page_url"],
                    is_deleted=bool(row["is_deleted"])
                )
        return None
    
    def get_all_pages(self, include_deleted: bool = False) -> List[ConfluencePage]:
        """Получает все страницы из базы."""
        query = "SELECT * FROM pages"
        if not include_deleted:
            query += " WHERE is_deleted = FALSE"
        query += " ORDER BY page_title"
        
        with self.get_connection() as conn:
            rows = conn.execute(query).fetchall()
            
            return [
                ConfluencePage(
                    page_id=row["page_id"],
                    page_title=row["page_title"],
                    last_edited_date=row["last_edited_date"],
                    last_check_date=row["last_check_date"],
                    last_sync_date=row["last_sync_date"],
                    needs_update=bool(row["needs_update"]),
                    update_attempts=row["update_attempts"],
                    last_update_error=row["last_update_error"],
                    space_key=row["space_key"],
                    page_url=row["page_url"],
                    is_deleted=bool(row["is_deleted"])
                )
                for row in rows
            ]
    
    def insert_page(self, page: ConfluencePage):
        """Добавляет новую страницу."""
        with self.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO pages (
                    page_id, page_title, last_edited_date, last_check_date, last_sync_date,
                    needs_update, update_attempts, last_update_error,
                    space_key, page_url, is_deleted
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    page.page_id, page.page_title, page.last_edited_date,
                    page.last_check_date, page.last_sync_date, page.needs_update, page.update_attempts,
                    page.last_update_error, page.space_key, page.page_url, page.is_deleted
                )
            )
    
    def update_page(self, page: ConfluencePage):
        """Обновляет существующую страницу."""
        with self.get_connection() as conn:
            conn.execute(
                """
                UPDATE pages 
                SET page_title = ?,
                    last_edited_date = ?,
                    last_check_date = ?,
                    last_sync_date = ?,
                    needs_update = ?,
                    update_attempts = ?,
                    last_update_error = ?,
                    space_key = ?,
                    page_url = ?,
                    is_deleted = ?
                WHERE page_id = ?
                """,
                (
                    page.page_title, page.last_edited_date, page.last_check_date,
                    page.last_sync_date, page.needs_update, page.update_attempts, page.last_update_error,
                    page.space_key, page.page_url, page.is_deleted, page.page_id
                )
            )
    
    def upsert_page(self, page: ConfluencePage):
        """
        Добавляет страницу или обновляет, если она существует.
        
        Логика:
        - Если страницы нет в БД → добавляем с needs_update=False
        - Если страница есть и last_edited_date изменилась → устанавливаем needs_update=True
          (старое last_edited_date сохраняем, не обновляем)
        - Если страница есть и last_edited_date не изменилась → needs_update=False
        """
        existing = self.get_page(page.page_id)
        
        if existing:
            # Проверяем, изменилась ли дата редактирования
            if existing.last_edited_date != page.last_edited_date:
                page.needs_update = True
                page.update_attempts = 0  # Сбрасываем счётчик при обнаружении изменений
                page.last_update_error = None
                # Сохраняем старое last_edited_date, не обновляем
                page.last_edited_date = existing.last_edited_date
                self.update_page(page)
            else:
                page.needs_update = False
                page.update_attempts = existing.update_attempts
                page.last_update_error = existing.last_update_error
                self.update_page(page)
        else:
            # Новая страница
            page.needs_update = False
            page.update_attempts = 0
            self.insert_page(page)
    
    def mark_page_for_update(self, page_id: str):
        """Устанавливает флаг needs_update для страницы."""
        with self.get_connection() as conn:
            conn.execute(
                "UPDATE pages SET needs_update = TRUE, last_check_date = ? WHERE page_id = ?",
                (datetime.now().isoformat(), page_id)
            )
    
    def mark_page_as_updated(self, page_id: str):
        """Снимает флаг needs_update после успешного обновления."""
        with self.get_connection() as conn:
            conn.execute(
                """
                UPDATE pages 
                SET needs_update = FALSE, update_attempts = 0, last_update_error = NULL, last_check_date = ?
                WHERE page_id = ?
                """,
                (datetime.now().isoformat(), page_id)
            )
    
    def mark_page_update_failed(self, page_id: str, error: str):
        """Регистрирует неудачную попытку обновления."""
        with self.get_connection() as conn:
            conn.execute(
                """
                UPDATE pages 
                SET update_attempts = update_attempts + 1,
                    last_update_error = ?,
                    last_check_date = ?
                WHERE page_id = ?
                """,
                (error, datetime.now().isoformat(), page_id)
            )
    
    def mark_page_as_deleted(self, page_id: str):
        """Помечает страницу как удалённую (мягкое удаление)."""
        with self.get_connection() as conn:
            conn.execute(
                "UPDATE pages SET is_deleted = TRUE, last_check_date = ? WHERE page_id = ?",
                (datetime.now().isoformat(), page_id)
            )
    
    def get_pages_needing_update(self) -> List[ConfluencePage]:
        """Получает все страницы, требующие обновления."""
        with self.get_connection() as conn:
            rows = conn.execute(
                """
                SELECT * FROM pages 
                WHERE needs_update = TRUE AND is_deleted = FALSE 
                ORDER BY page_title
                """,
            ).fetchall()
            
            return [
                ConfluencePage(
                    page_id=row["page_id"],
                    page_title=row["page_title"],
                    last_edited_date=row["last_edited_date"],
                    last_check_date=row["last_check_date"],
                    last_sync_date=row["last_sync_date"],
                    needs_update=bool(row["needs_update"]),
                    update_attempts=row["update_attempts"],
                    last_update_error=row["last_update_error"],
                    space_key=row["space_key"],
                    page_url=row["page_url"],
                    is_deleted=bool(row["is_deleted"])
                )
                for row in rows
            ]
    
    def get_pages_with_errors(self) -> List[ConfluencePage]:
        """Получает страницы с ошибками обновления."""
        with self.get_connection() as conn:
            rows = conn.execute(
                """
                SELECT * FROM pages 
                WHERE last_update_error IS NOT NULL AND is_deleted = FALSE 
                ORDER BY page_title
                """,
            ).fetchall()
            
            return [
                ConfluencePage(
                    page_id=row["page_id"],
                    page_title=row["page_title"],
                    last_edited_date=row["last_edited_date"],
                    last_check_date=row["last_check_date"],
                    last_sync_date=row["last_sync_date"],
                    needs_update=bool(row["needs_update"]),
                    update_attempts=row["update_attempts"],
                    last_update_error=row["last_update_error"],
                    space_key=row["space_key"],
                    page_url=row["page_url"],
                    is_deleted=bool(row["is_deleted"])
                )
                for row in rows
            ]
    
    def print_pages_table(self, pages: List[ConfluencePage], title: str = "Страницы"):
        """Выводит страницы в виде таблицы."""
        if not pages:
            print(f"\n{title}: нет данных")
            return
        
        # Определяем ширину колонок
        id_width = max(len("ID"), max(len(p.page_id) for p in pages))
        title_width = max(len("Наименование"), max(len(p.page_title) for p in pages))
        date_width = len("Дата редактирования")
        status_width = len("Требует обновл.")
        url_width = min(60, max(len("URL"), max(len(p.page_url or "") for p in pages)))
        
        # Выводим заголовок
        print("\n" + "=" * (id_width + title_width + date_width + status_width + url_width + 11))
        print(f"{'ID':<{id_width}} | {'Наименование':<{title_width}} | {'Дата редактирования':<{date_width}} | {'Требует обновл.':<{status_width}} | {'URL':<{url_width}}")
        print("=" * (id_width + title_width + date_width + status_width + url_width + 11))
        
        # Выводим страницы
        for page in pages:
            date_formatted = format_date(page.last_edited_date)
            status = "✓" if page.needs_update else ""
            url_display = page.page_url if page.page_url else ""
            if len(url_display) > url_width:
                url_display = "..." + url_display[-(url_width-3):]
            print(f"{page.page_id:<{id_width}} | {page.page_title:<{title_width}} | {date_formatted:<{date_width}} | {status:<{status_width}} | {url_display:<{url_width}}")
        
        print("=" * (id_width + title_width + date_width + status_width + url_width + 11))
        print(f"Всего страниц: {len(pages)}")
        
        needs_update_count = sum(1 for p in pages if p.needs_update)
        if needs_update_count:
            print(f"Требуют обновления: {needs_update_count}")
        
        error_count = sum(1 for p in pages if p.last_update_error)
        if error_count:
            print(f"С ошибками: {error_count}")


def format_date(date_string: str) -> str:
    """Форматирует дату в читаемый вид."""
    if not date_string:
        return "N/A"
    
    try:
        dt = datetime.fromisoformat(date_string.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, AttributeError):
        return date_string
