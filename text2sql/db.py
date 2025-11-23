import os
import sqlite3
from pathlib import Path
from typing import List, Tuple, Any, Optional, Dict
import pandas as pd
import re


DATA_DIR = Path("data")
DB_PATH = DATA_DIR / "database.db"


def ensure_database_exists() -> None:
    """Создает директорию для данных, но не создает стандартную БД."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def get_connection(readonly: bool = True, db_path: Optional[Path] = None) -> sqlite3.Connection:
    if db_path is None:
        raise ValueError("db_path must be provided. No default database is used.")
    ensure_database_exists()
    if readonly:
        # Enforce read-only mode to prevent writes from generated SQL
        uri = f"file:{db_path.as_posix()}?mode=ro"
        return sqlite3.connect(uri, uri=True, timeout=5)
    return sqlite3.connect(db_path.as_posix(), timeout=5)


def list_tables_and_schema(db_path: Optional[Path] = None, schema_description: Optional[str] = None, table_name: Optional[str] = None) -> str:
    """
    Возвращает схему БД на основе реальной SQL таблицы.
    schema_description используется только для LLM, но не для отображения схемы БД.
    
    Args:
        db_path: Путь к БД (если None, вернет сообщение об отсутствии данных)
        schema_description: Текстовое описание схемы (используется только для LLM, игнорируется здесь)
        table_name: Имя таблицы для отображения (если None, показываются все таблицы)
    """
    if db_path is None:
        return "Нет загруженных данных. Загрузите CSV файл и описание таблицы."
    
    conn = get_connection(readonly=True, db_path=db_path)
    try:
        cur = conn.cursor()
        
        # Если указано имя таблицы, показываем только её
        if table_name:
            # Проверяем, существует ли таблица
            safe_table_name = table_name.replace('"', '""')  # Escape double quotes
            tables = cur.execute(
                f'SELECT name FROM sqlite_master WHERE type="table" AND name="{safe_table_name}";'
            ).fetchall()
            if not tables:
                return f"Таблица '{table_name}' не найдена в базе данных."
            
            lines: List[str] = []
            lines.append(f"TABLE {table_name}")
            cols = cur.execute(f'PRAGMA table_info("{safe_table_name}");').fetchall()
            for col in cols:
                # pragma: cid, name, type, notnull, dflt_value, pk
                # Выводим только название столбца и тип (без NOT NULL, PRIMARY KEY и т.д.)
                cname = col[1]
                ctype = col[2]
                lines.append(f"  - {cname} {ctype}")
            return "\n".join(lines)
        else:
            # Если имя таблицы не указано, показываем все таблицы
            tables = cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;"
            ).fetchall()
            lines: List[str] = []
            for (tbl_name,) in tables:
                lines.append(f"TABLE {tbl_name}")
                # Use parameterized query to prevent SQL injection (though table_name comes from sqlite_master)
                # SQLite doesn't support parameters in PRAGMA, so we sanitize by quoting
                safe_tbl_name = tbl_name.replace('"', '""')  # Escape double quotes
                cols = cur.execute(f'PRAGMA table_info("{safe_tbl_name}");').fetchall()
                for col in cols:
                    # pragma: cid, name, type, notnull, dflt_value, pk
                    # Выводим только название столбца и тип (без NOT NULL, PRIMARY KEY и т.д.)
                    cname = col[1]
                    ctype = col[2]
                    lines.append(f"  - {cname} {ctype}")
            return "\n".join(lines) if lines else "Нет таблиц в базе данных"
    finally:
        conn.close()


def execute_readonly(sql: str, db_path: Optional[Path] = None) -> Tuple[List[str], List[Tuple[Any, ...]]]:
    if not sql.strip().lower().startswith("select"):
        raise ValueError("Only SELECT queries are allowed.")
    # Check for multiple statements by counting semicolons outside of string literals
    sql_clean = sql.strip().rstrip(";")
    # Simple check: count semicolons that are not inside quotes
    # This is a basic check - for production, use a proper SQL parser
    in_single_quote = False
    in_double_quote = False
    semicolon_count = 0
    for i, char in enumerate(sql_clean):
        if char == "'" and (i == 0 or sql_clean[i-1] != "\\"):
            in_single_quote = not in_single_quote
        elif char == '"' and (i == 0 or sql_clean[i-1] != "\\"):
            in_double_quote = not in_double_quote
        elif char == ";" and not in_single_quote and not in_double_quote:
            semicolon_count += 1
    if semicolon_count > 0:
        raise ValueError("Only a single SELECT statement is allowed.")

    conn = get_connection(readonly=True, db_path=db_path)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        headers = [desc[0] for desc in cur.description] if cur.description else []
        return headers, rows
    finally:
        conn.close()


def sanitize_table_name(name: str) -> str:
    """Преобразует имя таблицы в валидное SQLite имя."""
    # Удаляем все недопустимые символы, оставляем только буквы, цифры и подчеркивания
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    # Убираем множественные подчеркивания
    sanitized = re.sub(r'_+', '_', sanitized)
    # Убираем подчеркивания в начале и конце
    sanitized = sanitized.strip('_')
    # Если имя пустое или начинается с цифры, добавляем префикс
    if not sanitized or sanitized[0].isdigit():
        sanitized = 'table_' + sanitized
    return sanitized.lower() if sanitized else 'table'


def import_csv_to_sqlite(
    csv_file_path: str,
    table_name: str,
    db_path: Optional[Path] = None,
    encoding: str = 'utf-8'
) -> Tuple[str, Path]:
    """
    Импортирует CSV файл в SQLite базу данных.
    
    Args:
        csv_file_path: Путь к CSV файлу
        table_name: Имя таблицы (будет санитизировано)
        db_path: Путь к БД (если None, используется DB_PATH)
        encoding: Кодировка CSV файла
        
    Returns:
        Tuple[actual_table_name, db_path]
    """
    target_db = db_path or DB_PATH
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    # Санитизируем имя таблицы
    safe_table_name = sanitize_table_name(table_name)
    
    # Читаем CSV
    try:
        df = pd.read_csv(csv_file_path, encoding=encoding)
    except UnicodeDecodeError:
        # Пробуем другие кодировки
        for enc in ['cp1251', 'latin-1', 'iso-8859-1']:
            try:
                df = pd.read_csv(csv_file_path, encoding=enc)
                break
            except UnicodeDecodeError:
                continue
        else:
            raise ValueError(f"Не удалось прочитать CSV файл с кодировками: {encoding}, cp1251, latin-1, iso-8859-1")
    
    if df.empty:
        raise ValueError("CSV файл пуст")
    
    # Подключаемся к БД (не readonly для записи)
    conn = sqlite3.connect(target_db.as_posix(), timeout=10)
    try:
        cur = conn.cursor()
        
        # Определяем типы данных для колонок
        dtype_map = {}
        for col in df.columns:
            col_data = df[col].dropna()
            if col_data.empty:
                dtype_map[col] = 'TEXT'
            elif pd.api.types.is_integer_dtype(col_data):
                dtype_map[col] = 'INTEGER'
            elif pd.api.types.is_float_dtype(col_data):
                dtype_map[col] = 'REAL'
            elif pd.api.types.is_datetime64_any_dtype(col_data):
                dtype_map[col] = 'TEXT'
            else:
                dtype_map[col] = 'TEXT'
        
        # Создаем таблицу
        columns_def = []
        for col in df.columns:
            safe_col_name = sanitize_table_name(col)
            col_type = dtype_map[col]
            columns_def.append(f'"{safe_col_name}" {col_type}')
        
        # Вставляем данные
        df.columns = [sanitize_table_name(col) for col in df.columns]
        df.to_sql(safe_table_name, conn, if_exists='replace', index=False)
        
        conn.commit()
    finally:
        conn.close()
    
    return safe_table_name, target_db

