"""
Модуль для выполнения SQL запросов на разных типах баз данных (SQLite, PostgreSQL, MySQL).
"""

import sqlite3
import re
from pathlib import Path
from typing import List, Tuple, Any, Optional, Set
import subprocess
import tempfile
import os


class DBExecutor:
    """Класс для выполнения SQL запросов на разных типах БД."""
    
    def __init__(self, db_path: Path, db_type: str = "sqlite"):
        """
        Args:
            db_path: Путь к файлу базы данных
            db_type: Тип БД ("sqlite", "postgresql", "mysql")
        """
        self.db_path = Path(db_path)
        self.db_type = db_type.lower()
        
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {db_path}")
    
    def execute(self, sql: str) -> Tuple[List[str], List[Tuple[Any, ...]]]:
        """
        Выполняет SQL запрос и возвращает результаты.
        
        Args:
            sql: SQL запрос (должен быть SELECT)
            
        Returns:
            Tuple[headers, rows] где headers - список названий колонок,
            rows - список кортежей со значениями
        """
        # Проверяем, что это SELECT запрос
        sql_clean = sql.strip().rstrip(";")
        if not sql_clean.lower().startswith("select"):
            raise ValueError("Only SELECT queries are allowed")
        
        if self.db_type == "sqlite":
            return self._execute_sqlite(sql_clean)
        elif self.db_type == "postgresql":
            return self._execute_postgresql(sql_clean)
        elif self.db_type == "mysql":
            return self._execute_mysql(sql_clean)
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")
    
    def _execute_sqlite(self, sql: str) -> Tuple[List[str], List[Tuple[Any, ...]]]:
        """Выполняет запрос на SQLite."""
        conn = sqlite3.connect(self.db_path.as_posix())
        try:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            
            cur.execute(sql)
            rows = cur.fetchall()
            
            headers = [desc[0] for desc in cur.description] if cur.description else []
            rows_tuples = [tuple(row) for row in rows]
            
            return headers, rows_tuples
        finally:
            conn.close()
    
    def _execute_postgresql(self, sql: str) -> Tuple[List[str], List[Tuple[Any, ...]]]:
        """
        Выполняет запрос на PostgreSQL.
        Требует установленного psql или Python библиотеки psycopg2.
        """
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            
            # Пытаемся подключиться к локальной БД PostgreSQL
            # Для BIRD может потребоваться восстановление из dump
            conn = psycopg2.connect(
                host="localhost",
                database="bird_db",
                user=os.getenv("PGUSER", "postgres"),
                password=os.getenv("PGPASSWORD", ""),
            )
            try:
                cur = conn.cursor(cursor_factory=RealDictCursor)
                cur.execute(sql)
                rows = cur.fetchall()
                
                headers = list(rows[0].keys()) if rows else []
                rows_tuples = [tuple(row.values()) for row in rows]
                
                return headers, rows_tuples
            finally:
                conn.close()
        except ImportError:
            raise ImportError("psycopg2 is required for PostgreSQL support. Install with: pip install psycopg2-binary")
        except Exception as e:
            # Fallback: используем SQLite если PostgreSQL недоступен
            print(f"Warning: PostgreSQL execution failed: {e}. Falling back to SQLite.")
            return self._execute_sqlite(sql)
    
    def _execute_mysql(self, sql: str) -> Tuple[List[str], List[Tuple[Any, ...]]]:
        """
        Выполняет запрос на MySQL.
        Требует установленной библиотеки mysql-connector-python или pymysql.
        """
        try:
            import mysql.connector
            
            conn = mysql.connector.connect(
                host="localhost",
                database="bird_db",
                user=os.getenv("MYSQL_USER", "root"),
                password=os.getenv("MYSQL_PASSWORD", ""),
            )
            try:
                cur = conn.cursor(dictionary=True)
                cur.execute(sql)
                rows = cur.fetchall()
                
                headers = list(rows[0].keys()) if rows else []
                rows_tuples = [tuple(row.values()) for row in rows]
                
                return headers, rows_tuples
            finally:
                conn.close()
        except ImportError:
            raise ImportError("mysql-connector-python is required for MySQL support. Install with: pip install mysql-connector-python")
        except Exception as e:
            # Fallback: используем SQLite если MySQL недоступен
            print(f"Warning: MySQL execution failed: {e}. Falling back to SQLite.")
            return self._execute_sqlite(sql)
    
    def execute_to_set(self, sql: str) -> Set[Tuple[Any, ...]]:
        """
        Выполняет SQL запрос и возвращает результаты как множество.
        Полезно для сравнения результатов без учета порядка.
        
        Args:
            sql: SQL запрос
            
        Returns:
            Множество кортежей результатов
        """
        _, rows = self.execute(sql)
        return set(rows)
    
    def compare_results(
        self,
        sql1: str,
        sql2: str,
        order_matters: bool = False,
    ) -> bool:
        """
        Сравнивает результаты двух SQL запросов.
        
        Args:
            sql1: Первый SQL запрос
            sql2: Второй SQL запрос
            order_matters: Если True, порядок строк имеет значение
            
        Returns:
            True если результаты совпадают, False иначе
        """
        try:
            headers1, rows1 = self.execute(sql1)
        except Exception:
            return False
        
        try:
            headers2, rows2 = self.execute(sql2)
        except Exception:
            return False
        
        # Проверяем заголовки
        if set(headers1) != set(headers2):
            return False
        
        # Проверяем данные
        if order_matters:
            return rows1 == rows2
        else:
            return set(rows1) == set(rows2)


def normalize_sql(sql: str) -> str:
    """
    Нормализует SQL запрос для сравнения.
    
    Удаляет:
    - Лишние пробелы
    - Комментарии
    - Приводит к нижнему регистру ключевые слова
    - Нормализует кавычки
    
    Args:
        sql: Исходный SQL запрос
        
    Returns:
        Нормализованный SQL запрос
    """
    if not sql:
        return ""
    
    # Удаляем комментарии (-- и /* */)
    sql = re.sub(r'--.*?$', '', sql, flags=re.MULTILINE)
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    
    # Заменяем множественные пробелы на один
    sql = re.sub(r'\s+', ' ', sql)
    
    # Удаляем пробелы вокруг операторов и скобок
    sql = re.sub(r'\s*([(),;])\s*', r'\1', sql)
    
    # Приводим к нижнему регистру ключевые слова SQL
    keywords = [
        'select', 'from', 'where', 'group', 'by', 'order', 'having',
        'join', 'inner', 'left', 'right', 'outer', 'on', 'as', 'and',
        'or', 'not', 'in', 'exists', 'union', 'intersect', 'except',
        'distinct', 'limit', 'offset', 'case', 'when', 'then', 'else',
        'end', 'is', 'null', 'like', 'between', 'asc', 'desc'
    ]
    
    for keyword in keywords:
        # Заменяем только целые слова
        pattern = r'\b' + re.escape(keyword) + r'\b'
        sql = re.sub(pattern, keyword.upper(), sql, flags=re.IGNORECASE)
    
    sql = sql.strip()
    
    return sql
