"""
Модуль для выполнения SQL запросов и сравнения результатов.
"""

import sqlite3
import re
from pathlib import Path
from typing import List, Tuple, Any, Optional, Set
import pandas as pd


class SQLExecutor:
    """Класс для выполнения SQL запросов и сравнения результатов."""
    
    def __init__(self, db_path: Path):
        """
        Args:
            db_path: Путь к SQLite базе данных
        """
        self.db_path = Path(db_path)
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
        
        conn = sqlite3.connect(self.db_path.as_posix())
        try:
            # Устанавливаем режим, который позволяет сравнивать результаты
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            
            cur.execute(sql_clean)
            rows = cur.fetchall()
            
            # Преобразуем Row объекты в кортежи для сравнения
            headers = [desc[0] for desc in cur.description] if cur.description else []
            rows_tuples = [tuple(row) for row in rows]
            
            return headers, rows_tuples
        finally:
            conn.close()
    
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
            # Если первый запрос не выполнился, считаем что не совпадает
            return False
        
        try:
            headers2, rows2 = self.execute(sql2)
        except Exception:
            # Если второй запрос не выполнился, считаем что не совпадает
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
    
    # Нормализуем кавычки (заменяем двойные на одинарные для строк)
    # Это упрощенная версия, в реальности нужно быть осторожнее
    sql = sql.strip()
    
    return sql
