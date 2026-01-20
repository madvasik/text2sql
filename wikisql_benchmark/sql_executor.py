"""
Модуль для выполнения SQL запросов и сравнения результатов.
Переиспользует код из spider_benchmark/sql_executor.py
"""

from spider_benchmark.sql_executor import SQLExecutor, normalize_sql

__all__ = ["SQLExecutor", "normalize_sql"]
