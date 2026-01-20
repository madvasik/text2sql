"""
Модуль для оценки text2sql моделей на бенчмарке WikiSQL.
"""

from .wikisql_dataset import WikiSQLDataset, load_wikisql_dataset
from .evaluator import WikiSQLEvaluator, EvaluationResult
from .sql_executor import SQLExecutor, normalize_sql

__all__ = [
    "WikiSQLDataset",
    "load_wikisql_dataset",
    "WikiSQLEvaluator",
    "EvaluationResult",
    "SQLExecutor",
    "normalize_sql",
]
