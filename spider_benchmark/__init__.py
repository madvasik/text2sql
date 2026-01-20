"""
Модуль для оценки text2sql моделей на бенчмарке Spider.
"""

from .spider_dataset import SpiderDataset, load_spider_dataset
from .evaluator import SpiderEvaluator, EvaluationResult
from .sql_executor import SQLExecutor, normalize_sql

__all__ = [
    "SpiderDataset",
    "load_spider_dataset",
    "SpiderEvaluator",
    "EvaluationResult",
    "SQLExecutor",
    "normalize_sql",
]
