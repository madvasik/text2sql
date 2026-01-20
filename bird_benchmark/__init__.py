"""
Модуль для оценки text2sql моделей на бенчмарке BIRD.
"""

from .bird_dataset import BirdDataset, load_bird_dataset
from .evaluator import BirdEvaluator, EvaluationResult
from .db_executor import DBExecutor, normalize_sql

__all__ = [
    "BirdDataset",
    "load_bird_dataset",
    "BirdEvaluator",
    "EvaluationResult",
    "DBExecutor",
    "normalize_sql",
]
