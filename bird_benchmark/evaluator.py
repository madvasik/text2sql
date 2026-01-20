"""
Модуль для оценки модели на датасете BIRD.
Поддерживает метрики EX (Execution Accuracy) и VES (Valid Efficiency Score).
"""

import json
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict
from tqdm import tqdm
import time

from .bird_dataset import BirdDataset, BirdExample
from .db_executor import DBExecutor, normalize_sql
from text2sql.llm import generate_sql_from_nl


@dataclass
class EvaluationResult:
    """Результат оценки одного примера."""
    question_id: Optional[str]
    db_id: str
    question: str
    gold_sql: str
    predicted_sql: str
    exact_match: bool
    execution_match: bool
    execution_time: Optional[float] = None  # Время выполнения запроса
    error: Optional[str] = None


@dataclass
class EvaluationMetrics:
    """Метрики оценки на всем датасете."""
    total: int
    exact_match: int
    execution_match: int
    exact_match_rate: float
    execution_match_rate: float
    errors: int
    error_rate: float
    avg_execution_time: Optional[float] = None
    valid_efficiency_score: Optional[float] = None  # VES метрика


class BirdEvaluator:
    """Класс для оценки модели на датасете BIRD."""
    
    def __init__(
        self,
        dataset: BirdDataset,
        model: Optional[str] = None,
        max_examples: Optional[int] = None,
    ):
        """
        Args:
            dataset: Экземпляр BirdDataset
            model: Имя модели (если None, используется из окружения)
            max_examples: Максимальное количество примеров для оценки
        """
        self.dataset = dataset
        self.model = model
        self.max_examples = max_examples
    
    def evaluate(
        self,
        split: str = "dev",
        verbose: bool = True,
    ) -> List[EvaluationResult]:
        """
        Оценивает модель на указанном сплите.
        
        Args:
            split: "train", "dev", "test" или "dev_mini"
            verbose: Показывать прогресс-бар
            
        Returns:
            Список результатов оценки
        """
        examples = self.dataset.load_examples(split)
        
        if self.max_examples:
            examples = examples[:self.max_examples]
        
        results = []
        
        iterator = tqdm(examples, desc=f"Evaluating on {split}") if verbose else examples
        
        for example in iterator:
            result = self._evaluate_single(example)
            results.append(result)
            
            if verbose:
                iterator.set_postfix({
                    "EM": f"{sum(r.exact_match for r in results)}/{len(results)}",
                    "EX": f"{sum(r.execution_match for r in results)}/{len(results)}",
                })
        
        return results
    
    def _evaluate_single(self, example: BirdExample) -> EvaluationResult:
        """
        Оценивает один пример.
        
        Args:
            example: Пример из датасета
            
        Returns:
            EvaluationResult
        """
        db_path = self.dataset.get_database_path(example.db_id)
        db_type = self.dataset.get_database_type(example.db_id)
        
        # Генерируем SQL
        try:
            predicted_sql = generate_sql_from_nl(
                question=example.question,
                db_path=db_path,
                model=self.model,
            )
        except Exception as e:
            return EvaluationResult(
                question_id=example.question_id,
                db_id=example.db_id,
                question=example.question,
                gold_sql=example.sql,
                predicted_sql="",
                exact_match=False,
                execution_match=False,
                error=str(e),
            )
        
        # Проверяем exact match
        gold_normalized = normalize_sql(example.sql)
        pred_normalized = normalize_sql(predicted_sql)
        exact_match = gold_normalized == pred_normalized
        
        # Проверяем execution match и измеряем время выполнения
        executor = DBExecutor(db_path, db_type)
        execution_match = False
        execution_time = None
        
        try:
            start_time = time.time()
            execution_match = executor.compare_results(
                example.sql,
                predicted_sql,
                order_matters=False,
            )
            execution_time = time.time() - start_time
        except Exception as e:
            # Если выполнение не удалось, но SQL синтаксически правильный,
            # это может быть проблема с данными или схемой
            pass
        
        return EvaluationResult(
            question_id=example.question_id,
            db_id=example.db_id,
            question=example.question,
            gold_sql=example.sql,
            predicted_sql=predicted_sql,
            exact_match=exact_match,
            execution_match=execution_match,
            execution_time=execution_time,
        )
    
    def compute_metrics(self, results: List[EvaluationResult]) -> EvaluationMetrics:
        """
        Вычисляет метрики на основе результатов оценки.
        Включает EX (Execution Accuracy) и VES (Valid Efficiency Score).
        
        Args:
            results: Список результатов оценки
            
        Returns:
            EvaluationMetrics
        """
        total = len(results)
        exact_match = sum(1 for r in results if r.exact_match)
        execution_match = sum(1 for r in results if r.execution_match)
        errors = sum(1 for r in results if r.error is not None)
        
        # Вычисляем среднее время выполнения для успешных запросов
        execution_times = [r.execution_time for r in results if r.execution_time is not None]
        avg_execution_time = sum(execution_times) / len(execution_times) if execution_times else None
        
        # VES (Valid Efficiency Score) - метрика эффективности SQL
        # Упрощенная версия: учитывает только успешные запросы с разумным временем выполнения
        valid_efficiency_score = None
        if execution_times:
            # Считаем долю запросов, выполненных за разумное время (например, < 10 секунд)
            reasonable_time_threshold = 10.0
            efficient_count = sum(1 for t in execution_times if t < reasonable_time_threshold)
            valid_efficiency_score = efficient_count / len(execution_times)
        
        return EvaluationMetrics(
            total=total,
            exact_match=exact_match,
            execution_match=execution_match,
            exact_match_rate=exact_match / total if total > 0 else 0.0,
            execution_match_rate=execution_match / total if total > 0 else 0.0,
            errors=errors,
            error_rate=errors / total if total > 0 else 0.0,
            avg_execution_time=avg_execution_time,
            valid_efficiency_score=valid_efficiency_score,
        )
    
    def save_results(
        self,
        results: List[EvaluationResult],
        output_path: Path,
    ) -> None:
        """
        Сохраняет результаты оценки в JSON файл.
        
        Args:
            results: Список результатов оценки
            output_path: Путь к выходному файлу
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        data = {
            "results": [asdict(r) for r in results],
            "metrics": asdict(self.compute_metrics(results)),
        }
        
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
