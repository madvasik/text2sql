"""
Модуль для оценки модели на датасете Spider.
"""

import json
from pathlib import Path
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass, asdict
from tqdm import tqdm

from .spider_dataset import SpiderDataset, SpiderExample
from .sql_executor import SQLExecutor, normalize_sql
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


class SpiderEvaluator:
    """Класс для оценки модели на датасете Spider."""
    
    def __init__(
        self,
        dataset: SpiderDataset,
        model: Optional[str] = None,
        max_examples: Optional[int] = None,
    ):
        """
        Args:
            dataset: Экземпляр SpiderDataset
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
            split: "train", "dev" или "test"
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
    
    def _evaluate_single(self, example: SpiderExample) -> EvaluationResult:
        """
        Оценивает один пример.
        
        Args:
            example: Пример из датасета
            
        Returns:
            EvaluationResult
        """
        db_path = self.dataset.get_database_path(example.db_id)
        
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
        
        # Проверяем execution match
        executor = SQLExecutor(db_path)
        execution_match = executor.compare_results(
            example.sql,
            predicted_sql,
            order_matters=False,
        )
        
        return EvaluationResult(
            question_id=example.question_id,
            db_id=example.db_id,
            question=example.question,
            gold_sql=example.sql,
            predicted_sql=predicted_sql,
            exact_match=exact_match,
            execution_match=execution_match,
        )
    
    def compute_metrics(self, results: List[EvaluationResult]) -> EvaluationMetrics:
        """
        Вычисляет метрики на основе результатов оценки.
        
        Args:
            results: Список результатов оценки
            
        Returns:
            EvaluationMetrics
        """
        total = len(results)
        exact_match = sum(1 for r in results if r.exact_match)
        execution_match = sum(1 for r in results if r.execution_match)
        errors = sum(1 for r in results if r.error is not None)
        
        return EvaluationMetrics(
            total=total,
            exact_match=exact_match,
            execution_match=execution_match,
            exact_match_rate=exact_match / total if total > 0 else 0.0,
            execution_match_rate=execution_match / total if total > 0 else 0.0,
            errors=errors,
            error_rate=errors / total if total > 0 else 0.0,
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
