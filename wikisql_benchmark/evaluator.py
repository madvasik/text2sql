"""
Модуль для оценки модели на датасете WikiSQL.
"""

import json
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict
from tqdm import tqdm

from .wikisql_dataset import WikiSQLDataset, WikiSQLExample
from .sql_executor import SQLExecutor, normalize_sql
from .sql_converter import wikisql_to_sql, sql_to_wikisql
from text2sql.llm import generate_sql_from_nl


@dataclass
class EvaluationResult:
    """Результат оценки одного примера."""
    question_id: Optional[str]
    table_id: str
    question: str
    gold_sql: str
    gold_sql_struct: Dict[str, Any]
    predicted_sql: str
    predicted_sql_struct: Optional[Dict[str, Any]]
    exact_match: bool
    execution_match: bool
    logical_form_match: bool
    error: Optional[str] = None


@dataclass
class EvaluationMetrics:
    """Метрики оценки на всем датасете."""
    total: int
    exact_match: int
    execution_match: int
    logical_form_match: int
    exact_match_rate: float
    execution_match_rate: float
    logical_form_match_rate: float
    errors: int
    error_rate: float


class WikiSQLEvaluator:
    """Класс для оценки модели на датасете WikiSQL."""
    
    def __init__(
        self,
        dataset: WikiSQLDataset,
        model: Optional[str] = None,
        max_examples: Optional[int] = None,
    ):
        """
        Args:
            dataset: Экземпляр WikiSQLDataset
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
                    "LF": f"{sum(r.logical_form_match for r in results)}/{len(results)}",
                })
        
        return results
    
    def _evaluate_single(self, example: WikiSQLExample) -> EvaluationResult:
        """
        Оценивает один пример.
        
        Args:
            example: Пример из датасета
            
        Returns:
            EvaluationResult
        """
        # Создаем временную БД для таблицы
        try:
            db_path = self.dataset.create_table_db(example)
        except Exception as e:
            return EvaluationResult(
                question_id=example.question_id,
                table_id=example.table_id,
                question=example.question,
                gold_sql="",
                gold_sql_struct=example.sql,
                predicted_sql="",
                predicted_sql_struct=None,
                exact_match=False,
                execution_match=False,
                logical_form_match=False,
                error=f"Failed to create DB: {e}",
            )
        
        # Конвертируем gold SQL в строку
        try:
            gold_sql = wikisql_to_sql(example.sql, example.table)
        except Exception as e:
            return EvaluationResult(
                question_id=example.question_id,
                table_id=example.table_id,
                question=example.question,
                gold_sql="",
                gold_sql_struct=example.sql,
                predicted_sql="",
                predicted_sql_struct=None,
                exact_match=False,
                execution_match=False,
                logical_form_match=False,
                error=f"Failed to convert gold SQL: {e}",
            )
        
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
                table_id=example.table_id,
                question=example.question,
                gold_sql=gold_sql,
                gold_sql_struct=example.sql,
                predicted_sql="",
                predicted_sql_struct=None,
                exact_match=False,
                execution_match=False,
                logical_form_match=False,
                error=str(e),
            )
        
        # Пытаемся конвертировать predicted SQL в структурированный формат
        predicted_sql_struct = None
        if example.table:
            predicted_sql_struct = sql_to_wikisql(predicted_sql, example.table)
        
        # Проверяем exact match (нормализованные SQL строки)
        gold_normalized = normalize_sql(gold_sql)
        pred_normalized = normalize_sql(predicted_sql)
        exact_match = gold_normalized == pred_normalized
        
        # Проверяем logical form match (структурированное сравнение)
        logical_form_match = False
        if predicted_sql_struct:
            logical_form_match = (
                predicted_sql_struct.get("sel") == example.sql.get("sel") and
                predicted_sql_struct.get("agg") == example.sql.get("agg") and
                self._compare_conditions(
                    predicted_sql_struct.get("conds", []),
                    example.sql.get("conds", [])
                )
            )
        
        # Проверяем execution match
        executor = SQLExecutor(db_path)
        execution_match = executor.compare_results(
            gold_sql,
            predicted_sql,
            order_matters=False,
        )
        
        return EvaluationResult(
            question_id=example.question_id,
            table_id=example.table_id,
            question=example.question,
            gold_sql=gold_sql,
            gold_sql_struct=example.sql,
            predicted_sql=predicted_sql,
            predicted_sql_struct=predicted_sql_struct,
            exact_match=exact_match,
            execution_match=execution_match,
            logical_form_match=logical_form_match,
        )
    
    def _compare_conditions(
        self,
        conds1: List[List[Any]],
        conds2: List[List[Any]]
    ) -> bool:
        """Сравнивает два списка условий."""
        if len(conds1) != len(conds2):
            return False
        
        # Сортируем условия для сравнения
        def normalize_cond(cond):
            if len(cond) >= 3:
                return tuple(cond[:3])
            return tuple(cond)
        
        conds1_norm = sorted([normalize_cond(c) for c in conds1])
        conds2_norm = sorted([normalize_cond(c) for c in conds2])
        
        return conds1_norm == conds2_norm
    
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
        logical_form_match = sum(1 for r in results if r.logical_form_match)
        errors = sum(1 for r in results if r.error is not None)
        
        return EvaluationMetrics(
            total=total,
            exact_match=exact_match,
            execution_match=execution_match,
            logical_form_match=logical_form_match,
            exact_match_rate=exact_match / total if total > 0 else 0.0,
            execution_match_rate=execution_match / total if total > 0 else 0.0,
            logical_form_match_rate=logical_form_match / total if total > 0 else 0.0,
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
