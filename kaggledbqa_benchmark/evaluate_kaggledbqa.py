#!/usr/bin/env python3
"""
Скрипт для оценки модели на бенчмарке KaggleDBQA.

Использование:
    python -m kaggledbqa_benchmark.evaluate_kaggledbqa --kaggledbqa-dir /path/to/kaggledbqa --split plain --model qwen3-coder:30b
    или
    python kaggledbqa_benchmark/evaluate_kaggledbqa.py --kaggledbqa-dir /path/to/kaggledbqa --split plain --model qwen3-coder:30b
"""

import argparse
import os
import sys
from pathlib import Path

from kaggledbqa_benchmark import (
    load_kaggledbqa_dataset,
    KaggleDBQAEvaluator,
)


def main():
    parser = argparse.ArgumentParser(
        description="Оценка text2sql модели на бенчмарке KaggleDBQA"
    )
    
    parser.add_argument(
        "--kaggledbqa-dir",
        type=str,
        required=True,
        help="Путь к директории с датасетом KaggleDBQA (должна содержать examples_plain.json/examples_fewshot.json, databases/, schemas/)",
    )
    
    parser.add_argument(
        "--split",
        type=str,
        default="plain",
        choices=["plain", "fewshot"],
        help="Сплит для оценки (plain/fewshot)",
    )
    
    parser.add_argument(
        "--mode",
        type=str,
        default=None,
        choices=["plain", "fewshot"],
        help="Режим оценки (plain/fewshot). Если не указан, используется значение --split",
    )
    
    parser.add_argument(
        "--model",
        type=str,
        default=None,
        help="Имя модели (если не указано, используется из LLM_MODEL)",
    )
    
    parser.add_argument(
        "--provider",
        type=str,
        default="ollama",
        choices=["ollama", "mistral"],
        help="LLM провайдер (ollama/mistral)",
    )
    
    parser.add_argument(
        "--max-examples",
        type=int,
        default=None,
        help="Максимальное количество примеров для оценки (для тестирования)",
    )
    
    parser.add_argument(
        "--no-documentation",
        action="store_true",
        help="Не использовать документацию БД при генерации SQL",
    )
    
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Путь к файлу для сохранения результатов (JSON)",
    )
    
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Подробный вывод",
    )
    
    args = parser.parse_args()
    
    # Устанавливаем провайдер
    os.environ["LLM_PROVIDER"] = args.provider
    if args.model:
        os.environ["LLM_MODEL"] = args.model
    
    # Определяем режим
    mode = args.mode or args.split
    
    # Загружаем датасет
    print(f"Загрузка датасета KaggleDBQA из {args.kaggledbqa_dir}...")
    try:
        dataset = load_kaggledbqa_dataset(args.kaggledbqa_dir)
    except Exception as e:
        print(f"Ошибка загрузки датасета: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Создаем evaluator
    evaluator = KaggleDBQAEvaluator(
        dataset=dataset,
        model=args.model,
        max_examples=args.max_examples,
        use_documentation=not args.no_documentation,
    )
    
    # Выполняем оценку
    print(f"\nОценка на сплите '{args.split}' (режим: {mode})...")
    print(f"Модель: {args.model or os.getenv('LLM_MODEL', 'default')}")
    print(f"Провайдер: {args.provider}")
    print(f"Использование документации БД: {not args.no_documentation}")
    if args.max_examples:
        print(f"Ограничение: {args.max_examples} примеров")
    print()
    
    try:
        results = evaluator.evaluate(split=args.split, mode=mode, verbose=True)
    except KeyboardInterrupt:
        print("\n\nОценка прервана пользователем", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\nОшибка во время оценки: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    # Вычисляем метрики
    metrics = evaluator.compute_metrics(results)
    
    # Выводим результаты
    print("\n" + "=" * 60)
    print("РЕЗУЛЬТАТЫ ОЦЕНКИ")
    print("=" * 60)
    print(f"Всего примеров:        {metrics.total}")
    print(f"Exact Match:            {metrics.exact_match} ({metrics.exact_match_rate:.2%})")
    print(f"Execution Match:        {metrics.execution_match} ({metrics.execution_match_rate:.2%})")
    print(f"Ошибки:                 {metrics.errors} ({metrics.error_rate:.2%})")
    print("=" * 60)
    
    # Сохраняем результаты
    if args.output:
        output_path = Path(args.output)
        print(f"\nСохранение результатов в {output_path}...")
        evaluator.save_results(results, output_path)
        print("Готово!")
    
    # Выводим примеры ошибок, если есть
    if args.verbose and metrics.errors > 0:
        print("\nПримеры ошибок:")
        error_examples = [r for r in results if r.error is not None][:5]
        for i, result in enumerate(error_examples, 1):
            print(f"\n{i}. DB: {result.db_id}")
            print(f"   Вопрос: {result.question[:100]}...")
            print(f"   Ошибка: {result.error}")
    
    # Выводим примеры несовпадений
    if args.verbose:
        failed_examples = [
            r for r in results
            if not r.execution_match and r.error is None
        ][:5]
        
        if failed_examples:
            print("\nПримеры несовпадений (execution):")
            for i, result in enumerate(failed_examples, 1):
                print(f"\n{i}. DB: {result.db_id}")
                print(f"   Вопрос: {result.question[:100]}...")
                print(f"   Gold SQL:   {result.gold_sql[:150]}...")
                print(f"   Pred SQL:   {result.predicted_sql[:150]}...")


if __name__ == "__main__":
    main()
