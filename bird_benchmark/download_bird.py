#!/usr/bin/env python3
"""
Скрипт для загрузки датасета BIRD.

Использование:
    python -m bird_benchmark.download_bird --output-dir ./bird_data
    или
    python bird_benchmark/download_bird.py --output-dir ./bird_data
"""

import argparse
import sys
from pathlib import Path
import urllib.request
import zipfile
import shutil
import os
import json


BIRD_DATA_URL = "https://github.com/bird-bench/bird-bench/archive/refs/heads/main.zip"


def download_file(url: str, output_path: Path) -> None:
    """Скачивает файл по URL."""
    print(f"Скачивание {url}...")
    print(f"Сохранение в {output_path}...")
    
    def show_progress(block_num, block_size, total_size):
        downloaded = block_num * block_size
        percent = min(100, (downloaded * 100) / total_size) if total_size > 0 else 0
        print(f"\rПрогресс: {percent:.1f}%", end="", flush=True)
    
    try:
        urllib.request.urlretrieve(url, output_path, reporthook=show_progress)
        print()  # Новая строка после прогресс-бара
    except Exception as e:
        print(f"\nОшибка при скачивании: {e}", file=sys.stderr)
        sys.exit(1)


def extract_bird(zip_path: Path, output_dir: Path) -> None:
    """Распаковывает архив BIRD."""
    print(f"\nРаспаковка архива...")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        # Находим корневую директорию в архиве
        members = zip_ref.namelist()
        root_dir = members[0].split('/')[0] if members else None
        
        if not root_dir:
            print("Ошибка: не удалось определить корневую директорию архива", file=sys.stderr)
            sys.exit(1)
        
        # Извлекаем все файлы
        zip_ref.extractall(output_dir)
        
        # Перемещаем содержимое из bird-bench-main в output_dir
        extracted_dir = output_dir / root_dir
        if extracted_dir.exists():
            # Ищем директорию data
            data_dir = extracted_dir / "data"
            if data_dir.exists():
                # Перемещаем файлы из data в output_dir
                for item in data_dir.iterdir():
                    target = output_dir / item.name
                    if target.exists():
                        if target.is_dir():
                            shutil.rmtree(target)
                        else:
                            target.unlink()
                    shutil.move(str(item), str(output_dir))
            
            # Удаляем пустую директорию
            try:
                shutil.rmtree(extracted_dir)
            except:
                pass
    
    print("Распаковка завершена")


def download_mini_dev_hf(output_dir: Path) -> bool:
    """
    Пытается загрузить Mini-Dev набор через Hugging Face datasets.
    
    Returns:
        True если загрузка успешна, False иначе
    """
    try:
        from datasets import load_dataset
        
        print("\nПопытка загрузки Mini-Dev через Hugging Face...")
        dataset = load_dataset("birdsql/bird_mini_dev", trust_remote_code=True)
        
        mini_dev_dir = output_dir / "dev_mini"
        mini_dev_dir.mkdir(exist_ok=True)
        
        # Сохраняем данные в JSON формат
        if "dev" in dataset:
            dev_data = dataset["dev"]
            dev_file = mini_dev_dir / "dev_mini.json"
            
            examples = []
            for item in dev_data:
                examples.append({
                    "question": item.get("question", ""),
                    "SQL": item.get("SQL", ""),
                    "db_id": item.get("db_id", ""),
                    "question_id": item.get("question_id"),
                    "evidence": item.get("evidence"),
                    "difficulty": item.get("difficulty"),
                })
            
            with open(dev_file, "w", encoding="utf-8") as f:
                json.dump(examples, f, ensure_ascii=False, indent=2)
            
            print(f"Mini-Dev набор сохранен в: {dev_file}")
            print(f"Загружено примеров: {len(examples)}")
            return True
        else:
            print("Предупреждение: раздел 'dev' не найден в датасете")
            return False
            
    except ImportError:
        print("\nБиблиотека 'datasets' не установлена.")
        print("Для загрузки Mini-Dev через Hugging Face установите:")
        print("  pip install datasets")
        return False
    except Exception as e:
        print(f"\nОшибка при загрузке через Hugging Face: {e}")
        print("Продолжаем с ручной загрузкой...")
        return False


def download_databases(db_dir: Path) -> None:
    """
    Выводит инструкции по загрузке баз данных BIRD.
    Базы данных BIRD очень большие (~33GB) и должны быть загружены отдельно
    с официального сайта или через Hugging Face.
    """
    print("\n" + "=" * 60)
    print("ИНСТРУКЦИИ ПО ЗАГРУЗКЕ БАЗ ДАННЫХ BIRD")
    print("=" * 60)
    print("\nБазы данных BIRD очень большие (~33GB) и загружаются отдельно.")
    print("\nВарианты загрузки:")
    print("\n1. Официальный сайт:")
    print("   https://bird-bench.github.io/")
    print("   Скачайте базы данных и распакуйте в:", db_dir)
    print("\n2. Hugging Face (для Mini-Dev набора):")
    print("   pip install datasets")
    print("   python -c \"from datasets import load_dataset; load_dataset('birdsql/bird_mini_dev')\"")
    print("\n3. GitHub репозиторий:")
    print("   https://github.com/bird-bench/bird-bench")
    print("   Следуйте инструкциям в README для загрузки БД")
    print("\nПосле загрузки структура должна быть:")
    print(f"  {db_dir}/")
    print("    ├── db_id_1/")
    print("    │   └── db_id_1.sqlite")
    print("    ├── db_id_2/")
    print("    │   └── db_id_2.sqlite")
    print("    └── ...")
    print("\nДля быстрого тестирования используйте Mini-Dev набор:")
    print("  python bird_benchmark/download_bird.py --mini-dev-only")


def main():
    parser = argparse.ArgumentParser(
        description="Загрузка датасета BIRD"
    )
    
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./bird_data",
        help="Директория для сохранения датасета",
    )
    
    parser.add_argument(
        "--url",
        type=str,
        default=BIRD_DATA_URL,
        help="URL для скачивания BIRD (по умолчанию GitHub)",
    )
    
    parser.add_argument(
        "--skip-db",
        action="store_true",
        help="Пропустить загрузку баз данных (они очень большие)",
    )
    
    parser.add_argument(
        "--mini-dev-only",
        action="store_true",
        help="Загрузить только Mini-Dev набор (для быстрого тестирования). "
             "Использует Hugging Face datasets API если доступно.",
    )
    
    args = parser.parse_args()
    
    output_dir = Path(args.output_dir)
    zip_path = output_dir / "bird.zip"
    
    print("=" * 60)
    print("ЗАГРУЗКА ДАТАСЕТА BIRD")
    print("=" * 60)
    
    # Скачиваем архив
    if not zip_path.exists():
        download_file(args.url, zip_path)
    else:
        print(f"Архив уже существует: {zip_path}")
        response = input("Перезаписать? (y/N): ")
        if response.lower() != 'y':
            print("Пропуск скачивания")
        else:
            download_file(args.url, zip_path)
    
    # Распаковываем
    extract_bird(zip_path, output_dir)
    
    # Удаляем архив
    print(f"\nУдаление архива {zip_path}...")
    zip_path.unlink()
    
    # Загружаем Mini-Dev через Hugging Face если запрошено
    if args.mini_dev_only:
        success = download_mini_dev_hf(output_dir)
        if not success:
            print("\nMini-Dev набор не загружен через Hugging Face.")
            print("Пожалуйста, загрузите его вручную с официального сайта:")
            print("  https://github.com/bird-bench/mini_dev")
    
    # Загружаем базы данных (если не пропущено)
    if not args.skip_db and not args.mini_dev_only:
        db_dir = output_dir / "database"
        db_dir.mkdir(exist_ok=True)
        download_databases(db_dir)
    
    print("\n" + "=" * 60)
    print("ГОТОВО!")
    print("=" * 60)
    print(f"\nДатасет сохранен в: {output_dir}")
    print(f"\nДля оценки модели используйте:")
    print(f"  python bird_benchmark/evaluate_bird.py --bird-dir {output_dir} --split dev")
    if args.mini_dev_only:
        print(f"\nДля быстрого тестирования используйте Mini-Dev:")
        print(f"  python bird_benchmark/evaluate_bird.py --bird-dir {output_dir} --split dev_mini")


if __name__ == "__main__":
    main()
