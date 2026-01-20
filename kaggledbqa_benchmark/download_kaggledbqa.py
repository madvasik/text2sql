#!/usr/bin/env python3
"""
Скрипт для загрузки датасета KaggleDBQA.

Использование:
    python -m kaggledbqa_benchmark.download_kaggledbqa --output-dir ./kaggledbqa_data
    или
    python kaggledbqa_benchmark/download_kaggledbqa.py --output-dir ./kaggledbqa_data
"""

import argparse
import sys
from pathlib import Path
import urllib.request
import zipfile
import shutil


KAGGLEDBQA_URL = "https://github.com/Chia-Hsuan-Lee/KaggleDBQA/archive/refs/heads/main.zip"


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


def extract_kaggledbqa(zip_path: Path, output_dir: Path) -> None:
    """Распаковывает архив KaggleDBQA."""
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
        
        # Перемещаем содержимое из KaggleDBQA-main в output_dir
        extracted_dir = output_dir / root_dir
        if extracted_dir.exists():
            # Ищем директории с данными
            data_dirs = ["databases", "schemas", "examples"]
            
            for data_dir_name in data_dirs:
                source_dir = extracted_dir / data_dir_name
                if source_dir.exists():
                    target_dir = output_dir / data_dir_name
                    if target_dir.exists():
                        if target_dir.is_dir():
                            shutil.rmtree(target_dir)
                        else:
                            target_dir.unlink()
                    shutil.move(str(source_dir), str(output_dir))
            
            # Перемещаем JSON файлы с примерами
            for json_file in extracted_dir.glob("examples*.json"):
                target = output_dir / json_file.name
                if target.exists():
                    target.unlink()
                shutil.move(str(json_file), str(output_dir))
            
            # Перемещаем другие важные файлы
            for file_name in ["README.md", "tables.json"]:
                source_file = extracted_dir / file_name
                if source_file.exists():
                    target_file = output_dir / file_name
                    if target_file.exists():
                        target_file.unlink()
                    shutil.move(str(source_file), str(output_dir))
            
            # Удаляем пустую директорию
            try:
                shutil.rmtree(extracted_dir)
            except:
                pass
    
    print("Распаковка завершена")


def main():
    parser = argparse.ArgumentParser(
        description="Загрузка датасета KaggleDBQA"
    )
    
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./kaggledbqa_data",
        help="Директория для сохранения датасета",
    )
    
    parser.add_argument(
        "--url",
        type=str,
        default=KAGGLEDBQA_URL,
        help="URL для скачивания KaggleDBQA (по умолчанию GitHub)",
    )
    
    args = parser.parse_args()
    
    output_dir = Path(args.output_dir)
    zip_path = output_dir / "kaggledbqa.zip"
    
    print("=" * 60)
    print("ЗАГРУЗКА ДАТАСЕТА KAGGLEDBQA")
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
    extract_kaggledbqa(zip_path, output_dir)
    
    # Удаляем архив
    print(f"\nУдаление архива {zip_path}...")
    zip_path.unlink()
    
    print("\n" + "=" * 60)
    print("ГОТОВО!")
    print("=" * 60)
    print(f"\nДатасет сохранен в: {output_dir}")
    print(f"\nСтруктура:")
    print(f"  {output_dir}/")
    print(f"    ├── databases/          # SQLite базы данных")
    print(f"    ├── schemas/            # Схемы баз данных (JSON)")
    print(f"    ├── examples_plain.json # Примеры для plain-testing")
    print(f"    └── examples_fewshot.json # Примеры для few-shot режима")
    print(f"\nДля оценки модели используйте:")
    print(f"  python kaggledbqa_benchmark/evaluate_kaggledbqa.py --kaggledbqa-dir {output_dir} --split plain")


if __name__ == "__main__":
    main()
