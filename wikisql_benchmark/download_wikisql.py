#!/usr/bin/env python3
"""
Скрипт для загрузки датасета WikiSQL.

Использование:
    python -m wikisql_benchmark.download_wikisql --output-dir ./wikisql_data
    или
    python wikisql_benchmark/download_wikisql.py --output-dir ./wikisql_data
"""

import argparse
import sys
from pathlib import Path
import urllib.request
import zipfile
import shutil


WIKISQL_URL = "https://github.com/salesforce/WikiSQL/archive/refs/heads/master.zip"


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


def extract_wikisql(zip_path: Path, output_dir: Path) -> None:
    """Распаковывает архив WikiSQL."""
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
        
        # Перемещаем содержимое из WikiSQL-master в output_dir
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
            
            # Удаляем пустую директорию WikiSQL-master
            try:
                shutil.rmtree(extracted_dir)
            except:
                pass
    
    print("Распаковка завершена")


def main():
    parser = argparse.ArgumentParser(
        description="Загрузка датасета WikiSQL"
    )
    
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./wikisql_data",
        help="Директория для сохранения датасета",
    )
    
    parser.add_argument(
        "--url",
        type=str,
        default=WIKISQL_URL,
        help="URL для скачивания WikiSQL (по умолчанию GitHub)",
    )
    
    args = parser.parse_args()
    
    output_dir = Path(args.output_dir)
    zip_path = output_dir / "wikisql.zip"
    
    print("=" * 60)
    print("ЗАГРУЗКА ДАТАСЕТА WIKISQL")
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
    extract_wikisql(zip_path, output_dir)
    
    # Удаляем архив
    print(f"\nУдаление архива {zip_path}...")
    zip_path.unlink()
    
    print("\n" + "=" * 60)
    print("ГОТОВО!")
    print("=" * 60)
    print(f"\nДатасет сохранен в: {output_dir}")
    print(f"\nДля оценки модели используйте:")
    print(f"  python wikisql_benchmark/evaluate_wikisql.py --wikisql-dir {output_dir} --split dev")


if __name__ == "__main__":
    main()
