"""
Модуль для загрузки и работы с датасетом BIRD.
"""

import json
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class BirdExample:
    """Один пример из датасета BIRD."""
    question: str
    sql: str
    db_id: str
    question_id: Optional[str] = None
    evidence: Optional[str] = None  # Дополнительная информация для ответа
    difficulty: Optional[str] = None  # Уровень сложности
    db_path: Optional[str] = None  # Путь к БД (может быть указан в датасете)


class BirdDataset:
    """Класс для работы с датасетом BIRD."""
    
    def __init__(self, data_dir: Path, db_dir: Optional[Path] = None):
        """
        Args:
            data_dir: Путь к директории с датасетом BIRD
                     (должна содержать train.json/dev.json/test.json)
            db_dir: Путь к директории с базами данных (если None, ищется в data_dir/database)
        """
        self.data_dir = Path(data_dir)
        self.db_dir = db_dir or (self.data_dir / "database")
        
        if not self.data_dir.exists():
            raise ValueError(f"Data directory does not exist: {data_dir}")
    
    def load_examples(self, split: str = "dev") -> List[BirdExample]:
        """
        Загружает примеры из указанного сплита.
        
        Args:
            split: "train", "dev", "test" или "dev_mini" (Mini-Dev набор)
            
        Returns:
            Список примеров BirdExample
        """
        # Поддержка Mini-Dev набора
        if split == "dev_mini":
            json_file = self.data_dir / "dev_mini.json"
        else:
            json_file = self.data_dir / f"{split}.json"
        
        if not json_file.exists():
            raise FileNotFoundError(
                f"Split file not found: {json_file}. "
                f"Available files: {list(self.data_dir.glob('*.json'))}"
            )
        
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        examples = []
        for item in data:
            # BIRD использует "SQL" (заглавными), но поддерживаем оба варианта
            sql = item.get("SQL") or item.get("sql", "")
            example = BirdExample(
                question=item["question"],
                sql=sql,
                db_id=item["db_id"],
                question_id=item.get("question_id") or item.get("id"),
                evidence=item.get("evidence"),
                difficulty=item.get("difficulty"),
                db_path=item.get("db_path"),
            )
            examples.append(example)
        
        return examples
    
    def get_database_path(self, db_id: str) -> Path:
        """
        Возвращает путь к файлу базы данных для указанного db_id.
        
        Args:
            db_id: Идентификатор базы данных
            
        Returns:
            Path к файлу БД (SQLite, PostgreSQL dump, или MySQL dump)
        """
        # BIRD может использовать разные форматы БД
        # Сначала проверяем SQLite
        db_path = self.db_dir / db_id / f"{db_id}.sqlite"
        if db_path.exists():
            return db_path
        
        # Проверяем альтернативные варианты
        db_dir = self.db_dir / db_id
        if db_dir.exists():
            # Ищем SQLite файлы
            sqlite_files = list(db_dir.glob("*.sqlite"))
            if sqlite_files:
                return sqlite_files[0]
            
            # Ищем PostgreSQL dumps
            pg_dumps = list(db_dir.glob("*.sql"))
            if pg_dumps:
                return pg_dumps[0]
        
        # Проверяем в корне db_dir
        alt_path = self.db_dir / f"{db_id}.sqlite"
        if alt_path.exists():
            return alt_path
        
        raise FileNotFoundError(
            f"Database file not found for db_id={db_id}. "
            f"Expected in: {self.db_dir / db_id}"
        )
    
    def get_database_type(self, db_id: str) -> str:
        """
        Определяет тип базы данных по db_id.
        
        Args:
            db_id: Идентификатор базы данных
            
        Returns:
            Тип БД: "sqlite", "postgresql", "mysql"
        """
        db_path = self.get_database_path(db_id)
        
        if db_path.suffix == ".sqlite":
            return "sqlite"
        elif db_path.suffix == ".sql":
            # Может быть PostgreSQL или MySQL dump
            # Проверяем содержимое или используем метаданные
            return "postgresql"  # По умолчанию PostgreSQL
        else:
            return "sqlite"  # По умолчанию SQLite
    
    def list_databases(self) -> List[str]:
        """Возвращает список всех доступных db_id."""
        db_ids = []
        if self.db_dir.exists():
            for item in self.db_dir.iterdir():
                if item.is_dir():
                    db_ids.append(item.name)
        return sorted(db_ids)


def load_bird_dataset(data_dir: str, db_dir: Optional[str] = None) -> BirdDataset:
    """
    Удобная функция для создания BirdDataset.
    
    Args:
        data_dir: Путь к директории с датасетом BIRD
        db_dir: Путь к директории с базами данных (опционально)
        
    Returns:
        BirdDataset instance
    """
    return BirdDataset(Path(data_dir), Path(db_dir) if db_dir else None)
