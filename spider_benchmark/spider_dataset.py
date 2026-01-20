"""
Модуль для загрузки и работы с датасетом Spider.
"""

import json
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class SpiderExample:
    """Один пример из датасета Spider."""
    question: str
    sql: str
    db_id: str
    question_id: Optional[str] = None


class SpiderDataset:
    """Класс для работы с датасетом Spider."""
    
    def __init__(self, data_dir: Path):
        """
        Args:
            data_dir: Путь к директории с датасетом Spider
                     (должна содержать train.json/dev.json/test.json и database/)
        """
        self.data_dir = Path(data_dir)
        self.database_dir = self.data_dir / "database"
        
        if not self.data_dir.exists():
            raise ValueError(f"Data directory does not exist: {data_dir}")
        
        if not self.database_dir.exists():
            raise ValueError(f"Database directory does not exist: {self.database_dir}")
    
    def load_examples(self, split: str = "dev") -> List[SpiderExample]:
        """
        Загружает примеры из указанного сплита.
        
        Args:
            split: "train", "dev" или "test"
            
        Returns:
            Список примеров SpiderExample
        """
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
            example = SpiderExample(
                question=item["question"],
                sql=item["sql"],
                db_id=item["db_id"],
                question_id=item.get("question_id"),
            )
            examples.append(example)
        
        return examples
    
    def get_database_path(self, db_id: str) -> Path:
        """
        Возвращает путь к файлу базы данных для указанного db_id.
        
        Args:
            db_id: Идентификатор базы данных
            
        Returns:
            Path к SQLite файлу
        """
        db_path = self.database_dir / db_id / f"{db_id}.sqlite"
        
        # Также проверяем альтернативные варианты названий
        if not db_path.exists():
            # Попробуем найти любой .sqlite файл в директории db_id
            db_dir = self.database_dir / db_id
            if db_dir.exists():
                sqlite_files = list(db_dir.glob("*.sqlite"))
                if sqlite_files:
                    return sqlite_files[0]
            
            # Попробуем найти в корне database_dir
            alt_path = self.database_dir / f"{db_id}.sqlite"
            if alt_path.exists():
                return alt_path
        
        if not db_path.exists():
            raise FileNotFoundError(
                f"Database file not found for db_id={db_id}. "
                f"Expected: {db_path}"
            )
        
        return db_path
    
    def list_databases(self) -> List[str]:
        """Возвращает список всех доступных db_id."""
        db_ids = []
        for item in self.database_dir.iterdir():
            if item.is_dir():
                db_ids.append(item.name)
        return sorted(db_ids)


def load_spider_dataset(data_dir: str) -> SpiderDataset:
    """
    Удобная функция для создания SpiderDataset.
    
    Args:
        data_dir: Путь к директории с датасетом Spider
        
    Returns:
        SpiderDataset instance
    """
    return SpiderDataset(Path(data_dir))
