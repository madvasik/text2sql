"""
Модуль для загрузки и работы с датасетом KaggleDBQA.
"""

import json
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class KaggleDBQAExample:
    """Один пример из датасета KaggleDBQA."""
    question: str
    sql: str
    db_id: str
    question_id: Optional[str] = None
    db_documentation: Optional[str] = None  # Документация базы данных
    mode: Optional[str] = None  # "plain" или "fewshot"


class KaggleDBQADataset:
    """Класс для работы с датасетом KaggleDBQA."""
    
    def __init__(self, data_dir: Path):
        """
        Args:
            data_dir: Путь к директории с датасетом KaggleDBQA
                     (должна содержать examples_plain.json/examples_fewshot.json,
                      databases/, schemas/)
        """
        self.data_dir = Path(data_dir)
        self.database_dir = self.data_dir / "databases"
        self.schema_dir = self.data_dir / "schemas"
        
        if not self.data_dir.exists():
            raise ValueError(f"Data directory does not exist: {data_dir}")
    
    def load_examples(self, split: str = "plain", mode: str = "plain") -> List[KaggleDBQAExample]:
        """
        Загружает примеры из указанного сплита.
        
        Args:
            split: "plain" или "fewshot"
            mode: "plain" для plain-testing, "fewshot" для few-shot режима
            
        Returns:
            Список примеров KaggleDBQAExample
        """
        # KaggleDBQA использует examples_plain.json и examples_fewshot.json
        if split == "fewshot" or mode == "fewshot":
            json_file = self.data_dir / "examples_fewshot.json"
        else:
            json_file = self.data_dir / "examples_plain.json"
        
        # Также проверяем альтернативные варианты названий
        if not json_file.exists():
            alt_file = self.data_dir / f"examples_{split}.json"
            if alt_file.exists():
                json_file = alt_file
            else:
                raise FileNotFoundError(
                    f"Examples file not found: {json_file}. "
                    f"Available files: {list(self.data_dir.glob('*.json'))}"
                )
        
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        examples = []
        for item in data:
            # Загружаем документацию БД если доступна
            db_doc = None
            if self.schema_dir.exists():
                schema_file = self.schema_dir / f"{item['db_id']}.json"
                if schema_file.exists():
                    with open(schema_file, "r", encoding="utf-8") as sf:
                        schema_data = json.load(sf)
                        db_doc = schema_data.get("documentation") or schema_data.get("description")
            
            example = KaggleDBQAExample(
                question=item["question"],
                sql=item["SQL"] if "SQL" in item else item.get("sql", ""),
                db_id=item["db_id"],
                question_id=item.get("question_id") or item.get("id"),
                db_documentation=db_doc,
                mode=mode,
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
        db_path = self.database_dir / f"{db_id}.sqlite"
        
        # Также проверяем альтернативные варианты названий
        if not db_path.exists():
            # Попробуем найти любой .sqlite файл в директории db_id
            db_subdir = self.database_dir / db_id
            if db_subdir.exists():
                sqlite_files = list(db_subdir.glob("*.sqlite"))
                if sqlite_files:
                    return sqlite_files[0]
            
            # Попробуем найти в корне database_dir
            alt_path = self.database_dir / f"{db_id}.db"
            if alt_path.exists():
                return alt_path
        
        if not db_path.exists():
            raise FileNotFoundError(
                f"Database file not found for db_id={db_id}. "
                f"Expected: {db_path} or {self.database_dir / db_id / '*.sqlite'}"
            )
        
        return db_path
    
    def get_schema(self, db_id: str) -> Optional[Dict[str, Any]]:
        """
        Возвращает схему базы данных.
        
        Args:
            db_id: Идентификатор базы данных
            
        Returns:
            Словарь со схемой БД или None
        """
        schema_file = self.schema_dir / f"{db_id}.json"
        if not schema_file.exists():
            # Проверяем альтернативные варианты
            alt_file = self.schema_dir / "tables.json"
            if alt_file.exists():
                with open(alt_file, "r", encoding="utf-8") as f:
                    all_schemas = json.load(f)
                    for schema in all_schemas:
                        if schema.get("db_id") == db_id:
                            return schema
            return None
        
        with open(schema_file, "r", encoding="utf-8") as f:
            return json.load(f)
    
    def list_databases(self) -> List[str]:
        """Возвращает список всех доступных db_id."""
        db_ids = set()
        
        # Из примеров
        for json_file in [self.data_dir / "examples_plain.json", 
                          self.data_dir / "examples_fewshot.json"]:
            if json_file.exists():
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    for item in data:
                        db_ids.add(item["db_id"])
        
        # Из директории баз данных
        if self.database_dir.exists():
            for item in self.database_dir.iterdir():
                if item.is_file() and item.suffix in [".sqlite", ".db"]:
                    db_ids.add(item.stem)
                elif item.is_dir():
                    db_ids.add(item.name)
        
        return sorted(db_ids)


def load_kaggledbqa_dataset(data_dir: str) -> KaggleDBQADataset:
    """
    Удобная функция для создания KaggleDBQADataset.
    
    Args:
        data_dir: Путь к директории с датасетом KaggleDBQA
        
    Returns:
        KaggleDBQADataset instance
    """
    return KaggleDBQADataset(Path(data_dir))
