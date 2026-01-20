"""
Модуль для загрузки и работы с датасетом WikiSQL.
"""

import json
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class WikiSQLExample:
    """Один пример из датасета WikiSQL."""
    question: str
    sql: Dict[str, Any]  # Структурированный SQL: {sel, agg, conds}
    table_id: str
    question_id: Optional[str] = None
    table: Optional[Dict[str, Any]] = None  # Таблица с headers, rows, types


class WikiSQLDataset:
    """Класс для работы с датасетом WikiSQL."""
    
    def __init__(self, data_dir: Path):
        """
        Args:
            data_dir: Путь к директории с датасетом WikiSQL
                     (должна содержать train.jsonl/dev.jsonl/test.jsonl и 
                      train.tables.jsonl/dev.tables.jsonl/test.tables.jsonl)
        """
        self.data_dir = Path(data_dir)
        
        if not self.data_dir.exists():
            raise ValueError(f"Data directory does not exist: {data_dir}")
    
    def load_examples(self, split: str = "dev") -> List[WikiSQLExample]:
        """
        Загружает примеры из указанного сплита.
        
        Args:
            split: "train", "dev" или "test"
            
        Returns:
            Список примеров WikiSQLExample
        """
        jsonl_file = self.data_dir / f"{split}.jsonl"
        tables_file = self.data_dir / f"{split}.tables.jsonl"
        
        if not jsonl_file.exists():
            raise FileNotFoundError(
                f"Split file not found: {jsonl_file}. "
                f"Available files: {list(self.data_dir.glob('*.jsonl'))}"
            )
        
        # Загружаем таблицы в память для быстрого доступа
        tables_dict = {}
        if tables_file.exists():
            with open(tables_file, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        table_data = json.loads(line)
                        tables_dict[table_data["id"]] = table_data
        
        # Загружаем примеры
        examples = []
        with open(jsonl_file, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                if not line.strip():
                    continue
                try:
                    item = json.loads(line)
                    example = WikiSQLExample(
                        question=item["question"],
                        sql=item["sql"],
                        table_id=item["table_id"],
                        question_id=item.get("question_id") or str(line_num),
                        table=tables_dict.get(item["table_id"]),
                    )
                    examples.append(example)
                except json.JSONDecodeError as e:
                    raise ValueError(f"Error parsing JSON at line {line_num} in {jsonl_file}: {e}")
        
        return examples
    
    def get_database_path(self, table_id: str) -> Optional[Path]:
        """
        Возвращает путь к файлу базы данных для указанного table_id.
        
        Args:
            table_id: Идентификатор таблицы
            
        Returns:
            Path к SQLite файлу или None если не найден
        """
        # WikiSQL может хранить таблицы в отдельных .db файлах
        # или в одном большом файле
        db_path = self.data_dir / f"{table_id}.db"
        if db_path.exists():
            return db_path
        
        # Также проверяем общий файл
        common_db = self.data_dir / "wikisql.db"
        if common_db.exists():
            return common_db
        
        return None
    
    def create_table_db(self, example: WikiSQLExample, db_path: Optional[Path] = None) -> Path:
        """
        Создает временную SQLite базу данных для таблицы из примера.
        
        Args:
            example: Пример из датасета
            db_path: Путь для сохранения БД (если None, создается временный файл)
            
        Returns:
            Path к созданной БД
        """
        import sqlite3
        import tempfile
        
        if example.table is None:
            raise ValueError(f"Table data not available for table_id={example.table_id}")
        
        if db_path is None:
            # Создаем временный файл
            temp_dir = self.data_dir / "temp_dbs"
            temp_dir.mkdir(exist_ok=True)
            db_path = temp_dir / f"{example.table_id}.db"
        
        db_path = Path(db_path)
        
        # Создаем БД и таблицу
        conn = sqlite3.connect(db_path.as_posix())
        try:
            cur = conn.cursor()
            
            # Получаем заголовки и типы
            headers = example.table["header"]
            types = example.table.get("types", ["text"] * len(headers))
            rows = example.table.get("rows", [])
            
            # Создаем таблицу
            columns = []
            for i, (header, col_type) in enumerate(zip(headers, types)):
                # Санитизируем имя колонки
                safe_header = header.replace('"', '""').replace("'", "''")
                sqlite_type = self._convert_type(col_type)
                columns.append(f'"{safe_header}" {sqlite_type}')
            
            create_sql = f'CREATE TABLE IF NOT EXISTS "table" ({", ".join(columns)})'
            cur.execute(create_sql)
            
            # Вставляем данные
            if rows:
                placeholders = ", ".join(["?"] * len(headers))
                insert_sql = f'INSERT INTO "table" VALUES ({placeholders})'
                cur.executemany(insert_sql, rows)
            
            conn.commit()
        finally:
            conn.close()
        
        return db_path
    
    def _convert_type(self, col_type: str) -> str:
        """Конвертирует тип WikiSQL в SQLite тип."""
        type_map = {
            "text": "TEXT",
            "number": "REAL",
            "real": "REAL",
            "integer": "INTEGER",
            "int": "INTEGER",
        }
        return type_map.get(col_type.lower(), "TEXT")
    
    def list_table_ids(self) -> List[str]:
        """Возвращает список всех доступных table_id."""
        table_ids = set()
        for jsonl_file in self.data_dir.glob("*.jsonl"):
            if ".tables.jsonl" in jsonl_file.name:
                continue
            with open(jsonl_file, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        try:
                            item = json.loads(line)
                            table_ids.add(item["table_id"])
                        except:
                            pass
        return sorted(table_ids)


def load_wikisql_dataset(data_dir: str) -> WikiSQLDataset:
    """
    Удобная функция для создания WikiSQLDataset.
    
    Args:
        data_dir: Путь к директории с датасетом WikiSQL
        
    Returns:
        WikiSQLDataset instance
    """
    return WikiSQLDataset(Path(data_dir))
