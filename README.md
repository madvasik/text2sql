# Text-to-SQL (SQLite) — Mistral SDK

NL-вопрос → SQL (SQLite) → результат.  
CLI и Streamlit UI. БД (`data/example.db`) создаётся и наполняется автоматически.

## Функционал
- Генерация `SELECT` из естественного языка (Mistral API)
- Безопасное выполнение (read-only SQLite)
- Автосоздание демо-БД: сотрудники, отделы, проекты, ревью
- Streamlit UI: ввод API-ключа, таблица, экспорт CSV/XLSX
- Краткие пояснения SQL и результатов


## Установка
```cmd
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Запуск (Streamlit)
```cmd
.venv\Scripts\python.exe -m streamlit run streamlit_app.py
```

## Пересоздание БД
```cmd
del data\example.db
.venv\Scripts\python.exe -m streamlit run streamlit_app.py
```

## Экспорт
- CSV — встроено  
- XLSX — `pip install openpyxl==3.1.5`

## Структура
```
text2sql/
  db.py        # создание/сидинг БД, SELECT
  llm.py       # Mistral API: SQL, пояснения
  cli.py, __main__.py
streamlit_app.py
data/example.db
requirements.txt
```