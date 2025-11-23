# Text-to-SQL (SQLite) — Mistral SDK

Генерация SQL запросов из естественного языка через Mistral API для SQLite.

## Реализовано

### Основной функционал
- Генерация `SELECT` запросов из NL через Mistral API (модели: mistral-small-latest, open-mistral-7b с fallback)
- Read-only выполнение SQL (только SELECT, защита от инъекций)
- Импорт CSV в SQLite с автоопределением типов данных
- Поддержка TXT файлов с описанием схемы таблицы для улучшения генерации

### Дополнительные фичи
- Автоматическая визуализация результатов (столбчатые, линейные, круговые графики)
- Генерация пояснений SQL и результатов через LLM
- Экспорт результатов: CSV, XLSX, PNG графики
- Валидация API ключа перед использованием
- Автоопределение колонок для графиков

## Быстрый старт

```cmd
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

**Запуск:**
```cmd
streamlit run streamlit_app.py
```

## Структура
- `text2sql/db.py` — работа с SQLite, импорт CSV, выполнение SELECT
- `text2sql/llm.py` — Mistral API: генерация SQL, пояснения, визуализация
- `streamlit_app.py` — Streamlit веб-интерфейс