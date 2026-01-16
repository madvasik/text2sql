from typing import Optional, List, Dict, Any
import json
import re
import os
from pathlib import Path

from .provider import get_provider, get_model_name
from .prompts import (
    SYSTEM_PROMPT,
    VIZ_SYSTEM_PROMPT,
    EXPLAIN_SQL_SYSTEM,
    EXPLAIN_RESULT_SYSTEM,
)
from ..db import list_tables_and_schema


# =================================================
# Helpers
# =================================================

_FORBIDDEN_SQL = re.compile(
    r"\b(insert|update|delete|drop|alter|create|truncate|replace)\b",
    re.IGNORECASE,
)


def _debug_set(key: str, value: str) -> None:
    if os.getenv("TEXT2SQL_DEBUG") == "1":
        os.environ[key] = value


def _validate_sql(sql: str) -> None:
    """
    Базовая защита от опасных запросов.
    """
    if not sql.lower().startswith("select"):
        raise ValueError("Only SELECT queries are allowed")

    if _FORBIDDEN_SQL.search(sql):
        raise ValueError("Dangerous SQL statement detected")


def _safe_json_loads(text: str) -> Dict[str, Any]:
    """
    Парсит JSON, даже если модель добавила текст вокруг.
    """
    try:
        return json.loads(text)
    except Exception:
        pass

    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        return json.loads(match.group(0))

    raise ValueError("Invalid JSON returned by model")


def _extract_sql(text: str) -> str:
    """
    Жёстко извлекает SELECT-запрос из ответа LLM
    (устойчиво к пояснениям, markdown и болтовне).
    """
    if not text or not text.strip():
        raise ValueError("Empty LLM response")

    original_text = text
    text = text.strip()

    # 1. Если есть ``` — берём самый длинный блок
    if "```" in text:
        parts = text.split("```")
        text = max(parts, key=len).strip()

    lower = text.lower()

    # 2. Ищем первый SELECT
    if "select" not in lower:
        _debug_set("TEXT2SQL_LAST_LLM_OUTPUT", original_text)
        raise ValueError("No SELECT statement found in LLM output")

    text = text[lower.index("select"):]

    # 3. Обрезаем всё после первого ;
    if ";" in text:
        text = text.split(";", 1)[0] + ";"

    sql = text.strip()

    if not sql.lower().startswith("select"):
        _debug_set("TEXT2SQL_LAST_LLM_OUTPUT", original_text)
        raise ValueError("Only SELECT queries are allowed")

    return sql


# =================================================
# Public API
# =================================================

def generate_sql_from_nl(
    question: str,
    db_path: Optional[Path] = None,
    schema_description: Optional[str] = None,
    model: Optional[str] = None,
) -> str:
    """
    Генерирует SQLite SELECT-запрос из вопроса на естественном языке.
    """
    schema = list_tables_and_schema(
        db_path=db_path,
        schema_description=schema_description,
    )

    provider = get_provider()
    model_name = get_model_name(model)

    user_prompt = (
        "Database schema (SQLite):\n"
        f"{schema}\n\n"
        f"Question: {question}\n"
        "Rules:\n"
        "- Return ONLY one SQLite SELECT statement\n"
        "- Do NOT explain anything\n"
        "- Do NOT use markdown\n"
    )

    try:
        text = provider.chat(
            system=SYSTEM_PROMPT,
            user=user_prompt,
            model=model_name,
        )

        _debug_set("TEXT2SQL_LAST_LLM_OUTPUT", text)

        sql = _extract_sql(text)
        _validate_sql(sql)

        return sql

    except Exception as e:
        _debug_set("TEXT2SQL_LAST_ERROR", str(e))
        raise


def decide_visualization(
    question: str,
    available_columns: List[str],
    model: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Определяет, нужен ли график и какой.
    """
    provider = get_provider()
    model_name = get_model_name(model)

    user_prompt = (
        f"Question: {question}\n"
        f"Available columns: {', '.join(available_columns)}\n"
        "Respond with JSON only."
    )

    try:
        text = provider.chat(
            system=VIZ_SYSTEM_PROMPT,
            user=user_prompt,
            model=model_name,
        )
        data = _safe_json_loads(text)

        return {
            "need_chart": bool(data.get("need_chart", False)),
            "chart_type": data.get("chart_type", "none"),
            "x_col": data.get("x_col"),
            "y_col": data.get("y_col"),
        }

    except Exception:
        return {
            "need_chart": False,
            "chart_type": "none",
            "x_col": None,
            "y_col": None,
        }


def explain_sql_brief(
    question: str,
    sql: str,
    model: Optional[str] = None,
) -> str:
    """
    Кратко объясняет, что делает SQL-запрос.
    """
    provider = get_provider()
    model_name = get_model_name(model)

    user_prompt = (
        f"Question: {question}\n"
        f"SQL:\n{sql}\n\n"
        "Explain briefly in 2-3 sentences (Russian)."
    )

    try:
        return provider.chat(
            system=EXPLAIN_SQL_SYSTEM,
            user=user_prompt,
            model=model_name,
        ).strip()
    except Exception:
        return ""


def summarize_result_brief(
    question: str,
    sql: str,
    preview_rows: List[Dict[str, Any]],
    schema_description: Optional[str] = None,
    model: Optional[str] = None,
) -> str:
    """
    Генерирует краткий аналитический вывод по результатам запроса.
    """
    provider = get_provider()
    model_name = get_model_name(model)

    payload: Dict[str, Any] = {
        "question": question,
        "sql": sql,
        "preview": preview_rows[:20],
    }

    if schema_description:
        payload["schema_description"] = schema_description

    try:
        return provider.chat(
            system=EXPLAIN_RESULT_SYSTEM,
            user=json.dumps(payload, ensure_ascii=False),
            model=model_name,
        ).strip()
    except Exception:
        return ""
