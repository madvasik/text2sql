import os
from typing import Optional, List, Dict, Any
import json

from mistralai.client import MistralClient
from mistralai.models.chat_completion import ChatMessage

from .db import list_tables_and_schema


SYSTEM_PROMPT = (
    "You are an expert SQLite SQL generator. "
    "Given a question and the database schema, generate a single SQLite-compatible SELECT query that answers the question. "
    "Follow rules: 1) Output only SQL without backticks or commentary. 2) Use only available tables/columns. 3) Prefer explicit column names. 4) LIMIT 50 by default."
)


def get_mistral_client() -> MistralClient:
    api_key = os.getenv("MISTRAL_API_KEY")
    if not api_key:
        raise RuntimeError(
            "MISTRAL_API_KEY is not set. Please set it in your environment or .env file."
        )
    return MistralClient(api_key=api_key)


def generate_sql_from_nl(question: str, model: str = "mistral-small-latest") -> str:
    schema = list_tables_and_schema()
    client = get_mistral_client()

    messages = [
        ChatMessage(role="system", content=SYSTEM_PROMPT),
        ChatMessage(
            role="user",
            content=(
                "Database schema (SQLite):\n" + schema + "\n\n" +
                "Question: " + question + "\n" +
                "Return only a single SELECT statement (SQLite)."
            ),
        ),
    ]

    # Try the requested model first, then a couple of fallbacks if rate-limited or unavailable
    candidate_models = [model, "mistral-small-latest", "open-mistral-7b"]
    last_err: Exception | None = None
    for m in candidate_models:
        try:
            resp = client.chat(model=m, messages=messages, temperature=0.0)
            text = resp.choices[0].message.content.strip()
            break
        except Exception as e:
            last_err = e
            continue
    else:
        raise last_err if last_err else RuntimeError("Failed to generate SQL")
    # Remove code fences if any
    if text.startswith("```"):
        text = text.strip("`")
        # if language tag present, try to split
        parts = text.split("\n", 1)
        text = parts[1] if len(parts) > 1 else text
    # Strip trailing semicolon to be permissive; executor will check
    return text.strip()


VIZ_SYSTEM_PROMPT = (
    "You determine if a visualization (chart) is useful for answering a given analytics question over tabular SQL results. "
    "Only answer with strict JSON. Fields: need_chart (true/false), chart_type ('bar'|'line'|'pie'|'none'), "
    "x_col (string|null), y_col (string|null). Prefer bar for top/aggregation by category, line for time series."
)


def decide_visualization(
    question: str,
    available_columns: List[str],
    model: str = "mistral-small-latest",
) -> Dict[str, Any]:
    client = get_mistral_client()
    user = (
        "Question: " + question + "\n" +
        "Available columns: " + ", ".join(available_columns) + "\n" +
        "Respond JSON only."
    )
    messages = [
        ChatMessage(role="system", content=VIZ_SYSTEM_PROMPT),
        ChatMessage(role="user", content=user),
    ]
    try:
        resp = client.chat(model=model, messages=messages, temperature=0.0)
        txt = resp.choices[0].message.content.strip()
        data = json.loads(txt)
        # Basic validation
        if not isinstance(data, dict):
            raise ValueError("bad json")
        data.setdefault("need_chart", False)
        data.setdefault("chart_type", "none")
        data.setdefault("x_col", None)
        data.setdefault("y_col", None)
        return data
    except Exception:
        return {"need_chart": False, "chart_type": "none", "x_col": None, "y_col": None}


EXPLAIN_SQL_SYSTEM = (
    "You explain in 2-3 concise sentences what an SQL query does."
)


def explain_sql_brief(question: str, sql: str, model: str = "mistral-small-latest") -> str:
    client = get_mistral_client()
    messages = [
        ChatMessage(role="system", content=EXPLAIN_SQL_SYSTEM),
        ChatMessage(
            role="user",
            content=(
                "Question: " + question + "\n" +
                "SQL:\n" + sql + "\n\n" +
                "Explain briefly in 2-3 sentences (Russian)."
            ),
        ),
    ]
    try:
        resp = client.chat(model=model, messages=messages, temperature=0.0)
        return resp.choices[0].message.content.strip()
    except Exception:
        return ""


EXPLAIN_RESULT_SYSTEM = (
    "You summarize tabular query results. Given question, SQL and a small sample of rows, write a brief 2-3 sentence insight in Russian."
)


def summarize_result_brief(
    question: str,
    sql: str,
    preview_rows: List[Dict[str, Any]],
    model: str = "mistral-small-latest",
) -> str:
    client = get_mistral_client()
    payload = {
        "question": question,
        "sql": sql,
        "preview": preview_rows[:20],
    }
    messages = [
        ChatMessage(role="system", content=EXPLAIN_RESULT_SYSTEM),
        ChatMessage(role="user", content=json.dumps(payload, ensure_ascii=False)),
    ]
    try:
        resp = client.chat(model=model, messages=messages, temperature=0.0)
        return resp.choices[0].message.content.strip()
    except Exception:
        return ""

