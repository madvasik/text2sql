import os
import sqlite3
from pathlib import Path

import streamlit as st
import pandas as pd

from text2sql.db import list_tables_and_schema
from text2sql.llm import (
    generate_sql_from_nl,
    decide_visualization,
    explain_sql_brief,
    summarize_result_brief,
)

# -------------------------------------------------
# Utils
# -------------------------------------------------

def check_ollama_status() -> bool:
    try:
        import ollama
        ollama.chat(
            model=os.getenv("LLM_MODEL", "qwen3-coder:30b"),
            messages=[{"role": "user", "content": "ping"}],
            options={"temperature": 0},
        )
        return True
    except Exception:
        return False


def run_sql(db_path: Path, sql: str) -> pd.DataFrame:
    with sqlite3.connect(db_path) as conn:
        return pd.read_sql_query(sql, conn)


# -------------------------------------------------
# Streamlit config
# -------------------------------------------------

st.set_page_config(
    page_title="Text-to-SQL (Ollama)",
    layout="wide",
)

st.title("📊 Text → SQL аналитика (Ollama)")

# -------------------------------------------------
# Sidebar
# -------------------------------------------------

st.sidebar.header("Настройки")

# --- LLM status ---
st.sidebar.subheader("LLM")

# @st.cache_data(ttl=30)
@st.cache_data(show_spinner=False)
def cached_ollama_check():
    return check_ollama_status()

if cached_ollama_check():
    st.sidebar.success("Ollama подключен")
else:
    st.sidebar.error("Ollama недоступен")
    st.sidebar.caption("Запустите: ollama serve")
    st.stop()

# --- Model selection ---
model_name = st.sidebar.selectbox(
    "Модель",
    ["qwen3-coder:30b", "solar"],
    index=0,
)

os.environ["LLM_PROVIDER"] = "ollama"
os.environ["LLM_MODEL"] = model_name

st.sidebar.caption(f"Используется модель: {model_name}")


# --- Database selection ---
st.sidebar.subheader("База данных")

db_path_input = st.sidebar.text_input(
    "Путь к SQLite файлу",
    value="data.db",
)

db_path = Path(db_path_input)

if not db_path.exists():
    st.sidebar.warning("Файл БД не найден")
    st.stop()

# -------------------------------------------------
# Main UI (FORM!)
# -------------------------------------------------

st.subheader("Вопрос на естественном языке")

with st.form("query_form"):
    question = st.text_area(
        "Введите аналитический вопрос",
        height=120,
        placeholder="Например: Все работники из города Lethbridge",
    )
    submitted = st.form_submit_button("🚀 Сгенерировать SQL")

if not submitted:
    st.info("Введите вопрос и нажмите Ctrl+Enter или кнопку")
    st.stop()

if not question.strip():
    st.warning("Вопрос пустой")
    st.stop()

# -------------------------------------------------
# Generate SQL
# -------------------------------------------------

with st.spinner("Генерирую SQL…"):
    try:
        sql = generate_sql_from_nl(
            question=question,
            db_path=db_path,
        )
    except Exception as e:
        st.error(f"Ошибка генерации SQL: {e}")
        st.stop()

st.subheader("Сгенерированный SQL")
st.code(sql, language="sql")

# -------------------------------------------------
# Execute SQL
# -------------------------------------------------

with st.spinner("Выполняю запрос…"):
    try:
        df = run_sql(db_path, sql)
    except Exception as e:
        st.error(f"Ошибка выполнения SQL: {e}")
        st.stop()

if df.empty:
    st.warning("Запрос выполнен, но данных нет")
    st.stop()

st.subheader("Результат")
st.dataframe(df, use_container_width=True)

# -------------------------------------------------
# Visualization
# -------------------------------------------------

viz = decide_visualization(
    question=question,
    available_columns=list(df.columns),
)

if viz.get("need_chart"):
    st.subheader("Визуализация")

    x = viz.get("x_col")
    y = viz.get("y_col")
    chart_type = viz.get("chart_type")

    if x in df.columns and y in df.columns:
        if chart_type == "bar":
            st.bar_chart(df.set_index(x)[y])
        elif chart_type == "line":
            st.line_chart(df.set_index(x)[y])
        elif chart_type == "pie":
            st.pyplot(
                df.groupby(x)[y].sum().plot.pie(autopct="%1.1f%%").figure
            )

# -------------------------------------------------
# Explanations
# -------------------------------------------------

with st.spinner("Готовлю объяснение…"):
    explanation = explain_sql_brief(
        question=question,
        sql=sql,
    )

if explanation:
    st.subheader("Что делает этот запрос")
    st.write(explanation)

with st.spinner("Готовлю вывод…"):
    summary = summarize_result_brief(
        question=question,
        sql=sql,
        preview_rows=df.head(20).to_dict(orient="records"),
        schema_description=list_tables_and_schema(db_path=db_path),
    )

if summary:
    st.subheader("Краткий вывод")
    st.write(summary)
