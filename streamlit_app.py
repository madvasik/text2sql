import os
from typing import Optional

import streamlit as st

from text2sql.db import ensure_database_exists, list_tables_and_schema, execute_readonly
from text2sql.llm import generate_sql_from_nl, decide_visualization, explain_sql_brief, summarize_result_brief


def init() -> None:
    # No .env loading; key is provided via UI and stored in process env
    ensure_database_exists()


def render_sidebar() -> None:
    st.sidebar.title("Настройки")
    st.sidebar.caption("Mistral API")
    current_key = os.getenv("MISTRAL_API_KEY")
    masked = (current_key[:6] + "…") if current_key else "не задан"
    st.sidebar.write(f"Ключ: {masked}")
    if "api_key_input" not in st.session_state:
        st.session_state["api_key_input"] = ""
    st.sidebar.text_input(
        "MISTRAL_API_KEY",
        key="api_key_input",
        type="password",
        help="Ключ хранится только в памяти процесса",
    )
    apply = st.sidebar.button("Применить ключ")
    if apply:
        new_key = st.session_state.get("api_key_input", "").strip()
        if new_key:
            os.environ["MISTRAL_API_KEY"] = new_key
            st.sidebar.success("Ключ задан для текущего процесса.")
    st.sidebar.divider()
    if st.sidebar.checkbox("Показать схему БД", value=False):
        st.sidebar.code(list_tables_and_schema())


def main() -> None:
    init()
    st.set_page_config(page_title="Text → SQL (SQLite)", page_icon="🧮", layout="centered")
    st.title("Text → SQL для SQLite")
    st.caption("Запрашивайте данные на естественном языке. Генерация SQL — через Mistral.")

    render_sidebar()

    default_q = "какие сотрудники работают в отделе Engineering?"
    question = st.text_area("Ваш запрос", value=default_q, height=100, placeholder="например: топ-3 сотрудников по зарплате")

    col1, col2 = st.columns([1, 1])
    with col1:
        run = st.button("Сгенерировать и выполнить", type="primary")
    with col2:
        clear = st.button("Очистить")

    if clear:
        st.experimental_rerun()

    if run:
        if not os.getenv("MISTRAL_API_KEY"):
            st.error("Сначала задайте MISTRAL_API_KEY в боковой панели.")
        elif not question.strip():
            st.warning("Введите запрос.")
        else:
            with st.spinner("Генерация SQL и выполнение…"):
                try:
                    sql = generate_sql_from_nl(question)
                except Exception as e:
                    st.error(f"Не удалось сгенерировать SQL: {e}")
                    sql = None
                if sql:
                    try:
                        headers, rows = execute_readonly(sql)
                        # Compute brief explanations once and cache in session
                        rationale = ""
                        result_summary = ""
                        try:
                            rationale = explain_sql_brief(question, sql)
                        except Exception:
                            pass
                        try:
                            import pandas as pd
                            df_preview = pd.DataFrame(rows, columns=headers).head(20)
                            result_summary = summarize_result_brief(question, sql, df_preview.to_dict(orient="records"))
                        except Exception:
                            pass
                        st.session_state["last_result"] = {"sql": sql, "headers": headers, "rows": rows, "question": question, "rationale": rationale, "summary": result_summary}
                    except Exception as e:
                        st.error(f"Не удалось выполнить SQL: {e}")

    # Always render last result (persists across widget changes)
    last = st.session_state.get("last_result")
    if last and isinstance(last, dict):
        sql = last.get("sql")
        headers = last.get("headers") or []
        rows = last.get("rows") or []
        last_question = last.get("question") or question

        st.subheader("Сгенерированный SQL")
        st.code(sql or "", language="sql")
        rationale = last.get("rationale") or ""
        if rationale:
            st.caption("Обоснование скрипта")
            st.markdown(rationale)

        st.subheader("Результат")
        if not rows:
            st.info("Нет строк в результате.")
        else:
            # Convert to a simple table
            import pandas as pd
            df = pd.DataFrame(rows, columns=headers)
            # Downloads
            cold1, cold2 = st.columns(2)
            with cold1:
                csv_bytes = df.to_csv(index=False).encode("utf-8")
                st.download_button("Скачать CSV", data=csv_bytes, file_name="result.csv", mime="text/csv")
            with cold2:
                from io import BytesIO
                bio = BytesIO()
                excel_bytes = None
                try:
                    try:
                        df.to_excel(bio, index=False)
                        excel_bytes = bio.getvalue()
                    except Exception:
                        # Try explicit engine
                        bio = BytesIO()
                        df.to_excel(bio, index=False, engine="openpyxl")
                        excel_bytes = bio.getvalue()
                except Exception:
                    excel_bytes = None
                if excel_bytes is not None:
                    st.download_button("Скачать Excel", data=excel_bytes, file_name="result.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                else:
                    st.caption("Для экспорта Excel установите пакет openpyxl: pip install openpyxl")
            st.dataframe(df, use_container_width=True)

            # Charts disabled by request – show only table and downloads

        summary = last.get("summary") or ""
        st.caption("Краткое пояснение результата")
        if summary:
            st.markdown(summary)
        else:
            if st.button("Сгенерировать пояснение", key="gen_summary_btn"):
                with st.spinner("Генерация пояснения…"):
                    try:
                        import pandas as pd
                        df_preview = pd.DataFrame(rows, columns=headers).head(20)
                        new_summary = summarize_result_brief(last_question, sql or "", df_preview.to_dict(orient="records"))
                        st.session_state["last_result"]["summary"] = new_summary
                        if new_summary:
                            st.success("Готово")
                            st.markdown(new_summary)
                        else:
                            st.info("Модель не вернула пояснение.")
                    except Exception as e:
                        st.warning(f"Не удалось сгенерировать пояснение: {e}")


if __name__ == "__main__":
    main()

