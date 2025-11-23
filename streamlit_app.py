import os
from typing import Optional
from pathlib import Path
import tempfile

import streamlit as st

from text2sql.db import (
    ensure_database_exists, list_tables_and_schema, execute_readonly,
    import_csv_to_sqlite, DB_PATH, DATA_DIR
)
from text2sql.llm import generate_sql_from_nl, decide_visualization, explain_sql_brief, summarize_result_brief, validate_api_key


def init() -> None:
    # No .env loading; key is provided via UI and stored in process env
    ensure_database_exists()
    if "db_path" not in st.session_state:
        st.session_state["db_path"] = None
        st.session_state["table_name"] = None
        st.session_state["schema_description"] = None


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
            # Проверяем ключ перед применением
            status_placeholder = st.sidebar.empty()
            status_placeholder.info("⏳ Проверка ключа API...")
            try:
                is_valid, message = validate_api_key(new_key)
                status_placeholder.empty()
                if is_valid:
                    os.environ["MISTRAL_API_KEY"] = new_key
                    st.sidebar.success(message)
                else:
                    st.sidebar.error(message)
            except Exception as e:
                status_placeholder.empty()
                st.sidebar.error(f"Ошибка при проверке ключа: {e}")
        else:
            st.sidebar.warning("Введите ключ API")
    st.sidebar.divider()
    
    # Загрузка данных
    st.sidebar.subheader("Загрузка данных")
    
    # Загрузка CSV файла
    uploaded_csv = st.sidebar.file_uploader(
        "Загрузить CSV файл",
        type=['csv'],
        help="Первая строка должна содержать названия столбцов",
        key="csv_uploader"
    )
    
    # Загрузка TXT файла с описанием таблицы
    uploaded_txt = st.sidebar.file_uploader(
        "Загрузить TXT файл с описанием таблицы",
        type=['txt'],
        help="Текстовый файл с описанием структуры таблицы для LLM",
        key="txt_uploader"
    )
    
    if uploaded_txt is not None:
        # Проверяем, не загружали ли мы уже этот файл (по размеру и имени)
        file_id = f"{uploaded_txt.name}_{uploaded_txt.size}"
        if st.session_state.get("last_txt_file_id") != file_id:
            try:
                # Перемещаем указатель в начало на случай повторного чтения
                uploaded_txt.seek(0)
                schema_text = uploaded_txt.read().decode('utf-8')
                st.session_state["schema_description"] = schema_text
                st.session_state["last_txt_file_id"] = file_id
                st.sidebar.success("Описание таблицы загружено")
            except Exception as e:
                st.sidebar.error(f"Ошибка чтения TXT файла: {e}")
        
        # Показываем описание если оно есть
        schema_text = st.session_state.get("schema_description")
        if schema_text:
            with st.sidebar.expander("Просмотр описания"):
                st.code(schema_text)
    
    if uploaded_csv is not None:
        table_name_input = st.sidebar.text_input(
            "Имя таблицы",
            value="uploaded_data",
            help="Имя таблицы в базе данных (будет автоматически очищено от спецсимволов)"
        )
        
        if st.sidebar.button("Импортировать CSV", type="primary"):
            with st.spinner("Импорт CSV в SQLite..."):
                tmp_path = None
                try:
                    # Сохраняем временный файл
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv', mode='wb') as tmp_file:
                        tmp_file.write(uploaded_csv.getvalue())
                        tmp_path = tmp_file.name
                    
                    # Импортируем CSV
                    table_name, db_path = import_csv_to_sqlite(
                        tmp_path,
                        table_name_input or "uploaded_data",
                        db_path=DB_PATH
                    )
                    
                    # Обновляем session state
                    st.session_state["db_path"] = db_path
                    st.session_state["table_name"] = table_name
                    st.session_state["csv_uploaded"] = True
                    
                    st.sidebar.success(f"CSV импортирован в таблицу '{table_name}'")
                    
                except Exception as e:
                    st.sidebar.error(f"Ошибка импорта: {e}")
                finally:
                    # Удаляем временный файл в любом случае
                    if tmp_path and os.path.exists(tmp_path):
                        try:
                            os.unlink(tmp_path)
                        except Exception:
                            pass
    
    # Кнопка для очистки данных
    if st.sidebar.button("Очистить данные"):
        st.session_state["db_path"] = None
        st.session_state["table_name"] = None
        st.session_state["csv_uploaded"] = False
        st.session_state["schema_description"] = None
        st.sidebar.success("Данные очищены")
        st.rerun()
    
    st.sidebar.divider()
    if st.sidebar.checkbox("Показать схему БД", value=False):
        db_path = st.session_state.get("db_path")
        table_name = st.session_state.get("table_name")
        schema_desc = st.session_state.get("schema_description")
        schema = list_tables_and_schema(db_path=db_path, schema_description=schema_desc, table_name=table_name)
        st.sidebar.code(schema)


def main() -> None:
    init()
    st.set_page_config(page_title="Text → SQL (SQLite)", page_icon="🧮", layout="centered")
    st.title("Text → SQL для SQLite")
    st.caption("Запрашивайте данные на естественном языке. Генерация SQL — через Mistral.")

    render_sidebar()
    
    # Показываем информацию о текущей БД
    if st.session_state.get("csv_uploaded"):
        table_name = st.session_state.get("table_name")
        if table_name:
            st.info(f"📊 Используется загруженная таблица: **{table_name}**")
    else:
        st.warning("⚠️ Загрузите CSV файл и TXT файл с описанием таблицы для начала работы")

    default_q = ""
    question = st.text_area("Ваш запрос", value=default_q, height=100, placeholder="например: топ-3 сотрудников по зарплате")

    col1, col2 = st.columns([1, 1])
    with col1:
        run = st.button("Сгенерировать и выполнить", type="primary")
    with col2:
        clear = st.button("Очистить")

    if clear:
        st.rerun()

    if run:
        if not os.getenv("MISTRAL_API_KEY"):
            st.error("Сначала задайте MISTRAL_API_KEY в боковой панели.")
        elif not question.strip():
            st.warning("Введите запрос.")
        else:
            with st.spinner("Генерация SQL и выполнение…"):
                db_path = st.session_state.get("db_path")
                schema_desc = st.session_state.get("schema_description")
                if not db_path:
                    st.error("Сначала загрузите CSV файл с данными.")
                elif not schema_desc:
                    st.error("Сначала загрузите TXT файл с описанием таблицы.")
                else:
                    try:
                        sql = generate_sql_from_nl(question, db_path=db_path, schema_description=schema_desc)
                    except Exception as e:
                        st.error(f"Не удалось сгенерировать SQL: {e}")
                        sql = None
                    
                    if sql:
                        try:
                            headers, rows = execute_readonly(sql, db_path=db_path)
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
            st.subheader("Обоснование скрипта")
            st.markdown(rationale)

        st.subheader("Результат")
        if not rows:
            st.info("Нет строк в результате.")
        else:
            # Convert to a simple table
            import pandas as pd
            df = pd.DataFrame(rows, columns=headers)
            # Показываем таблицу сначала
            st.dataframe(df, use_container_width=True)
            # Downloads под таблицей
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

            # Charts disabled by request – show only table and downloads

        summary = last.get("summary") or ""
        st.subheader("Краткое пояснение результата")
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

