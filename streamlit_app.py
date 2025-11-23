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


def user_requests_chart(question: str) -> bool:
    """Проверяет, просит ли пользователь график напрямую в запросе."""
    question_lower = question.lower()
    chart_keywords = [
        "график", "диаграмма", "визуализация", "визуализировать",
        "chart", "graph", "plot", "visualization", "visualize",
        "построить график", "показать график", "нарисовать график",
        "столбчатая", "линейная", "круговая", "pie", "bar", "line"
    ]
    return any(keyword in question_lower for keyword in chart_keywords)


def auto_detect_chart_columns(df):
    """Автоматически определяет колонки для графика, если они не указаны."""
    import pandas as pd
    
    if len(df.columns) < 2:
        return None, None
    
    # Ищем первую текстовую/категориальную колонку для X
    x_col = None
    for col in df.columns:
        if df[col].dtype == 'object' or df[col].dtype.name == 'category':
            x_col = col
            break
    
    # Если не нашли текстовую, берем первую колонку
    if x_col is None:
        x_col = df.columns[0]
    
    # Ищем первую числовую колонку для Y
    y_col = None
    for col in df.columns:
        if col != x_col and pd.api.types.is_numeric_dtype(df[col]):
            y_col = col
            break
    
    # Если не нашли числовую, берем вторую колонку
    if y_col is None:
        y_col = df.columns[1] if len(df.columns) > 1 else None
    
    return x_col, y_col


def generate_chart_png(df, chart_type: str, x_col: str, y_col: str) -> Optional[bytes]:
    """Генерирует PNG изображение графика и возвращает байты."""
    from io import BytesIO
    import matplotlib
    matplotlib.use('Agg')  # Используем backend без GUI
    import matplotlib.pyplot as plt
    
    if chart_type == "none" or not x_col or not y_col:
        return None
    
    if x_col not in df.columns or y_col not in df.columns:
        return None
    
    try:
        fig, ax = plt.subplots(figsize=(10, 6))
        
        if chart_type == "bar":
            ax.bar(df[x_col].astype(str), df[y_col])
            ax.set_xlabel(x_col)
            ax.set_ylabel(y_col)
            ax.set_title(f"{y_col} по {x_col}")
            plt.xticks(rotation=45, ha='right')
        elif chart_type == "line":
            ax.plot(df[x_col].astype(str), df[y_col], marker='o')
            ax.set_xlabel(x_col)
            ax.set_ylabel(y_col)
            ax.set_title(f"{y_col} по {x_col}")
            plt.xticks(rotation=45, ha='right')
        elif chart_type == "pie":
            ax.pie(df[y_col], labels=df[x_col].astype(str), autopct='%1.1f%%')
            ax.set_title(f"{y_col} по {x_col}")
        else:
            plt.close(fig)
            return None
        
        plt.tight_layout()
        
        # Сохраняем в BytesIO
        buf = BytesIO()
        fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
        buf.seek(0)
        png_bytes = buf.getvalue()
        buf.close()
        plt.close(fig)
        
        return png_bytes
    except Exception as e:
        return None


def render_chart(df, chart_type: str, x_col: str, y_col: str):
    """Строит график указанного типа на основе DataFrame."""
    if chart_type == "none":
        return False
    
    # Если колонки не указаны, пытаемся определить автоматически
    if not x_col or not y_col:
        x_col, y_col = auto_detect_chart_columns(df)
        if not x_col or not y_col:
            return False
    
    # Проверяем наличие колонок
    if x_col not in df.columns or y_col not in df.columns:
        return False
    
    try:
        if chart_type == "bar":
            st.bar_chart(df.set_index(x_col)[y_col])
        elif chart_type == "line":
            st.line_chart(df.set_index(x_col)[y_col])
        elif chart_type == "pie":
            # Для pie chart используем plotly или matplotlib
            try:
                import plotly.express as px
                fig = px.pie(df, values=y_col, names=x_col, title=f"{y_col} по {x_col}")
                st.plotly_chart(fig, use_container_width=True)
            except ImportError:
                # Fallback на matplotlib
                import matplotlib.pyplot as plt
                fig, ax = plt.subplots()
                ax.pie(df[y_col], labels=df[x_col], autopct='%1.1f%%')
                ax.set_title(f"{y_col} по {x_col}")
                st.pyplot(fig)
                plt.close(fig)
        else:
            return False
        return True
    except Exception as e:
        st.warning(f"Не удалось построить график: {e}")
        return False


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
                                schema_desc = st.session_state.get("schema_description")
                                result_summary = summarize_result_brief(question, sql, df_preview.to_dict(orient="records"), schema_description=schema_desc)
                            except Exception:
                                pass
                            
                            # Определяем, нужен ли график
                            chart_info = None
                            try:
                                import pandas as pd
                                df_full = pd.DataFrame(rows, columns=headers)
                                user_wants_chart = user_requests_chart(question)
                                
                                if user_wants_chart or len(df_full) > 0:
                                    chart_info = decide_visualization(question, headers)
                                    # Если пользователь просит напрямую, принудительно включаем график
                                    if user_wants_chart and not chart_info.get("need_chart"):
                                        chart_info["need_chart"] = True
                                        # Пытаемся определить тип графика из запроса
                                        question_lower = question.lower()
                                        if "столбчатая" in question_lower or "bar" in question_lower:
                                            chart_info["chart_type"] = "bar"
                                        elif "линейная" in question_lower or "line" in question_lower:
                                            chart_info["chart_type"] = "line"
                                        elif "круговая" in question_lower or "pie" in question_lower:
                                            chart_info["chart_type"] = "pie"
                                        # Если тип не определен, используем bar по умолчанию
                                        if chart_info["chart_type"] == "none":
                                            chart_info["chart_type"] = "bar"
                            except Exception:
                                pass
                            
                            st.session_state["last_result"] = {
                                "sql": sql, 
                                "headers": headers, 
                                "rows": rows, 
                                "question": question, 
                                "rationale": rationale, 
                                "summary": result_summary,
                                "chart_info": chart_info
                            }
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

            # Построение графиков
            chart_info = last.get("chart_info")
            if chart_info and chart_info.get("need_chart"):
                chart_type = chart_info.get("chart_type", "none")
                x_col = chart_info.get("x_col")
                y_col = chart_info.get("y_col")
                
                if chart_type != "none":
                    st.subheader("Визуализация")
                    # Определяем колонки, если они не указаны
                    if not x_col or not y_col:
                        x_col, y_col = auto_detect_chart_columns(df)
                    
                    if render_chart(df, chart_type, x_col, y_col):
                        # Показываем информацию о графике только если колонки были определены
                        if x_col and y_col:
                            chart_type_names = {"bar": "столбчатая", "line": "линейная", "pie": "круговая"}
                            chart_name = chart_type_names.get(chart_type, chart_type)
                            st.caption(f"Тип графика: {chart_name}, X: {x_col}, Y: {y_col}")
                            
                            # Кнопка скачивания PNG
                            png_bytes = generate_chart_png(df, chart_type, x_col, y_col)
                            if png_bytes:
                                st.download_button(
                                    "Скачать PNG",
                                    data=png_bytes,
                                    file_name=f"chart_{chart_type}.png",
                                    mime="image/png"
                                )

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
                        schema_desc = st.session_state.get("schema_description")
                        new_summary = summarize_result_brief(last_question, sql or "", df_preview.to_dict(orient="records"), schema_description=schema_desc)
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

