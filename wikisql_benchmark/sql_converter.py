"""
Модуль для конвертации между структурированным форматом WikiSQL и SQL строками.
"""

from typing import Dict, Any, List, Tuple, Optional


# Операторы WikiSQL
OPERATORS = ["", "=", ">", "<", ">=", "<=", "!="]
# Агрегации WikiSQL
AGGREGATIONS = ["", "MAX", "MIN", "COUNT", "SUM", "AVG"]


def wikisql_to_sql(
    sql_struct: Dict[str, Any],
    table: Dict[str, Any],
    table_name: str = "table"
) -> str:
    """
    Конвертирует структурированный SQL WikiSQL в SQL строку.
    
    Args:
        sql_struct: Структурированный SQL с полями:
            - sel: индекс колонки для SELECT
            - agg: индекс агрегации (0=None, 1=MAX, 2=MIN, 3=COUNT, 4=SUM, 5=AVG)
            - conds: список условий [(column_index, operator_index, value), ...]
        table: Объект таблицы с полями:
            - header: список названий колонок
            - types: список типов колонок
            - rows: список строк данных
        table_name: Имя таблицы в SQL (по умолчанию "table")
        
    Returns:
        SQL строка
    """
    headers = table["header"]
    sel_idx = sql_struct["sel"]
    agg_idx = sql_struct.get("agg", 0)
    
    # Выбираем колонку
    if sel_idx >= len(headers):
        raise ValueError(f"Column index {sel_idx} out of range (max: {len(headers)-1})")
    
    column = headers[sel_idx]
    safe_column = f'"{column.replace(\'"\', \'""\')}"'
    
    # Добавляем агрегацию
    agg = AGGREGATIONS[agg_idx] if agg_idx < len(AGGREGATIONS) else ""
    if agg:
        select_expr = f"{agg}({safe_column})"
    else:
        select_expr = safe_column
    
    # Формируем WHERE условия
    conds = sql_struct.get("conds", [])
    where_parts = []
    
    for cond in conds:
        if len(cond) < 3:
            continue
        col_idx, op_idx, value = cond[0], cond[1], cond[2]
        
        if col_idx >= len(headers):
            continue
        
        col_name = headers[col_idx]
        safe_col = f'"{col_name.replace(\'"\', \'""\')}"'
        op = OPERATORS[op_idx] if op_idx < len(OPERATORS) else "="
        
        # Экранируем значение
        if isinstance(value, str):
            safe_value = f"'{value.replace(\"'\", \"''\")}'"
        else:
            safe_value = str(value)
        
        where_parts.append(f"{safe_col} {op} {safe_value}")
    
    # Собираем SQL
    sql = f'SELECT {select_expr} FROM "{table_name}"'
    if where_parts:
        sql += " WHERE " + " AND ".join(where_parts)
    
    return sql


def sql_to_wikisql(
    sql: str,
    table: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Пытается конвертировать SQL строку в структурированный формат WikiSQL.
    Это упрощенная версия, которая работает только для простых запросов.
    
    Args:
        sql: SQL строка
        table: Объект таблицы с полями header, types, rows
        
    Returns:
        Структурированный SQL или None если не удалось распарсить
    """
    import re
    
    headers = table["header"]
    sql_upper = sql.upper().strip()
    
    # Извлекаем SELECT выражение
    select_match = re.search(r'SELECT\s+(.+?)\s+FROM', sql_upper, re.IGNORECASE)
    if not select_match:
        return None
    
    select_expr = select_match.group(1).strip()
    
    # Определяем агрегацию и колонку
    agg_idx = 0
    col_idx = 0
    
    for i, agg in enumerate(AGGREGATIONS):
        if agg and select_expr.startswith(agg + "("):
            agg_idx = i
            # Извлекаем колонку из агрегации
            col_match = re.search(r'\((.+?)\)', select_expr)
            if col_match:
                col_name = col_match.group(1).strip().strip('"').strip("'")
                if col_name in headers:
                    col_idx = headers.index(col_name)
            break
    else:
        # Нет агрегации, ищем колонку напрямую
        col_name = select_expr.strip().strip('"').strip("'")
        if col_name in headers:
            col_idx = headers.index(col_name)
        else:
            return None
    
    # Извлекаем WHERE условия
    where_match = re.search(r'WHERE\s+(.+?)(?:\s+ORDER|\s+LIMIT|$)', sql_upper, re.IGNORECASE)
    conds = []
    
    if where_match:
        where_clause = where_match.group(1).strip()
        # Простой парсинг условий (работает только для простых случаев)
        # Разбиваем по AND
        conditions = re.split(r'\s+AND\s+', where_clause, flags=re.IGNORECASE)
        
        for cond in conditions:
            cond = cond.strip()
            # Ищем паттерн: column operator value
            for op_idx, op in enumerate(OPERATORS):
                if op and op in cond:
                    parts = cond.split(op, 1)
                    if len(parts) == 2:
                        col_name = parts[0].strip().strip('"').strip("'")
                        value = parts[1].strip().strip("'").strip('"')
                        
                        if col_name in headers:
                            col_idx_cond = headers.index(col_name)
                            # Пытаемся преобразовать значение в число если возможно
                            try:
                                if '.' in value:
                                    value = float(value)
                                else:
                                    value = int(value)
                            except:
                                pass
                            
                            conds.append([col_idx_cond, op_idx, value])
                            break
    
    return {
        "sel": col_idx,
        "agg": agg_idx,
        "conds": conds,
    }
