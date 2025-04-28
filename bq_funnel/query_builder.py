"""
Модуль для построения SQL-запросов к BigQuery для анализа воронок.
"""

from typing import List, Tuple, Dict, Optional, Union, Any


def parse_time_window(window: str) -> int:
    """
    Преобразование строкового представления временного окна в секунды.
    
    Args:
        window: Строка с указанием временного окна (например, '8h', '24h', '7d')
        
    Returns:
        Количество секунд
    """
    unit = window[-1].lower()
    value = int(window[:-1])
    
    if unit == 'h':
        return value * 3600
    elif unit == 'd':
        return value * 86400
    elif unit == 'm':
        return value * 60
    elif unit == 's':
        return value
    else:
        raise ValueError(f"Неподдерживаемая единица времени: {unit}. Используйте 's', 'm', 'h' или 'd'.")


def build_filter_conditions(filters: Dict[str, Union[str, List[str]]]) -> str:
    """
    Создание условий фильтрации для SQL запроса.
    
    Args:
        filters: Словарь фильтров в формате {поле: значение}
        
    Returns:
        Строка с условиями WHERE для SQL запроса
    """
    conditions = []
    
    for field, value in filters.items():
        if isinstance(value, list):
            placeholders = ', '.join([f"'{v}'" for v in value])
            conditions.append(f"{field} IN ({placeholders})")
        else:
            conditions.append(f"{field} = '{value}'")
            
    return " AND ".join(conditions)


def normalize_event(event: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Нормализует представление события, преобразуя строку в словарь.
    
    Args:
        event: Название события (строка) или словарь с параметрами
        
    Returns:
        Нормализованный словарь события
    """
    if isinstance(event, str):
        return {'name': event, 'params': {}}
    elif isinstance(event, dict) and 'name' in event:
        if 'params' not in event:
            event['params'] = {}
        return event
    else:
        raise ValueError("Событие должно быть строкой или словарем с ключом 'name'")


def build_event_condition(event_dict: Dict[str, Any]) -> str:
    """
    Создает условие SQL для события с учетом дополнительных параметров.
    
    Args:
        event_dict: Словарь события с ключами 'name' и 'params'
        
    Returns:
        SQL условие для фильтрации события
    """
    conditions = [f"event_name = '{event_dict['name']}'"]
    
    for param, value in event_dict['params'].items():
        if isinstance(value, list):
            placeholders = ', '.join([f"'{v}'" for v in value])
            conditions.append(f"{param} IN ({placeholders})")
        else:
            conditions.append(f"{param} = '{value}'")
    
    return " AND ".join(conditions)


def build_funnel_query(
    events: List[Union[str, Dict[str, Any]]],
    date_range: Tuple[str, str],
    window_seconds: int,
    table_id: str,
    group_by: Optional[str] = None,
    filters: Optional[Dict[str, Union[str, List[str]]]] = None
) -> str:
    """
    Формирование оптимизированного SQL запроса для анализа воронки.
    
    Args:
        events: Список событий в воронке
        date_range: Кортеж (начальная_дата, конечная_дата)
        window_seconds: Временное окно в секундах
        table_id: Полный идентификатор таблицы в формате "project.dataset.table"
        group_by: Поле для группировки
        filters: Словарь фильтров
        
    Returns:
        SQL запрос для выполнения в BigQuery
    """
    start_date, end_date = date_range
    
    # Нормализация событий
    normalized_events = [normalize_event(event) for event in events]
    
    # Создание общего CTE для базовой фильтрации
    base_filters = f"DATE(timestamp) BETWEEN '{start_date}' AND '{end_date}'"
    
    if filters:
        filter_conditions = build_filter_conditions(filters)
        base_filters = f"{base_filters} AND {filter_conditions}"
    
    # Создание общего базового CTE
    base_cte = f"""
    filtered_events AS (
        SELECT *
        FROM `{table_id}`
        WHERE {base_filters}
    )
    """
    
    # Формирование подзапросов для каждого события в воронке
    event_ctes = []
    
    for i, event_dict in enumerate(normalized_events):
        event_condition = build_event_condition(event_dict)
        event_alias = f"e{i}"
        
        event_cte = f"""
        {event_alias} AS (
            SELECT
                user_id,
                timestamp as {event_alias}_timestamp,
                {f"{group_by} as group_value," if group_by else ""}
                '{event_dict["name"]}' as {event_alias}_name
            FROM
                filtered_events
            WHERE
                {event_condition}
        )
        """
        
        event_ctes.append(event_cte)
    
    # Объединение подзапросов в единый SQL запрос
    join_clauses = []
    select_clauses = ["e0.user_id"]
    
    if group_by:
        select_clauses.append("e0.group_value")
    
    for i in range(len(normalized_events)):
        select_clauses.append(f"e{i}.{f'e{i}_timestamp'}")
        select_clauses.append(f"e{i}.{f'e{i}_name'}")
    
    for i in range(1, len(normalized_events)):
        time_condition = f"e{i}.{f'e{i}_timestamp'} >= e{i-1}.{f'e{i-1}_timestamp'} AND " \
                        f"e{i}.{f'e{i}_timestamp'} <= TIMESTAMP_ADD(e{i-1}.{f'e{i-1}_timestamp'}, INTERVAL {window_seconds} SECOND)"
        
        join_condition = f"e{i}.user_id = e0.user_id AND {time_condition}"
        
        if group_by:
            join_condition += f" AND e{i}.group_value = e0.group_value"
            
        join_clauses.append(f"LEFT JOIN {f'e{i}'} ON {join_condition}")
    
    # Формирование итогового SQL запроса с оптимизированной структурой
    all_ctes = [base_cte] + event_ctes
    
    base_query = f"""
    WITH {", ".join(all_ctes)}
    
    SELECT
        {', '.join(select_clauses)},
        COUNT(DISTINCT e0.user_id) as total_users,
        {', '.join([f"COUNT(DISTINCT CASE WHEN e{i}.user_id IS NOT NULL THEN e{i}.user_id END) as step{i+1}_users" for i in range(len(normalized_events))])}
    FROM
        e0
        {' '.join(join_clauses)}
    """
    
    # Добавление группировки, если указан параметр group_by
    if group_by:
        group_columns = ['e0.group_value']
        base_query += f"\nGROUP BY {', '.join(group_columns)}"
        base_query += f"\nORDER BY e0.group_value"
    
    return base_query