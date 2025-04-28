"""
Модуль для построения SQL-запросов к таблицам GA4 в BigQuery для анализа воронок.
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
    Поддерживает операторы сравнения, включая LIKE.
    
    Args:
        event_dict: Словарь события с ключами 'name' и 'params'
            Значения в params могут включать операторы сравнения 
            в начале строки (%value% для LIKE)
        
    Returns:
        SQL условие для фильтрации события
    """
    conditions = [f"event_name = '{event_dict['name']}'"]
    
    for param, value in event_dict['params'].items():
        if isinstance(value, list):
            # Обработка списков значений
            placeholders = ', '.join([f"'{v}'" for v in value])
            conditions.append(f"{param} IN ({placeholders})")
        elif isinstance(value, str) and '%' in value:
            # Обработка LIKE условий с %
            conditions.append(f"{param} LIKE '{value}'")
        else:
            # Стандартное сравнение на равенство
            conditions.append(f"{param} = '{value}'")
    
    return " AND ".join(conditions)


def build_funnel_query_ga4(
    events: List[Union[str, Dict[str, Any]]],
    date_range: Tuple[str, str],
    window_seconds: int,
    table_id: str,
    group_by: Optional[str] = None,
    filters: Optional[Dict[str, Union[str, List[str]]]] = None,
    timestamp_field: str = "event_timestamp"
) -> str:
    """
    Формирование оптимизированного SQL запроса для анализа воронки в таблицах GA4.
    
    Args:
        events: Список событий в воронке
        date_range: Кортеж (начальная_дата, конечная_дата)
        window_seconds: Временное окно в секундах
        table_id: Полный идентификатор таблицы в формате "project.dataset.table"
        group_by: Поле для группировки
        filters: Словарь фильтров
        timestamp_field: Название поля с временной меткой (по умолчанию "event_timestamp" для GA4)
        
    Returns:
        SQL запрос для выполнения в BigQuery
    """
    start_date, end_date = date_range
    
    # Нормализация событий
    normalized_events = [normalize_event(event) for event in events]
    
    # Для GA4 используем event_date вместо преобразования timestamp
    base_filters = f"event_date BETWEEN '{start_date}' AND '{end_date}'"
    
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
                user_pseudo_id as user_id,  -- GA4 использует user_pseudo_id вместо user_id
                {timestamp_field} as {event_alias}_timestamp,
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
        select_clauses.append(f"TIMESTAMP_MICROS(e{i}.{f'e{i}_timestamp'}) as {f'e{i}_timestamp'}")
        select_clauses.append(f"e{i}.{f'e{i}_name'}")
    
    for i in range(1, len(normalized_events)):
        # Используем TIMESTAMP_DIFF для более простого и понятного условия оконного периода
        time_condition = f"TIMESTAMP_DIFF(TIMESTAMP_MICROS(e{i}.{f'e{i}_timestamp'}), TIMESTAMP_MICROS(e{i-1}.{f'e{i-1}_timestamp'}), SECOND) <= {window_seconds}"
        
        join_condition = f"e{i}.user_id = e0.user_id AND e{i}.{f'e{i}_timestamp'} >= e{i-1}.{f'e{i-1}_timestamp'} AND {time_condition}"
        
        if group_by:
            join_condition += f" AND e{i}.group_value = e0.group_value"
            
        join_clauses.append(f"LEFT JOIN {f'e{i}'} ON {join_condition}")
    
    # Формирование итогового SQL запроса с оптимизированной структурой
    all_ctes = [base_cte] + event_ctes
    
    # Упрощаем подсчет пользователей на каждом шаге
    steps_count = []
    for i in range(len(normalized_events)):
        steps_count.append(f"COUNT(DISTINCT e{i}.user_id) as step{i+1}_users")
    
    base_query = f"""
    WITH {", ".join(all_ctes)}
    
    SELECT
        {', '.join(select_clauses)},
        COUNT(DISTINCT e0.user_id) as total_users,
        {', '.join(steps_count)}
    FROM
        e0
        {' '.join(join_clauses)}
    """
    
    # Добавление группировки
    if group_by:
        group_columns = ['e0.group_value']
        base_query += f"\nGROUP BY {', '.join(group_columns)}"
        base_query += f"\nORDER BY e0.group_value"
    else:
        # Если нет группировки, добавляем GROUP BY ALL для получения общих результатов
        base_query += "\nGROUP BY ALL"
    
    return base_query