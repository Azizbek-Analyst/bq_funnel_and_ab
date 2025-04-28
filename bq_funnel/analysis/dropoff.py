"""
Модуль для анализа оттока пользователей между шагами воронки.
"""

import pandas as pd


def analyze_dropoffs(df: pd.DataFrame, total_users_col: str = 'total_users') -> pd.DataFrame:
    """
    Анализирует отток пользователей между шагами воронки и определяет критические точки.
    
    Args:
        df: DataFrame с данными воронки
        total_users_col: Название столбца с общим количеством пользователей
        
    Returns:
        DataFrame с анализом оттока на каждом шаге
    """
    # Поиск столбцов с количеством пользователей на каждом шаге
    step_columns = [col for col in df.columns if col.startswith('step') and col.endswith('_users')]
    step_columns.sort(key=lambda x: int(x.replace('step', '').replace('_users', '')))
    
    # Создание нового DataFrame для анализа оттока
    dropoff_data = []
    
    # Если есть несколько групп, обрабатываем каждую отдельно
    if 'group_value' in df.columns:
        group_values = df['group_value'].unique()
        for group in group_values:
            group_df = df[df['group_value'] == group]
            dropoff_data.extend(_calculate_dropoffs_for_df(group_df, step_columns, group))
    else:
        # Обработка случая без группировки
        dropoff_data.extend(_calculate_dropoffs_for_df(df, step_columns))
    
    dropoff_df = pd.DataFrame(dropoff_data)
    
    # Определение критических точек (где отток наибольший) для каждой группы
    if not dropoff_df.empty:
        if 'group_value' in dropoff_df.columns:
            for group in dropoff_df['group_value'].unique():
                group_mask = dropoff_df['group_value'] == group
                group_indices = dropoff_df[group_mask].index
                if len(group_indices) > 0:
                    max_dropoff_idx = dropoff_df.loc[group_indices, 'dropoff_percent'].idxmax()
                    dropoff_df.loc[max_dropoff_idx, 'is_critical'] = True
        else:
            max_dropoff_idx = dropoff_df['dropoff_percent'].idxmax()
            dropoff_df.loc[max_dropoff_idx, 'is_critical'] = True
            
        dropoff_df['is_critical'] = dropoff_df['is_critical'].fillna(False)
    
    return dropoff_df


def _calculate_dropoffs_for_df(df: pd.DataFrame, step_columns: list, group_value=None) -> list:
    """
    Вспомогательная функция для расчета оттока для одного DataFrame.
    
    Args:
        df: DataFrame с данными
        step_columns: Список столбцов с количеством пользователей
        group_value: Значение группы (если есть)
        
    Returns:
        Список словарей с данными об оттоке
    """
    dropoff_data = []
    
    # Общее количество пользователей, вошедших в воронку
    initial_users = df[step_columns[0]].iloc[0]
    
    for i in range(len(step_columns) - 1):
        current_step = step_columns[i]
        next_step = step_columns[i+1]
        
        users_current = df[current_step].iloc[0]
        users_next = df[next_step].iloc[0]
        
        # Количество отпавших пользователей
        dropoff_count = users_current - users_next
        
        # Процент отпавших от текущего шага
        dropoff_percent = (dropoff_count / users_current * 100) if users_current > 0 else 0
        
        # Процент отпавших от общего количества в начале воронки
        dropoff_percent_total = (dropoff_count / initial_users * 100) if initial_users > 0 else 0
        
        data = {
            'step_from': f"Шаг {i+1}",
            'step_to': f"Шаг {i+2}",
            'users_before': users_current,
            'users_after': users_next,
            'dropoff_count': dropoff_count,
            'dropoff_percent': round(dropoff_percent, 2),
            'dropoff_percent_total': round(dropoff_percent_total, 2),
            'retention_percent': round(100 - dropoff_percent, 2)
        }
        
        # Добавляем значение группы, если оно было передано
        if group_value is not None:
            data['group_value'] = group_value
            
        dropoff_data.append(data)
    
    return dropoff_data