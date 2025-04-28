"""
Модуль для расчета коэффициентов конверсии между шагами воронки.
"""

import pandas as pd
from typing import Optional, Union, List
def calculate_conversion_rates(
    df: pd.DataFrame, 
    group_by: Optional[Union[str, List[str]]] = None,
    aggregation_type: str = "unique",
    step_names: Optional[List[str]] = None  # Новый параметр для передачи названий шагов
) -> pd.DataFrame:
    """
    Рассчитывает коэффициенты конверсии между шагами воронки.
    
    Args:
        df: DataFrame с данными воронки, полученный из метода optimized_funnel
        group_by: Столбец(цы) для группировки результатов (None для агрегированных результатов)
        aggregation_type: Тип агрегации для подсчета конверсии ("unique" для уникальных пользователей,
                         "total" для общего количества событий)
        step_names: Список названий шагов воронки (опционально). Если не указан, 
                   будет использован из столбцов e*_name, если они есть, 
                   иначе будут использованы имена по умолчанию "Step 1", "Step 2" и т.д.
        
    Returns:
        DataFrame с добавленными столбцами коэффициентов конверсии и названиями шагов
    """
    # Проверяем тип агрегации
    if aggregation_type.lower() not in ["unique", "total"]:
        raise ValueError("aggregation_type должен быть 'unique' или 'total'")
    
    # Используем уникальных пользователей или общее количество событий
    count_unique = aggregation_type.lower() == "unique"
    
    if 'user_id' not in df.columns:
        raise ValueError("Для расчета конверсии требуется столбец 'user_id'")
    
    # Определяем столбцы с информацией о прохождении шагов
    if count_unique:
        # При подсчете уникальных пользователей
        step_indicators = [col for col in df.columns if col.startswith('step') and '_users' in col]
        if not step_indicators:
            step_indicators = [col for col in df.columns if col.startswith('e') and '_name' in col]
    else:
        # При подсчете общего количества событий
        step_indicators = [col for col in df.columns if col.startswith('step') and '_events' in col]
        if not step_indicators:
            step_indicators = [col for col in df.columns if col.startswith('e') and '_name' in col]
    
    if not step_indicators:
        raise ValueError("Не найдены столбцы, указывающие на шаги воронки")
    
    # Сортировка столбцов по номеру шага
    step_indicators.sort(key=lambda x: int(''.join(filter(str.isdigit, x.split('_')[0]))))
    
    # Проверяем, есть ли столбцы с названиями шагов
    name_columns = [col for col in df.columns if col.startswith('e') and '_name' in col]
    name_columns.sort(key=lambda x: int(''.join(filter(str.isdigit, x.split('_')[0]))))
    
    # Определяем названия шагов
    default_step_names = []
    
    # Если названия шагов переданы как параметр, используем их
    if step_names is not None:
        default_step_names = step_names[:len(step_indicators)]
    # Иначе, если есть столбцы e*_name, извлекаем названия из них
    elif name_columns and not df.empty:
        for col in name_columns[:len(step_indicators)]:
            # Берем первое непустое значение из столбца
            name = df[col].dropna().iloc[0] if not df[col].dropna().empty else f"Step {int(''.join(filter(str.isdigit, col.split('_')[0]))) + 1}"
            default_step_names.append(name)
    
    # Если названия все еще не определены, используем значения по умолчанию
    if not default_step_names:
        default_step_names = [f"Step {i+1}" for i in range(len(step_indicators))]
    
    # Функция расчета конверсии для группы
    def calculate_group_conversion(group_df):
        result = {}
        
        # Проверяем, как определяются шаги (индикаторы или названия)
        if ('_users' in step_indicators[0] and count_unique) or ('_events' in step_indicators[0] and not count_unique):
            # Для воронок со специальными столбцами step1_users/step1_events
            if count_unique:
                # Считаем уникальных пользователей на каждом шаге
                total_users = len(group_df['user_id'].unique())
                result['total_users'] = total_users
                
                # Считаем пользователей на каждом шаге
                for i, step in enumerate(step_indicators):
                    step_users = group_df[group_df[step] > 0]['user_id'].nunique()
                    result[f'step{i+1}_users'] = step_users
                
                # Используем step*_users для расчетов
                step_columns = [f'step{i+1}_users' for i in range(len(step_indicators))]
            else:
                # Считаем общее количество событий на каждом шаге
                total_events = len(group_df)
                result['total_events'] = total_events
                
                # Считаем события на каждом шаге
                for i, step in enumerate(step_indicators):
                    step_events = group_df[step].sum()  # Суммируем, так как может быть > 1 на пользователя
                    result[f'step{i+1}_events'] = step_events
                
                # Используем step*_events для расчетов
                step_columns = [f'step{i+1}_events' for i in range(len(step_indicators))]
            
            # Добавляем названия шагов из определенного списка
            for i, name in enumerate(default_step_names[:len(step_indicators)]):
                result[f'step{i+1}_name'] = name
                
        else:
            # Для воронок с событиями e0_name, e1_name и т.д.
            step_names = []
            for i, step in enumerate(step_indicators):
                step_name = group_df[step].iloc[0] if not group_df.empty else default_step_names[i]
                step_names.append(step_name)
                
                if count_unique:
                    # Считаем уникальных пользователей на каждом шаге
                    step_users = group_df[group_df[step].notna()]['user_id'].nunique()
                    result[f'step{i+1}_users'] = step_users
                else:
                    # Считаем общее количество событий на каждом шаге
                    step_events = group_df[step].notna().sum()
                    result[f'step{i+1}_events'] = step_events
            
            if count_unique:
                # Для подсчета уникальных пользователей
                total_users = len(group_df['user_id'].unique())
                result['total_users'] = total_users
                # Используем step*_users для расчетов
                step_columns = [f'step{i+1}_users' for i in range(len(step_indicators))]
            else:
                # Для подсчета общего количества событий
                total_events = len(group_df)
                result['total_events'] = total_events
                # Используем step*_events для расчетов
                step_columns = [f'step{i+1}_events' for i in range(len(step_indicators))]
            
            # Добавляем имена шагов
            for i, name in enumerate(step_names):
                result[f'step{i+1}_name'] = name
        
        # Общая конверсия
        first_step = step_columns[0]
        last_step = step_columns[-1]
        
        first_step_value = result[first_step]
        last_step_value = result[last_step]
        
        if first_step_value > 0:
            result['total_conversion'] = round((last_step_value / first_step_value) * 100, 2)
        else:
            result['total_conversion'] = 0
        
        # Конверсия между шагами
        for i in range(1, len(step_indicators)):
            prev_step = step_columns[i-1]
            curr_step = step_columns[i]
            
            prev_value = result[prev_step]
            curr_value = result[curr_step]
            
            if prev_value > 0:
                result[f'conversion_{i}_to_{i+1}'] = round((curr_value / prev_value) * 100, 2)
            else:
                result[f'conversion_{i}_to_{i+1}'] = 0
        
        return result
    
    # Если группировка не указана, рассчитываем конверсию по всему DataFrame
    if group_by is None:
        conversion_data = calculate_group_conversion(df)
        return pd.DataFrame([conversion_data])
    
    # Если указана группировка, рассчитываем конверсию для каждой группы
    if isinstance(group_by, str):
        group_by = [group_by]
    
    # Проверяем наличие столбцов группировки
    missing_columns = [col for col in group_by if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Столбцы {missing_columns} не найдены в DataFrame")
    
    # Получаем уникальные комбинации значений группировки
    group_combinations = df[group_by].drop_duplicates()
    
    # Рассчитываем конверсию для каждой группы
    results = []
    for _, group_values in group_combinations.iterrows():
        # Формируем условие фильтрации для группы
        group_filter = True
        for col in group_by:
            group_filter &= (df[col] == group_values[col])
        
        # Получаем данные группы
        group_df = df[group_filter]
        
        # Рассчитываем конверсию для группы
        group_result = calculate_group_conversion(group_df)
        
        # Добавляем значения группировки
        for col in group_by:
            group_result[col] = group_values[col]
        
        results.append(group_result)
    
    # Создаем DataFrame из результатов
    result_df = pd.DataFrame(results)
    
    # Переупорядочиваем столбцы для лучшей читаемости
    if result_df.empty:
        return result_df
        
    columns_order = group_by.copy()
    for i in range(len(step_indicators)):
        step_num = i + 1
        if count_unique:
            step_column = f'step{step_num}_users'
        else:
            step_column = f'step{step_num}_events'
        
        if step_column in result_df.columns:
            columns_order.append(step_column)
        
        if f'step{step_num}_name' in result_df.columns:
            columns_order.append(f'step{step_num}_name')
    
    for i in range(1, len(step_indicators)):
        conversion_column = f'conversion_{i}_to_{i+1}'
        if conversion_column in result_df.columns:
            columns_order.append(conversion_column)
    
    if 'total_conversion' in result_df.columns:
        columns_order.append('total_conversion')
    
    # Оставляем только существующие столбцы и добавляем остальные в конец
    existing_columns = [col for col in columns_order if col in result_df.columns]
    other_columns = [col for col in result_df.columns if col not in columns_order]
    final_columns = existing_columns + other_columns
    
    # Сортируем результаты по группировке
    result_df = result_df[final_columns].sort_values(by=group_by).reset_index(drop=True)
    
    return result_df