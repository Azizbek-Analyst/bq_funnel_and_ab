"""
Модуль для визуализации воронки конверсии.
"""

import warnings
import pandas as pd
import re


def visualize_funnel(df, title: str = "Воронка пользователей") -> None:
    """
    Визуализирует воронку на основе данных.
    
    Args:
        df: DataFrame с данными воронки
        title: Заголовок графика
    """
    try:
        import matplotlib.pyplot as plt
        import seaborn as sns
    except ImportError:
        warnings.warn("Для визуализации требуется установить matplotlib и seaborn.")
        return
    
    # Проверяем наличие столбцов с количеством уникальных пользователей
    user_columns = [col for col in df.columns if col.startswith('step') and col.endswith('_users')]
    
    # Если нет столбцов с пользователями, проверяем столбцы с событиями
    if not user_columns:
        user_columns = [col for col in df.columns if col.startswith('step') and col.endswith('_events')]
        
    if not user_columns:
        raise ValueError("Не найдены столбцы с количеством пользователей (step*_users) или событий (step*_events)")
    
    # Сортируем столбцы по номеру шага
    user_columns.sort(key=lambda x: int(re.search(r'(\d+)', x).group(1)))
    
    # Если есть несколько групп, выбираем первую для визуализации
    if len(df) > 1 and 'group_value' in df.columns:
        first_group = df.iloc[0]['group_value']
        warnings.warn(f"DataFrame содержит несколько групп. Визуализируется только группа '{first_group}'. "
                     f"Для сравнения групп используйте метод compare_funnels().")
        df_to_viz = df[df['group_value'] == first_group].iloc[[0]]
    else:
        df_to_viz = df.iloc[[0]] if len(df) > 0 else df
    
    # Поиск столбцов с названиями шагов
    step_name_columns = [col for col in df.columns if col.startswith('step') and col.endswith('_name')]
    has_step_names = len(step_name_columns) > 0
    
    # Если есть столбцы с названиями шагов, используем их
    if has_step_names:
        step_name_columns.sort(key=lambda x: int(re.search(r'(\d+)', x).group(1)))
        step_names = [df_to_viz[col].iloc[0] if not pd.isna(df_to_viz[col].iloc[0]) else f"Шаг {i+1}" 
                     for i, col in enumerate(step_name_columns)]
        
        # Проверяем, что у нас есть название для каждого шага
        if len(step_names) < len(user_columns):
            # Дополняем названия шагов, если их меньше, чем столбцов с данными
            step_names.extend([f"Шаг {i+1+len(step_names)}" for i in range(len(user_columns) - len(step_names))])
    else:
        # Если нет столбцов с названиями, используем стандартные
        step_names = [f"Шаг {i+1}" for i in range(len(user_columns))]
    
    # Извлекаем данные для визуализации
    if len(df_to_viz) > 0:
        users = [df_to_viz[col].iloc[0] for col in user_columns]
    else:
        warnings.warn("Предоставленный DataFrame пуст. Визуализация невозможна.")
        return
    
    # Построение графика
    plt.figure(figsize=(12, 6))
    sns.set_style("whitegrid")
    
    # Используем цвета, которые хорошо различаются
    colors = ['#3498db', '#2ecc71', '#e74c3c', '#f39c12', '#9b59b6', '#1abc9c', '#e67e22', '#34495e']
    bars = plt.bar(range(len(users)), users, width=0.6, color=colors[:len(users)])
    
    # Добавляем метки значений на столбцы
    for i, (bar, value) in enumerate(zip(bars, users)):
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                 f'{int(value)}',
                 ha='center', va='bottom', fontweight='bold')
        
        # Добавляем проценты конверсии между столбцами
        if i > 0:
            prev_value = users[i-1]
            curr_value = value
            conversion = (curr_value / prev_value * 100) if prev_value > 0 else 0
            x_pos = bar.get_x() - 0.15
            y_pos = (height + users[i-1]) / 2
            plt.text(x_pos, y_pos, f"{conversion:.1f}%", ha='center', va='center',
                    fontsize=9, rotation=90, color='#2c3e50', fontweight='bold')
    
    # Настройка осей и меток
    plt.xticks(range(len(users)), step_names, rotation=15, ha='center')
    plt.ylabel('Количество пользователей', fontsize=12)
    plt.title(title, fontsize=14, fontweight='bold')
    
    # Добавляем процент конверсии от первого к последнему шагу
    if len(users) > 1:
        total_conversion = (users[-1] / users[0] * 100) if users[0] > 0 else 0
        plt.figtext(0.5, 0.01, f'Общая конверсия: {total_conversion:.1f}%', 
                  ha='center', fontsize=12, fontweight='bold')
    
    plt.tight_layout()
    plt.show()