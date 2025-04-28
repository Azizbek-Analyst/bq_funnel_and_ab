"""
Модуль для сравнения нескольких воронок на одном графике.
"""

import warnings
import re
import pandas as pd
from typing import List


def compare_funnels(dfs: List, labels: List[str], title: str = "Сравнение воронок") -> None:
    """
    Сравнивает несколько воронок на одном графике.
    
    Args:
        dfs: Список DataFrames с данными воронок
        labels: Список меток для каждой воронки
        title: Заголовок графика
    """
    try:
        import matplotlib.pyplot as plt
        import seaborn as sns
        import numpy as np
    except ImportError:
        warnings.warn("Для визуализации требуется установить matplotlib и seaborn.")
        return
    
    if len(dfs) != len(labels):
        raise ValueError("Количество DataFrame должно соответствовать количеству меток.")
    
    # Определяем тип столбцов (пользователи или события)
    first_df = dfs[0]
    user_columns = [col for col in first_df.columns if col.startswith('step') and col.endswith('_users')]
    if not user_columns:
        user_columns = [col for col in first_df.columns if col.startswith('step') and col.endswith('_events')]
    
    if not user_columns:
        raise ValueError("Не найдены столбцы с количеством пользователей (step*_users) или событий (step*_events)")
    
    # Сортируем столбцы по номеру шага
    user_columns.sort(key=lambda x: int(re.search(r'(\d+)', x).group(1)))
    
    # Поиск столбцов с названиями шагов
    step_name_columns = [col for col in first_df.columns if col.startswith('step') and col.endswith('_name')]
    has_step_names = len(step_name_columns) > 0
    
    # Если есть столбцы с названиями шагов, используем их
    if has_step_names:
        step_name_columns.sort(key=lambda x: int(re.search(r'(\d+)', x).group(1)))
        # Берем названия из первого DataFrame
        if len(first_df) > 0:
            step_names = [first_df[col].iloc[0] if not pd.isna(first_df[col].iloc[0]) else f"Шаг {i+1}" 
                          for i, col in enumerate(step_name_columns)]
            
            # Дополняем названия шагов, если их меньше, чем столбцов с данными
            if len(step_names) < len(user_columns):
                step_names.extend([f"Шаг {i+1+len(step_names)}" for i in range(len(user_columns) - len(step_names))])
        else:
            step_names = [f"Шаг {i+1}" for i in range(len(user_columns))]
    else:
        # Если нет столбцов с названиями, используем стандартные
        step_names = [f"Шаг {i+1}" for i in range(len(user_columns))]
    
    # Создаем график для сравнения воронок
    plt.figure(figsize=(14, 7))
    sns.set_style("whitegrid")
    
    bar_width = 0.8 / len(dfs)
    colors = ['#3498db', '#2ecc71', '#e74c3c', '#f39c12', '#9b59b6', '#1abc9c', '#e67e22', '#34495e']
    
    for i, (df, label) in enumerate(zip(dfs, labels)):
        # Если DataFrame содержит группировку, используем только первую строку
        if len(df) > 1 and 'group_value' in df.columns:
            warnings.warn(f"DataFrame для группы '{label}' содержит несколько строк. Используется только первая строка.")
            df_to_viz = df.iloc[[0]]
        else:
            df_to_viz = df.iloc[[0]] if len(df) > 0 else df
            
        if len(df_to_viz) == 0:
            warnings.warn(f"DataFrame для группы '{label}' пуст. Группа пропущена.")
            continue
        
        # Получаем данные для текущей воронки
        users = [df_to_viz[col].iloc[0] for col in user_columns]
        
        # Позиции для отрисовки столбцов (со смещением для разных групп)
        positions = np.arange(len(user_columns)) + (i - len(dfs)/2 + 0.5) * bar_width
        
        # Отрисовка столбцов
        bars = plt.bar(positions, users, width=bar_width, label=label, 
                       color=colors[i % len(colors)], alpha=0.8)
        
        # Добавление значений на столбцы
        for j, (bar, value) in enumerate(zip(bars, users)):
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                     f'{int(value)}',
                     ha='center', va='bottom', fontsize=8)
            
            # Добавляем проценты конверсии для каждой группы
            if j > 0:
                prev_value = users[j-1]
                curr_value = value
                conversion = (curr_value / prev_value * 100) if prev_value > 0 else 0
                # Конверсию показываем только для первой и последней группы, чтобы не загромождать график
                if i == 0 or i == len(dfs) - 1:
                    x_pos = bar.get_x() + bar.get_width()/2
                    y_pos = height / 2
                    plt.text(x_pos, y_pos, f"{conversion:.1f}%", ha='center', va='center',
                            fontsize=7, rotation=90, color='white', fontweight='bold')
    
    # Настройка осей и меток
    plt.title(title, fontsize=14, fontweight='bold')
    plt.ylabel("Количество пользователей", fontsize=12)
    plt.xticks(np.arange(len(user_columns)), step_names, rotation=15)
    plt.legend(title="Группа", fontsize=10)
    
    # Добавление общей конверсии для каждой группы в легенду
    handles, labels_current = plt.gca().get_legend_handles_labels()
    
    # Обновляем метки с информацией о конверсии
    for i, (df, label) in enumerate(zip(dfs, labels)):
        if len(df) > 0:
            df_to_viz = df.iloc[[0]]
            users = [df_to_viz[col].iloc[0] for col in user_columns]
            if len(users) > 1 and users[0] > 0:
                total_conversion = (users[-1] / users[0] * 100)
                labels_current[i] = f"{label} (конв: {total_conversion:.1f}%)"
    
    plt.legend(handles, labels_current, title="Группа", fontsize=10)
    
    plt.tight_layout()
    plt.show()
    
    # Дополнительный график: сравнение конверсий между шагами
    if len(user_columns) > 1:
        plt.figure(figsize=(14, 7))
        
        # Подготовка данных о конверсии между шагами
        conversion_data = []
        
        for i, (df, label) in enumerate(zip(dfs, labels)):
            if len(df) > 0:
                df_to_viz = df.iloc[[0]]
                users = [df_to_viz[col].iloc[0] for col in user_columns]
                
                # Расчет конверсий между шагами
                for j in range(1, len(user_columns)):
                    prev_value = users[j-1]
                    curr_value = users[j]
                    conversion = (curr_value / prev_value * 100) if prev_value > 0 else 0
                    conversion_data.append({
                        'Группа': label,
                        'Переход': f"{step_names[j-1]} → {step_names[j]}",
                        'Конверсия (%)': conversion
                    })
                
                # Добавляем общую конверсию
                total_conversion = (users[-1] / users[0] * 100) if users[0] > 0 else 0
                conversion_data.append({
                    'Группа': label,
                    'Переход': 'Общая конверсия',
                    'Конверсия (%)': total_conversion
                })
        
        # Создаем DataFrame для построения графика
        conversion_df = pd.DataFrame(conversion_data)
        
        # Создаем график сравнения конверсий
        ax = sns.barplot(
            data=conversion_df,
            x='Переход',
            y='Конверсия (%)',
            hue='Группа',
            palette=colors[:len(dfs)]
        )
        
        # Добавляем значения на столбцы
        for container in ax.containers:
            ax.bar_label(container, fmt='%.1f%%', fontsize=8)
        
        plt.title(f"{title} - Сравнение конверсий между шагами", fontsize=14, fontweight='bold')
        plt.ylabel("Конверсия (%)", fontsize=12)
        plt.xticks(rotation=15)
        plt.tight_layout()
        plt.show()