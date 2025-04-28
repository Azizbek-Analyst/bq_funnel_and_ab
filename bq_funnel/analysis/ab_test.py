"""
Модуль для статистического анализа A/B-тестов на основе данных воронки.
"""

import numpy as np
import warnings
from typing import Dict, Union, Optional


def analyze_ab_test_significance(
    control_df,
    test_df,
    first_step: str = 'step1_users',
    last_step: Optional[str] = None,
    confidence_level: float = 0.95
) -> Dict[str, Union[float, bool, str]]:
    """
    Проводит статистический анализ значимости различий между контрольной и тестовой группами.
    
    Args:
        control_df: DataFrame с данными контрольной группы
        test_df: DataFrame с данными тестовой группы
        first_step: Название столбца с количеством пользователей на первом шаге
        last_step: Название столбца с количеством пользователей на последнем шаге (по умолчанию последний доступный)
        confidence_level: Уровень доверия для статистического теста (по умолчанию 0.95)
        
    Returns:
        Словарь с результатами статистического анализа
    """
    try:
        from scipy import stats
    except ImportError:
        warnings.warn("Для статистического анализа требуется установить scipy.")
        return {"error": "Для статистического анализа требуется установить scipy."}
    
    # Если последний шаг не указан, находим его автоматически
    if last_step is None:
        step_columns = [col for col in control_df.columns if col.startswith('step') and col.endswith('_users')]
        step_columns.sort(key=lambda x: int(x.replace('step', '').replace('_users', '')))
        last_step = step_columns[-1]
    
    # Извлечение данных о конверсии
    control_start = control_df[first_step].iloc[0]
    control_end = control_df[last_step].iloc[0]
    test_start = test_df[first_step].iloc[0]
    test_end = test_df[last_step].iloc[0]
    
    # Расчет конверсии
    control_conv_rate = control_end / control_start if control_start > 0 else 0
    test_conv_rate = test_end / test_start if test_start > 0 else 0
    
    # Абсолютная разница в конверсии
    abs_diff = (test_conv_rate - control_conv_rate) * 100
    
    # Относительное улучшение
    rel_lift = ((test_conv_rate / control_conv_rate) - 1) * 100 if control_conv_rate > 0 else 0
    
    # Статистический тест (z-тест для пропорций)
    control_success = control_end
    control_total = control_start
    test_success = test_end
    test_total = test_start
    
    # Расчет стандартной ошибки и z-значения
    p1 = control_success / control_total if control_total > 0 else 0
    p2 = test_success / test_total if test_total > 0 else 0
    p_pool = (control_success + test_success) / (control_total + test_total) if (control_total + test_total) > 0 else 0
    
    se = np.sqrt(p_pool * (1 - p_pool) * (1/control_total + 1/test_total)) if p_pool > 0 and p_pool < 1 else 0
    
    # Избегаем деления на ноль
    if se > 0:
        z_score = (p2 - p1) / se
        p_value = 2 * (1 - stats.norm.cdf(abs(z_score)))  # Двусторонний тест
    else:
        z_score = 0
        p_value = 1.0
    
    # Определение значимости результата
    alpha = 1 - confidence_level
    is_significant = p_value < alpha
    
    # Формирование рекомендации
    if is_significant and rel_lift > 0:
        recommendation = "Результаты теста показывают статистически значимое улучшение. Рекомендуется внедрить изменения."
    elif is_significant and rel_lift < 0:
        recommendation = "Результаты теста показывают статистически значимое ухудшение. Рекомендуется отклонить изменения."
    else:
        recommendation = "Результаты теста не показывают статистически значимых изменений. Рекомендуется продолжить эксперимент или рассмотреть другие варианты."
    
    return {
        'control_conversion': round(control_conv_rate * 100, 2),
        'test_conversion': round(test_conv_rate * 100, 2),
        'absolute_difference': round(abs_diff, 2),
        'relative_lift': round(rel_lift, 2),
        'p_value': round(p_value, 4),
        'is_significant': is_significant,
        'confidence_level': int(confidence_level * 100),
        'recommendation': recommendation
    }