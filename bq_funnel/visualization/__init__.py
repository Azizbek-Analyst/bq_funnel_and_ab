"""
Подпакет для визуализации данных воронки.
"""

from bq_funnel.visualization.funnel_plot import visualize_funnel
from bq_funnel.visualization.comparison_plot import compare_funnels

__all__ = ["visualize_funnel", "compare_funnels"]