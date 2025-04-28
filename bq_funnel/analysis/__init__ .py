"""
Подпакет для анализа данных воронки.
"""

from bq_funnel.analysis.conversion import calculate_conversion_rates
from bq_funnel.analysis.dropoff import analyze_dropoffs
from bq_funnel.analysis.ab_test import analyze_ab_test_significance

__all__ = ["calculate_conversion_rates", "analyze_dropoffs", "analyze_ab_test_significance"]