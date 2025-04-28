"""
BigQuery Funnel - библиотека для анализа воронок пользователей в BigQuery.
"""

__version__ = "0.1.0"

from bq_funnel.core import BigQueryFunnel
from bq_funnel.auth import (
    setup_bigquery_client,
    authenticate_via_pydata,
    authenticate_with_service_account
)

__all__ = [
    "BigQueryFunnel", 
    "setup_bigquery_client", 
    "authenticate_via_pydata",
    "authenticate_with_service_account"
]