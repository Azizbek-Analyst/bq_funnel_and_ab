"""
Основной модуль с классом BigQueryFunnel для анализа воронок.
"""

from google.cloud import bigquery
import pandas as pd
from typing import List, Tuple, Dict, Optional, Union, Any

from bq_funnel.query_builder import (
    parse_time_window,
    build_funnel_query
)

# Импорт функций для работы с GA4
from bq_funnel.query_builder_ga4 import (
    build_funnel_query_ga4
)

from bq_funnel.analysis.conversion import calculate_conversion_rates
from bq_funnel.analysis.dropoff import analyze_dropoffs
from bq_funnel.analysis.ab_test import analyze_ab_test_significance
from bq_funnel.visualization.funnel_plot import visualize_funnel
from bq_funnel.visualization.comparison_plot import compare_funnels


class BigQueryFunnel:
    """
    Класс для получения и анализа данных воронки пользователя из BigQuery.
    Позволяет анализировать последовательности событий пользователей с учетом временных окон,
    группировать данные и применять различные фильтры.
    """
    
    def __init__(self, project_id: str, dataset_id: str, table_id: str, client=None, data_source="standard"):
        """
        Инициализация клиента BigQuery и настройка источника данных.
        
        Args:
            project_id: ID проекта Google Cloud
            dataset_id: ID набора данных BigQuery
            table_id: ID таблицы с данными событий
            client: Готовый авторизованный клиент BigQuery (необязательно)
            data_source: Тип источника данных ("standard" или "ga4")
        """
        self.client = client if client else bigquery.Client(project=project_id)
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.full_table_id = f"{project_id}.{dataset_id}.{table_id}"
        self.data_source = data_source
        
    def optimized_funnel(
        self,
        events: List[Union[str, Dict[str, Any]]],
        date_range: Tuple[str, str],
        window: str = '24h',
        group_by: Optional[str] = None,
        filters: Optional[Dict[str, Union[str, List[str]]]] = None,
        timestamp_field: str = None
    ) -> pd.DataFrame:
        """
        Получение данных воронки с оптимизированным запросом.
        
        Args:
            events: Список событий в воронке в порядке следования. 
                   Каждый элемент может быть строкой с названием события или словарем 
                   вида {'name': 'event_name', 'params': {'param1': 'value1', 'param2': 'value2'}}
            date_range: Кортеж (начальная_дата, конечная_дата) в формате 'YYYY-MM-DD'
            window: Временное окно для прохождения воронки (например, '8h', '24h', '7d')
            group_by: Поле для группировки результатов
            filters: Словарь фильтров в формате {поле: значение}, применяемых ко всем событиям
            timestamp_field: Название поля с временной меткой (по умолчанию "timestamp" для standard или "event_timestamp" для GA4)
            
        Returns:
            pandas.DataFrame с данными воронки
        """
        # Преобразование окна в секунды
        window_seconds = parse_time_window(window)
        
        # Определение поля времени по умолчанию в зависимости от источника данных
        if timestamp_field is None:
            timestamp_field = "event_timestamp" if self.data_source == "ga4" else "timestamp"
        
        # Формирование SQL запроса в зависимости от источника данных
        if self.data_source == "ga4":
            query = build_funnel_query_ga4(
                events=events,
                date_range=date_range,
                window_seconds=window_seconds,
                table_id=self.full_table_id,
                group_by=group_by,
                filters=filters,
                timestamp_field=timestamp_field
            )
        else:
            query = build_funnel_query(
                events=events,
                date_range=date_range,
                window_seconds=window_seconds,
                table_id=self.full_table_id,
                group_by=group_by,
                filters=filters
            )
        
        # Выполнение запроса
        query_job = self.client.query(query)
        results = query_job.result()
        
        # Преобразование результатов в pandas DataFrame
        df = results.to_dataframe()
        


        return df
    
    def custom_query(
        self, 
        query: str, 
        params: Optional[Dict[str, Any]] = None, 
        timeout: Optional[float] = None,
        dry_run: bool = False
    ) -> pd.DataFrame:
        """
        Выполняет произвольный SQL-запрос к BigQuery.
        
        Args:
            query: SQL-запрос для выполнения
            params: Параметры запроса для подстановки (необязательно)
            timeout: Время ожидания в секундах (необязательно)
            dry_run: Если True, запрос не будет выполнен, но будет проверен и оценен
                    (полезно для проверки синтаксиса и оценки стоимости)
            
        Returns:
            pandas.DataFrame с результатами запроса,
            или объект QueryJob с информацией о запросе, если dry_run=True
        """
        # Создание объекта параметров запроса
        job_config = bigquery.QueryJobConfig()
        
        # Если указаны параметры, добавляем их в конфигурацию
        if params:
            job_config.query_parameters = [
                bigquery.ScalarQueryParameter(name, 
                                             self._get_bigquery_param_type(params[name]), 
                                             params[name])
                for name in params
            ]
        
        # Если указан флаг dry_run, устанавливаем его
        if dry_run:
            job_config.dry_run = True
        
        # Выполнение запроса
        query_job = self.client.query(
            query,
            job_config=job_config,
            timeout=timeout
        )
        
        # Если dry_run=True, возвращаем информацию о запросе
        if dry_run:
            return query_job
        
        # Иначе получаем результаты и преобразуем в DataFrame
        results = query_job.result()
        df = results.to_dataframe()
        
        return df
    
    def _get_bigquery_param_type(self, value: Any) -> str:
        """
        Определяет тип параметра BigQuery на основе типа Python.
        
        Args:
            value: Значение параметра
            
        Returns:
            Строка с типом параметра BigQuery
        """
        if isinstance(value, bool):
            return 'BOOL'
        elif isinstance(value, int):
            return 'INT64'
        elif isinstance(value, float):
            return 'FLOAT64'
        elif isinstance(value, str):
            return 'STRING'
        elif isinstance(value, bytes):
            return 'BYTES'
        elif isinstance(value, (list, tuple)) and all(isinstance(x, str) for x in value):
            return 'ARRAY<STRING>'
        elif isinstance(value, (list, tuple)) and all(isinstance(x, int) for x in value):
            return 'ARRAY<INT64>'
        elif isinstance(value, (list, tuple)) and all(isinstance(x, float) for x in value):
            return 'ARRAY<FLOAT64>'
        else:
            # Для других типов используем строковое представление
            return 'STRING'
        
    def calculate_conversion_rates(
            self, 
            df: pd.DataFrame, 
            group_by: Optional[Union[str, List[str]]] = None, 
            aggregation_type: str = "unique"
        ) -> pd.DataFrame:
            """
            Рассчитывает коэффициенты конверсии между шагами воронки.
            
            Args:
                df: DataFrame с данными воронки, полученный из метода optimized_funnel
                group_by: Столбец(цы) для группировки результатов (None для агрегированных результатов)
                aggregation_type: Тип агрегации для подсчета конверсии ("unique" для уникальных пользователей,
                                "total" для общего количества событий)
                
            Returns:
                DataFrame с добавленными столбцами коэффициентов конверсии
            """
            from bq_funnel.analysis.conversion import calculate_conversion_rates
            return calculate_conversion_rates(df, group_by, aggregation_type)
    
    def analyze_dropoffs(self, df: pd.DataFrame, total_users_col: str = 'total_users') -> pd.DataFrame:
        """
        Анализирует отток пользователей между шагами воронки и определяет критические точки.
        
        Args:
            df: DataFrame с данными воронки
            total_users_col: Название столбца с общим количеством пользователей
            
        Returns:
            DataFrame с анализом оттока на каждом шаге
        """
        return analyze_dropoffs(df, total_users_col)
    
    def analyze_ab_test_significance(
        self, 
        control_df: pd.DataFrame, 
        test_df: pd.DataFrame, 
        first_step: str = 'step1_users', 
        last_step: str = None,
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
        return analyze_ab_test_significance(
            control_df=control_df,
            test_df=test_df,
            first_step=first_step,
            last_step=last_step,
            confidence_level=confidence_level
        )
    
    def visualize_funnel(self, df: pd.DataFrame, title: str = "Воронка пользователей") -> None:
        """
        Визуализирует воронку на основе данных.
        
        Args:
            df: DataFrame с данными воронки
            title: Заголовок графика
        """
        visualize_funnel(df, title)
    
    def compare_funnels(self, dfs: List[pd.DataFrame], labels: List[str], title: str = "Сравнение воронок") -> None:
        """
        Сравнивает несколько воронок на одном графике.
        
        Args:
            dfs: Список DataFrames с данными воронок
            labels: Список меток для каждой воронки
            title: Заголовок графика
        """
        compare_funnels(dfs, labels, title)


    def funnel_with_ab_test(
        self,
        events: List[Union[str, Dict[str, Any]]],
        date_range: Tuple[str, str],
        ab_test_config: Dict[str, str],
        window: str = '24h',
        filters: Optional[Dict[str, Union[str, List[str]]]] = None,
        timestamp_field: str = None
    ) -> Dict[str, pd.DataFrame]:
        """
        Получение данных воронки с интеграцией AB-тестирования.
        
        Args:
            events: Список событий в воронке в порядке следования
            date_range: Кортеж (начальная_дата, конечная_дата) в формате 'YYYY-MM-DD'
            ab_test_config: Конфигурация AB-теста в формате:
                {
                    'table_id': 'project.dataset.ab_tests_sessions',
                    'test_code': 'TRAVELUAEAQ',  # Код теста для фильтрации
                    'user_id_field': 'googleID'  # Поле с ID пользователя в таблице AB-тестов
                }
            window: Временное окно для прохождения воронки (например, '8h', '24h', '7d')
            filters: Словарь фильтров в формате {поле: значение}, применяемых ко всем событиям
            timestamp_field: Название поля с временной меткой
                
        Returns:
            Словарь с данными воронки для контрольной и тестовой групп:
            {
                'control': DataFrame контрольной группы,
                'test': DataFrame тестовой группы,
                'overall': DataFrame всех пользователей
            }
        """
        # Построение запроса для получения данных AB-теста
        ab_test_query = f"""
        WITH ab_test_data AS (
        SELECT 
            (CASE 
            WHEN GroupCode LIKE '%-A%' THEN 'control' 
            WHEN GroupCode LIKE '%-B%' THEN 'test' 
            ELSE GroupCode 
            END) AS ab_group,
            {ab_test_config['user_id_field']} AS user_id,
            MIN(DATE(date)) AS ab_date,
            -- Extract test code prefix (everything before the dash and A/B suffix)
            REGEXP_EXTRACT(GroupCode, '^([^-]+)') AS test_code
        FROM `{ab_test_config['table_id']}` 
        WHERE GroupCode LIKE '{ab_test_config['test_code']}-%'
            AND DATE(date) BETWEEN '{date_range[0]}' AND '{date_range[1]}'
        GROUP BY 1, 2, 4
        )
        SELECT * FROM ab_test_data
        """
        
        # Получение данных AB-теста
        ab_test_df = self.custom_query(ab_test_query)
        
        # Преобразование окна в секунды
        window_seconds = parse_time_window(window)
        
        # Определение поля времени по умолчанию в зависимости от источника данных
        if timestamp_field is None:
            timestamp_field = "event_timestamp" if self.data_source == "ga4" else "timestamp"
        
        # Формирование базового SQL запроса для воронки
        if self.data_source == "ga4":
            base_query_template = """
            WITH funnel_data AS (
                {base_funnel_query}
            ),
            ab_test_data AS (
                SELECT 
                (CASE 
                    WHEN GroupCode LIKE '%-A%' THEN 'control' 
                    WHEN GroupCode LIKE '%-B%' THEN 'test' 
                    ELSE GroupCode 
                END) AS ab_group,
                {user_id_field} AS user_id,
                MIN(DATE(date)) AS ab_date
                FROM `{ab_table_id}` 
                WHERE GroupCode LIKE '{test_code}-%'
                AND DATE(date) BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY 1, 2
            )
            
            SELECT 
                f.*,
                a.ab_group
            FROM funnel_data f
            JOIN ab_test_data a ON f.user_id = a.user_id
            """
            
            # Получаем базовый запрос воронки и интегрируем его в шаблон
            base_funnel_query = build_funnel_query_ga4(
                events=events,
                date_range=date_range,
                window_seconds=window_seconds,
                table_id=self.full_table_id,
                group_by=None,  # Не используем стандартную группировку
                filters=filters,
                timestamp_field=timestamp_field
            )
        else:
            base_query_template = """
            WITH funnel_data AS (
                {base_funnel_query}
            ),
            ab_test_data AS (
                SELECT 
                (CASE 
                    WHEN GroupCode LIKE '%-A%' THEN 'control' 
                    WHEN GroupCode LIKE '%-B%' THEN 'test' 
                    ELSE GroupCode 
                END) AS ab_group,
                {user_id_field} AS user_id,
                MIN(DATE(date)) AS ab_date
                FROM `{ab_table_id}` 
                WHERE GroupCode LIKE '{test_code}-%'
                AND DATE(date) BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY 1, 2
            )
            
            SELECT 
                f.*,
                a.ab_group
            FROM funnel_data f
            JOIN ab_test_data a ON f.user_id = a.user_id
            """
            
            # Получаем базовый запрос воронки и интегрируем его в шаблон
            base_funnel_query = build_funnel_query(
                events=events,
                date_range=date_range,
                window_seconds=window_seconds,
                table_id=self.full_table_id,
                group_by=None,  # Не используем стандартную группировку
                filters=filters
            )
        
        # Формирование итогового запроса с интеграцией AB-тестов
        final_query = base_query_template.format(
            base_funnel_query=base_funnel_query,
            user_id_field=ab_test_config['user_id_field'],
            ab_table_id=ab_test_config['table_id'],
            test_code=ab_test_config['test_code'],
            start_date=date_range[0],
            end_date=date_range[1]
        ) 
        # Выполнение запроса
        result_df = self.custom_query(final_query)
        
        result = {}
    
        # Проверяем, есть ли данные
        if len(result_df) > 0:
            # Получаем список идентификаторов пользователей и шагов воронки
            user_id_col = 'user_id' if self.data_source == "ga4" else 'user_id'
            step_cols = [col for col in result_df.columns if col.endswith('_users') or col == 'total_users']
            
            # Создаем пустой DataFrame для общих результатов
            overall_data = {}
            
            # Для каждого шага считаем уникальных пользователей
            for step_col in step_cols:
                # Фильтруем пользователей, которые достигли этого шага
                step_users = result_df[result_df[step_col] > 0][user_id_col].nunique()
                overall_data[step_col] = step_users
            
            # Преобразуем в DataFrame
            result['overall'] = pd.DataFrame([overall_data])
            
            # Группируем по ab_group и считаем уникальных пользователей для каждой группы
            for group_name, group_df in result_df.groupby('ab_group'):
                group_data = {}
                
                # Для каждого шага в группе считаем уникальных пользователей
                for step_col in step_cols:
                    step_users = group_df[group_df[step_col] > 0][user_id_col].nunique()
                    group_data[step_col] = step_users
                
                # Преобразуем в DataFrame
                result[group_name] = pd.DataFrame([group_data])
        else:
            # Если данных нет, возвращаем пустые DataFrame
            result = {
                'control': pd.DataFrame(),
                'test': pd.DataFrame(),
                'overall': pd.DataFrame()
            }
        
        return result

# Добавить этот метод в класс BigQueryFunnel в core.py