"""
Main module with the BigQueryFunnel class for analyzing funnels.
"""

from google.cloud import bigquery
import pandas as pd
from typing import List, Tuple, Dict, Optional, Union, Any

from bq_funnel.query_builder import (
    parse_time_window,
    build_funnel_query
)

# Importing functions for working with GA4
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
    Class for receiving and analyzing user funnel data from BigQuery.
    Allows you to analyze sequences of user events taking into account time windows,
    group data and apply various filters.
    """
    
    def __init__(self, project_id: str, dataset_id: str, table_id: str, client=None, data_source="standard"):
        """
        Initializing the BigQuery client and setting up the data source.
        
        Args:
            project_id: ID Google Cloud project
            dataset_id: ID BigQuery dataset
            table_id: ID tables with event data
            client: Ready-made authorized BigQuery client (optional))
            data_source: Data source type ("standard" or "ga4")
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
        Retrieving funnel data with an optimized query.
        
        Args:
            events: List of events in the funnel in order.
                   Each element can be a string with an event name or a dictionary
                   type {'name': 'event_name', 'params': {'param1': 'value1', 'param2': 'value2'}}
            date_range: Tuple (initial_end date_date) in the format 'YYYY-MM-DD'
            window: Time window for passing through the funnel (for example, '8h', '24h', '7d')
            group_by: Field for grouping results
            filters: Dictionary of filters in the format {field: value}, applicable to all events
            timestamp_field: Field name with timestamp (default "timestamp" for standard or "event_timestamp" for GA4)
            
        Returns:
            pandas.DataFrame with funnel data
        """
        # Convert window to seconds
        window_seconds = parse_time_window(window)
        
        # Define a default time field depending on the data source
        if timestamp_field is None:
            timestamp_field = "event_timestamp" if self.data_source == "ga4" else "timestamp"
        
        # Generating an SQL query depending on the data source
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
        
        # Executing the request
        query_job = self.client.query(query)
        results = query_job.result()
        
        # Converting results to pandas DataFrame
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
        Executes a random SQL query against BigQuery.
        
        Args:
            query: SQL-request to execute
            params: Query parameters for substitution (optional))
            timeout: Timeout in seconds (optional))
            dry_run: If True, the request will not be executed, but will be checked and evaluated
                    (useful for syntax checking and cost estimation)
            
        Returns:
            pandas.DataFrame with query results,
            or a QueryJob object with information about the request, if dry_run=True
        """
        # Creating a Query Parameters Object
        job_config = bigquery.QueryJobConfig()
        
        # If the parameters are specified, add them to the configuration
        if params:
            job_config.query_parameters = [
                bigquery.ScalarQueryParameter(name, 
                                             self._get_bigquery_param_type(params[name]), 
                                             params[name])
                for name in params
            ]
        
        # If the dry flag is specified_run, install it
        if dry_run:
            job_config.dry_run = True
        
        # Executing the request
        query_job = self.client.query(
            query,
            job_config=job_config,
            timeout=timeout
        )
        
        # If dry_run=True, return information about the request
        if dry_run:
            return query_job
        
        # Otherwise, we get the results and convert them to DataFrame
        results = query_job.result()
        df = results.to_dataframe()
        
        return df
    
    def _get_bigquery_param_type(self, value: Any) -> str:
        """
        Determines the type of a BigQuery parameter based on the Python type.
        
        Args:
            value: Parameter value
            
        Returns:
            String with BigQuery parameter type
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
            # For other types we use the string representation
            return 'STRING'
        
    def calculate_conversion_rates(
            self, 
            df: pd.DataFrame, 
            group_by: Optional[Union[str, List[str]]] = None, 
            aggregation_type: str = "unique"
        ) -> pd.DataFrame:
            """
            Calculates conversion rates between funnel steps.
            
            Args:
                df: DataFrame with funnel data obtained from the optimized method_funnel
                group_by: Column(s) for grouping results (None for aggregated results)
                aggregation_type: Aggregation type for conversion calculation ("unique" for unique users,
                                "total" for total number of events)
                
            Returns:
                DataFrame with added conversion rate columns
            """
            from bq_funnel.analysis.conversion import calculate_conversion_rates
            return calculate_conversion_rates(df, group_by, aggregation_type)
    
    def analyze_dropoffs(self, df: pd.DataFrame, total_users_col: str = 'total_users') -> pd.DataFrame:
        """
        Analyzes user churn between funnel steps and identifies critical points.
        
        Args:
            df: DataFrame with funnel data
            total_users_col: Column name with total number of users
            
        Returns:
            DataFrame with churn analysis at every step
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
        Conducts statistical analysis of the significance of differences between the control and test groups.
        
        Args:
            control_df: DataFrame with control group data
            test_df: DataFrame with test group data
            first_step: Name of the column with the number of users in the first step
            last_step: The name of the column with the number of users in the last step (by default the last available)
            confidence_level: Confidence level for statistical test (default 0.95)
            
        Returns:
            Dictionary with results of statistical analysis
        """
        return analyze_ab_test_significance(
            control_df=control_df,
            test_df=test_df,
            first_step=first_step,
            last_step=last_step,
            confidence_level=confidence_level
        )
    
    def visualize_funnel(self, df: pd.DataFrame, title: str = "User funnel") -> None:
        """
        Visualizes the funnel based on data.
        
        Args:
            df: DataFrame with funnel data
            title: Graph title
        """
        visualize_funnel(df, title)
    
    def compare_funnels(self, dfs: List[pd.DataFrame], labels: List[str], title: str = "Funnel comparison") -> None:
        """
        Compares multiple funnels on one chart.
        
        Args:
            dfs: List of DataFrames with funnel data
            labels: List of labels for each funnel
            title: Graph title
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
        Receiving funnel data with AB testing integration.
        
        Args:
            events: List of events in the funnel in order
            date_range: Tuple (initial_end date_date) in the format 'YYYY-MM-DD'
            ab_test_config: AB test configuration in format:
                {
                    'table_id': 'project.dataset.ab_tests_sessions',
                    'test_code': 'TRAVELUAEAQ',  # Test code for filtering
                    'user_id_field': 'googleID'  # User ID field in the AB tests table
                }
            window: Time window for passing through the funnel (for example, '8h', '24h', '7d')
            filters: Dictionary of filters in the format {field: value}, applicable to all events
            timestamp_field: Field name with timestamp
                
        Returns:
            Dictionary with funnel data for control and test groups:
            {
                'control': DataFrame control group,
                'test': DataFrame test group,
                'overall': DataFrame all users
            }
        """
        # Building a query to obtain AB test data
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
        
        # Retrieving AB Test Data
        ab_test_df = self.custom_query(ab_test_query)
        
        # Convert window to seconds
        window_seconds = parse_time_window(window)
        
        # Define a default time field depending on the data source
        if timestamp_field is None:
            timestamp_field = "event_timestamp" if self.data_source == "ga4" else "timestamp"
        
        # Generating a basic SQL query for a funnel
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
            
            # We get the basic funnel request and integrate it into the template
            base_funnel_query = build_funnel_query_ga4(
                events=events,
                date_range=date_range,
                window_seconds=window_seconds,
                table_id=self.full_table_id,
                group_by=None,  # We do not use standard grouping
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
            
            # We get the basic funnel request and integrate it into the template
            base_funnel_query = build_funnel_query(
                events=events,
                date_range=date_range,
                window_seconds=window_seconds,
                table_id=self.full_table_id,
                group_by=None,  # We do not use standard grouping
                filters=filters
            )
        
        # Formation of the final request with the integration of AB tests
        final_query = base_query_template.format(
            base_funnel_query=base_funnel_query,
            user_id_field=ab_test_config['user_id_field'],
            ab_table_id=ab_test_config['table_id'],
            test_code=ab_test_config['test_code'],
            start_date=date_range[0],
            end_date=date_range[1]
        ) 
        # Executing the request
        result_df = self.custom_query(final_query)
        
        result = {}
    
        # Checking if there is data
        if len(result_df) > 0:
            # We get a list of user IDs and funnel steps
            user_id_col = 'user_id' if self.data_source == "ga4" else 'user_id'
            step_cols = [col for col in result_df.columns if col.endswith('_users') or col == 'total_users']
            
            # Create an empty DataFrame for general results
            overall_data = {}
            
            # For each step we count unique users
            for step_col in step_cols:
                # Filtering users who have reached this step
                step_users = result_df[result_df[step_col] > 0][user_id_col].nunique()
                overall_data[step_col] = step_users
            
            # Convert to DataFrame
            result['overall'] = pd.DataFrame([overall_data])
            
            # Group by ab_group and count unique users for each group
            for group_name, group_df in result_df.groupby('ab_group'):
                group_data = {}
                
                # For each step in the group we count unique users
                for step_col in step_cols:
                    step_users = group_df[group_df[step_col] > 0][user_id_col].nunique()
                    group_data[step_col] = step_users
                
                # Convert to DataFrame
                result[group_name] = pd.DataFrame([group_data])
        else:
            # If there is no data, return empty DataFrame
            result = {
                'control': pd.DataFrame(),
                'test': pd.DataFrame(),
                'overall': pd.DataFrame()
            }
        
        return result

# Add this method to the BigQueryFunnel class in core.py