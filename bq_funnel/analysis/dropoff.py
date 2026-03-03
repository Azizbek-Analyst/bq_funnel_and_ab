"""
Module for analyzing user churn between funnel steps.
"""

import pandas as pd


def analyze_dropoffs(df: pd.DataFrame, total_users_col: str = 'total_users') -> pd.DataFrame:
    """
    Analyzes user churn between funnel steps and identifies critical points.
    
    Args:
        df: DataFrame with funnel data
        total_users_col: Column name with total number of users
        
    Returns:
        DataFrame with churn analysis at every step
    """
    # Finding columns with the number of users at each step
    step_columns = [col for col in df.columns if col.startswith('step') and col.endswith('_users')]
    step_columns.sort(key=lambda x: int(x.replace('step', '').replace('_users', '')))
    
    # Creating a New DataFrame for Churn Analysis
    dropoff_data = []
    
    # If there are several groups, we process each separately
    if 'group_value' in df.columns:
        group_values = df['group_value'].unique()
        for group in group_values:
            group_df = df[df['group_value'] == group]
            dropoff_data.extend(_calculate_dropoffs_for_df(group_df, step_columns, group))
    else:
        # Processing a case without grouping
        dropoff_data.extend(_calculate_dropoffs_for_df(df, step_columns))
    
    dropoff_df = pd.DataFrame(dropoff_data)
    
    # Determination of critical points (where outflow is greatest) for each group
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
    Helper function to calculate churn for one DataFrame.
    
    Args:
        df: DataFrame with data
        step_columns: List of columns with number of users
        group_value: Group value (if any))
        
    Returns:
        List of dictionaries with churn data
    """
    dropoff_data = []
    
    # Total number of users entering the funnel
    initial_users = df[step_columns[0]].iloc[0]
    
    for i in range(len(step_columns) - 1):
        current_step = step_columns[i]
        next_step = step_columns[i+1]
        
        users_current = df[current_step].iloc[0]
        users_next = df[next_step].iloc[0]
        
        # Number of dropped users
        dropoff_count = users_current - users_next
        
        # Percentage of dropouts from the current step
        dropoff_percent = (dropoff_count / users_current * 100) if users_current > 0 else 0
        
        # Percentage of dropouts from the total number at the beginning of the funnel
        dropoff_percent_total = (dropoff_count / initial_users * 100) if initial_users > 0 else 0
        
        data = {
            'step_from': f"Step {i+1}",
            'step_to': f"Step {i+2}",
            'users_before': users_current,
            'users_after': users_next,
            'dropoff_count': dropoff_count,
            'dropoff_percent': round(dropoff_percent, 2),
            'dropoff_percent_total': round(dropoff_percent_total, 2),
            'retention_percent': round(100 - dropoff_percent, 2)
        }
        
        # Add the group value if it was passed
        if group_value is not None:
            data['group_value'] = group_value
            
        dropoff_data.append(data)
    
    return dropoff_data