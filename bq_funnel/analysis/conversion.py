"""
Module for calculating conversion rates between funnel steps.
"""

import pandas as pd
from typing import Optional, Union, List
def calculate_conversion_rates(
    df: pd.DataFrame, 
    group_by: Optional[Union[str, List[str]]] = None,
    aggregation_type: str = "unique",
    step_names: Optional[List[str]] = None  # New option for passing step names
) -> pd.DataFrame:
    """
    Calculates conversion rates between funnel steps.
    
    Args:
        df: DataFrame with funnel data obtained from the optimized method_funnel
        group_by: Column(s) for grouping results (None for aggregated results)
        aggregation_type: Aggregation type for conversion calculation ("unique" for unique users,
                         "total" for total number of events)
        step_names: List of funnel step names (optional). If not specified,
                   will be used from columns e*_name, if they exist,
                   otherwise default names will be used "Step 1", "Step 2" etc.
        
    Returns:
        DataFrame with added conversion rate columns and step names
    """
    # Checking the aggregation type
    if aggregation_type.lower() not in ["unique", "total"]:
        raise ValueError("aggregation_type must be 'unique' or 'total'")
    
    # We use unique users or the total number of events
    count_unique = aggregation_type.lower() == "unique"
    
    if 'user_id' not in df.columns:
        raise ValueError("A column is required to calculate conversion 'user_id'")
    
    # Defining columns with information about the completion of steps
    if count_unique:
        # When counting unique users
        step_indicators = [col for col in df.columns if col.startswith('step') and '_users' in col]
        if not step_indicators:
            step_indicators = [col for col in df.columns if col.startswith('e') and '_name' in col]
    else:
        # When counting the total number of events
        step_indicators = [col for col in df.columns if col.startswith('step') and '_events' in col]
        if not step_indicators:
            step_indicators = [col for col in df.columns if col.startswith('e') and '_name' in col]
    
    if not step_indicators:
        raise ValueError("Columns indicating funnel steps were not found")
    
    # Sort columns by step number
    step_indicators.sort(key=lambda x: int(''.join(filter(str.isdigit, x.split('_')[0]))))
    
    # Checking if there are columns with the names of steps
    name_columns = [col for col in df.columns if col.startswith('e') and '_name' in col]
    name_columns.sort(key=lambda x: int(''.join(filter(str.isdigit, x.split('_')[0]))))
    
    # Determining the names of the steps
    default_step_names = []
    
    # If the names of the steps are passed as a parameter, use them
    if step_names is not None:
        default_step_names = step_names[:len(step_indicators)]
    # Otherwise, if there are columns e*_name, extract names from them
    elif name_columns and not df.empty:
        for col in name_columns[:len(step_indicators)]:
            # Take the first non-empty value from the column
            name = df[col].dropna().iloc[0] if not df[col].dropna().empty else f"Step {int(''.join(filter(str.isdigit, col.split('_')[0]))) + 1}"
            default_step_names.append(name)
    
    # If the names are still not defined, use the default values
    if not default_step_names:
        default_step_names = [f"Step {i+1}" for i in range(len(step_indicators))]
    
    # Conversion calculation function for a group
    def calculate_group_conversion(group_df):
        result = {}
        
        # We check how steps are defined (indicators or names)
        if ('_users' in step_indicators[0] and count_unique) or ('_events' in step_indicators[0] and not count_unique):
            # For funnels with special columns step1_users/step1_events
            if count_unique:
                # We count unique users at every step
                total_users = len(group_df['user_id'].unique())
                result['total_users'] = total_users
                
                # We count users at every step
                for i, step in enumerate(step_indicators):
                    step_users = group_df[group_df[step] > 0]['user_id'].nunique()
                    result[f'step{i+1}_users'] = step_users
                
                # We use step*_users for calculations
                step_columns = [f'step{i+1}_users' for i in range(len(step_indicators))]
            else:
                # We count the total number of events at each step
                total_events = len(group_df)
                result['total_events'] = total_events
                
                # We count events at every step
                for i, step in enumerate(step_indicators):
                    step_events = group_df[step].sum()  # Let's sum it up, as it may be > 1 per user
                    result[f'step{i+1}_events'] = step_events
                
                # We use step*_events for calculations
                step_columns = [f'step{i+1}_events' for i in range(len(step_indicators))]
            
            # Adding names of steps from a specific list
            for i, name in enumerate(default_step_names[:len(step_indicators)]):
                result[f'step{i+1}_name'] = name
                
        else:
            # For funnels with events e0_name, e1_name etc.
            step_names = []
            for i, step in enumerate(step_indicators):
                step_name = group_df[step].iloc[0] if not group_df.empty else default_step_names[i]
                step_names.append(step_name)
                
                if count_unique:
                    # We count unique users at every step
                    step_users = group_df[group_df[step].notna()]['user_id'].nunique()
                    result[f'step{i+1}_users'] = step_users
                else:
                    # We count the total number of events at each step
                    step_events = group_df[step].notna().sum()
                    result[f'step{i+1}_events'] = step_events
            
            if count_unique:
                # To count unique users
                total_users = len(group_df['user_id'].unique())
                result['total_users'] = total_users
                # We use step*_users for calculations
                step_columns = [f'step{i+1}_users' for i in range(len(step_indicators))]
            else:
                # To count the total number of events
                total_events = len(group_df)
                result['total_events'] = total_events
                # We use step*_events for calculations
                step_columns = [f'step{i+1}_events' for i in range(len(step_indicators))]
            
            # Adding step names
            for i, name in enumerate(step_names):
                result[f'step{i+1}_name'] = name
        
        # Total Conversion
        first_step = step_columns[0]
        last_step = step_columns[-1]
        
        first_step_value = result[first_step]
        last_step_value = result[last_step]
        
        if first_step_value > 0:
            result['total_conversion'] = round((last_step_value / first_step_value) * 100, 2)
        else:
            result['total_conversion'] = 0
        
        # Conversion between steps
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
    
    # If the grouping is not specified, we calculate the conversion for the entire DataFrame
    if group_by is None:
        conversion_data = calculate_group_conversion(df)
        return pd.DataFrame([conversion_data])
    
    # If a grouping is specified, we calculate the conversion for each group
    if isinstance(group_by, str):
        group_by = [group_by]
    
    # Checking for the presence of grouping columns
    missing_columns = [col for col in group_by if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Columns {missing_columns} not found in DataFrame")
    
    # Getting unique combinations of grouping values
    group_combinations = df[group_by].drop_duplicates()
    
    # We calculate the conversion for each group
    results = []
    for _, group_values in group_combinations.iterrows():
        # We create a filtering condition for the group
        group_filter = True
        for col in group_by:
            group_filter &= (df[col] == group_values[col])
        
        # Getting group data
        group_df = df[group_filter]
        
        # Calculating conversion for a group
        group_result = calculate_group_conversion(group_df)
        
        # Adding grouping values
        for col in group_by:
            group_result[col] = group_values[col]
        
        results.append(group_result)
    
    # Create a DataFrame from the results
    result_df = pd.DataFrame(results)
    
    # Rearranging columns for better readability
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
    
    # Keep only the existing columns and add the rest at the end
    existing_columns = [col for col in columns_order if col in result_df.columns]
    other_columns = [col for col in result_df.columns if col not in columns_order]
    final_columns = existing_columns + other_columns
    
    # Sort the results by grouping
    result_df = result_df[final_columns].sort_values(by=group_by).reset_index(drop=True)
    
    return result_df