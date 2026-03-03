"""
Conversion funnel visualization module.
"""

import warnings
import pandas as pd
import re


def visualize_funnel(df, title: str = "User funnel") -> None:
    """
    Visualizes the funnel based on data.
    
    Args:
        df: DataFrame with funnel data
        title: Graph title
    """
    try:
        import matplotlib.pyplot as plt
        import seaborn as sns
    except ImportError:
        warnings.warn("For visualization you need to install matplotlib and seaborn.")
        return
    
    # Checking for columns with the number of unique users
    user_columns = [col for col in df.columns if col.startswith('step') and col.endswith('_users')]
    
    # If there are no columns with users, check the columns with events
    if not user_columns:
        user_columns = [col for col in df.columns if col.startswith('step') and col.endswith('_events')]
        
    if not user_columns:
        raise ValueError("Columns with number of users were not found (step*_users) or events (step*_events)")
    
    # Sort the columns by step number
    user_columns.sort(key=lambda x: int(re.search(r'(\d+)', x).group(1)))
    
    # If there are several groups, select the first one for visualization
    if len(df) > 1 and 'group_value' in df.columns:
        first_group = df.iloc[0]['group_value']
        warnings.warn(f"DataFrame contains several groups. Only the group is rendered '{first_group}'. "
                     f"To compare groups, use the compare method_funnels().")
        df_to_viz = df[df['group_value'] == first_group].iloc[[0]]
    else:
        df_to_viz = df.iloc[[0]] if len(df) > 0 else df
    
    # Finding columns with step names
    step_name_columns = [col for col in df.columns if col.startswith('step') and col.endswith('_name')]
    has_step_names = len(step_name_columns) > 0
    
    # If there are columns with names of steps, use them
    if has_step_names:
        step_name_columns.sort(key=lambda x: int(re.search(r'(\d+)', x).group(1)))
        step_names = [df_to_viz[col].iloc[0] if not pd.isna(df_to_viz[col].iloc[0]) else f"Step {i+1}"
                     for i, col in enumerate(step_name_columns)]
        
        # Checking that we have a name for each step
        if len(step_names) < len(user_columns):
            # We supplement the names of steps if there are fewer of them than columns with data
            step_names.extend([f"Step {i+1+len(step_names)}" for i in range(len(user_columns) - len(step_names))])
    else:
        # If there are no columns with names, we use standard ones
        step_names = [f"Step {i+1}" for i in range(len(user_columns))]
    
    # Extracting data for visualization
    if len(df_to_viz) > 0:
        users = [df_to_viz[col].iloc[0] for col in user_columns]
    else:
        warnings.warn("The provided DataFrame is empty. Visualization not possible.")
        return
    
    # Plotting a graph
    plt.figure(figsize=(12, 6))
    sns.set_style("whitegrid")
    
    # We use colors that are easily distinguishable
    colors = ['#3498db', '#2ecc71', '#e74c3c', '#f39c12', '#9b59b6', '#1abc9c', '#e67e22', '#34495e']
    bars = plt.bar(range(len(users)), users, width=0.6, color=colors[:len(users)])
    
    # Adding Value Labels to Columns
    for i, (bar, value) in enumerate(zip(bars, users)):
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                 f'{int(value)}',
                 ha='center', va='bottom', fontweight='bold')
        
        # Adding conversion percentages between columns
        if i > 0:
            prev_value = users[i-1]
            curr_value = value
            conversion = (curr_value / prev_value * 100) if prev_value > 0 else 0
            x_pos = bar.get_x() - 0.15
            y_pos = (height + users[i-1]) / 2
            plt.text(x_pos, y_pos, f"{conversion:.1f}%", ha='center', va='center',
                    fontsize=9, rotation=90, color='#2c3e50', fontweight='bold')
    
    # Setting up axes and labels
    plt.xticks(range(len(users)), step_names, rotation=15, ha='center')
    plt.ylabel('Number of users', fontsize=12)
    plt.title(title, fontsize=14, fontweight='bold')
    
    # Adding the conversion percentage from the first to the last step
    if len(users) > 1:
        total_conversion = (users[-1] / users[0] * 100) if users[0] > 0 else 0
        plt.figtext(0.5, 0.01, f'Total Conversion: {total_conversion:.1f}%',
                  ha='center', fontsize=12, fontweight='bold')
    
    plt.tight_layout()
    plt.show()