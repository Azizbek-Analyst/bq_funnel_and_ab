"""
Module for comparing several funnels on one chart.
"""

import warnings
import re
import pandas as pd
from typing import List


def compare_funnels(dfs: List, labels: List[str], title: str = "Funnel comparison") -> None:
    """
    Compares multiple funnels on one chart.
    
    Args:
        dfs: List of DataFrames with funnel data
        labels: List of labels for each funnel
        title: Graph title
    """
    try:
        import matplotlib.pyplot as plt
        import seaborn as sns
        import numpy as np
    except ImportError:
        warnings.warn("For visualization you need to install matplotlib and seaborn.")
        return
    
    if len(dfs) != len(labels):
        raise ValueError("The number of DataFrames must match the number of labels.")
    
    # Determine the type of columns (users or events)
    first_df = dfs[0]
    user_columns = [col for col in first_df.columns if col.startswith('step') and col.endswith('_users')]
    if not user_columns:
        user_columns = [col for col in first_df.columns if col.startswith('step') and col.endswith('_events')]
    
    if not user_columns:
        raise ValueError("Columns with number of users were not found (step*_users) or events (step*_events)")
    
    # Sort the columns by step number
    user_columns.sort(key=lambda x: int(re.search(r'(\d+)', x).group(1)))
    
    # Finding columns with step names
    step_name_columns = [col for col in first_df.columns if col.startswith('step') and col.endswith('_name')]
    has_step_names = len(step_name_columns) > 0
    
    # If there are columns with names of steps, use them
    if has_step_names:
        step_name_columns.sort(key=lambda x: int(re.search(r'(\d+)', x).group(1)))
        # We take the names from the first DataFrame
        if len(first_df) > 0:
            step_names = [first_df[col].iloc[0] if not pd.isna(first_df[col].iloc[0]) else f"Step {i+1}"
                          for i, col in enumerate(step_name_columns)]
            
            # We supplement the names of steps if there are fewer of them than columns with data
            if len(step_names) < len(user_columns):
                step_names.extend([f"Step {i+1+len(step_names)}" for i in range(len(user_columns) - len(step_names))])
        else:
            step_names = [f"Step {i+1}" for i in range(len(user_columns))]
    else:
        # If there are no columns with names, we use standard ones
        step_names = [f"Step {i+1}" for i in range(len(user_columns))]
    
    # Creating a graph to compare funnels
    plt.figure(figsize=(14, 7))
    sns.set_style("whitegrid")
    
    bar_width = 0.8 / len(dfs)
    colors = ['#3498db', '#2ecc71', '#e74c3c', '#f39c12', '#9b59b6', '#1abc9c', '#e67e22', '#34495e']
    
    for i, (df, label) in enumerate(zip(dfs, labels)):
        # If the DataFrame contains a grouping, use only the first row
        if len(df) > 1 and 'group_value' in df.columns:
            warnings.warn(f"DataFrame for a group '{label}' contains several lines. Only the first line is used.")
            df_to_viz = df.iloc[[0]]
        else:
            df_to_viz = df.iloc[[0]] if len(df) > 0 else df
            
        if len(df_to_viz) == 0:
            warnings.warn(f"DataFrame for a group '{label}' empty Group skipped.")
            continue
        
        # Getting data for the current funnel
        users = [df_to_viz[col].iloc[0] for col in user_columns]
        
        # Positions for drawing columns (with offset for different groups)
        positions = np.arange(len(user_columns)) + (i - len(dfs)/2 + 0.5) * bar_width
        
        # Drawing Columns
        bars = plt.bar(positions, users, width=bar_width, label=label, 
                       color=colors[i % len(colors)], alpha=0.8)
        
        # Adding values ​​to columns
        for j, (bar, value) in enumerate(zip(bars, users)):
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                     f'{int(value)}',
                     ha='center', va='bottom', fontsize=8)
            
            # Adding conversion percentages for each group
            if j > 0:
                prev_value = users[j-1]
                curr_value = value
                conversion = (curr_value / prev_value * 100) if prev_value > 0 else 0
                # We show the conversion only for the first and last groups, so as not to clutter the graph
                if i == 0 or i == len(dfs) - 1:
                    x_pos = bar.get_x() + bar.get_width()/2
                    y_pos = height / 2
                    plt.text(x_pos, y_pos, f"{conversion:.1f}%", ha='center', va='center',
                            fontsize=7, rotation=90, color='white', fontweight='bold')
    
    # Setting up axes and labels
    plt.title(title, fontsize=14, fontweight='bold')
    plt.ylabel("Number of users", fontsize=12)
    plt.xticks(np.arange(len(user_columns)), step_names, rotation=15)
    plt.legend(title="Group", fontsize=10)
    
    # Adding the total conversion for each group to the legend
    handles, labels_current = plt.gca().get_legend_handles_labels()
    
    # Update labels with conversion information
    for i, (df, label) in enumerate(zip(dfs, labels)):
        if len(df) > 0:
            df_to_viz = df.iloc[[0]]
            users = [df_to_viz[col].iloc[0] for col in user_columns]
            if len(users) > 1 and users[0] > 0:
                total_conversion = (users[-1] / users[0] * 100)
                labels_current[i] = f"{label} (conv: {total_conversion:.1f}%)"
    
    plt.legend(handles, labels_current, title="Group", fontsize=10)
    
    plt.tight_layout()
    plt.show()
    
    # Additional graph: Comparing conversions between steps
    if len(user_columns) > 1:
        plt.figure(figsize=(14, 7))
        
        # Preparing conversion data between steps
        conversion_data = []
        
        for i, (df, label) in enumerate(zip(dfs, labels)):
            if len(df) > 0:
                df_to_viz = df.iloc[[0]]
                users = [df_to_viz[col].iloc[0] for col in user_columns]
                
                # Calculation of conversions between steps
                for j in range(1, len(user_columns)):
                    prev_value = users[j-1]
                    curr_value = users[j]
                    conversion = (curr_value / prev_value * 100) if prev_value > 0 else 0
                    conversion_data.append({
                        'Group': label,
                        'Transition': f"{step_names[j-1]} → {step_names[j]}",
                        'Conversion (%)': conversion
                    })
                
                # Adding total conversion
                total_conversion = (users[-1] / users[0] * 100) if users[0] > 0 else 0
                conversion_data.append({
                    'Group': label,
                    'Transition': 'Total Conversion',
                    'Conversion (%)': total_conversion
                })
        
        # Creating a DataFrame for plotting
        conversion_df = pd.DataFrame(conversion_data)
        
        # Create a conversion comparison graph
        ax = sns.barplot(
            data=conversion_df,
            x='Transition',
            y='Conversion (%)',
            hue='Group',
            palette=colors[:len(dfs)]
        )
        
        # Adding values ​​to columns
        for container in ax.containers:
            ax.bar_label(container, fmt='%.1f%%', fontsize=8)
        
        plt.title(f"{title} - Comparing conversions between steps", fontsize=14, fontweight='bold')
        plt.ylabel("Conversion (%)", fontsize=12)
        plt.xticks(rotation=15)
        plt.tight_layout()
        plt.show()