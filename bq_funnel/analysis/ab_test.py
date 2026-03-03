"""
Statistical Analysis Module A/B-tests based on funnel data.
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
    try:
        from scipy import stats
    except ImportError:
        warnings.warn("Statistical analysis requires scipy to be installed.")
        return {"error": "Statistical analysis requires scipy to be installed."}
    
    # If the last step is not specified, we find it automatically
    if last_step is None:
        step_columns = [col for col in control_df.columns if col.startswith('step') and col.endswith('_users')]
        step_columns.sort(key=lambda x: int(x.replace('step', '').replace('_users', '')))
        last_step = step_columns[-1]
    
    # Extracting conversion data
    control_start = control_df[first_step].iloc[0]
    control_end = control_df[last_step].iloc[0]
    test_start = test_df[first_step].iloc[0]
    test_end = test_df[last_step].iloc[0]
    
    # Conversion calculation
    control_conv_rate = control_end / control_start if control_start > 0 else 0
    test_conv_rate = test_end / test_start if test_start > 0 else 0
    
    # Absolute difference in conversion
    abs_diff = (test_conv_rate - control_conv_rate) * 100
    
    # Relative improvement
    rel_lift = ((test_conv_rate / control_conv_rate) - 1) * 100 if control_conv_rate > 0 else 0
    
    # Statistical test (z-test for proportions)
    control_success = control_end
    control_total = control_start
    test_success = test_end
    test_total = test_start
    
    # Calculation of standard error and z-score
    p1 = control_success / control_total if control_total > 0 else 0
    p2 = test_success / test_total if test_total > 0 else 0
    p_pool = (control_success + test_success) / (control_total + test_total) if (control_total + test_total) > 0 else 0
    
    se = np.sqrt(p_pool * (1 - p_pool) * (1/control_total + 1/test_total)) if p_pool > 0 and p_pool < 1 else 0
    
    # Avoiding division by zero
    if se > 0:
        z_score = (p2 - p1) / se
        p_value = 2 * (1 - stats.norm.cdf(abs(z_score)))  # Two-sided test
    else:
        z_score = 0
        p_value = 1.0
    
    # Determining the significance of the result
    alpha = 1 - confidence_level
    is_significant = p_value < alpha
    
    # Formation of recommendations
    if is_significant and rel_lift > 0:
        recommendation = "The test results show a statistically significant improvement. It is recommended to implement changes."
    elif is_significant and rel_lift < 0:
        recommendation = "The test results show a statistically significant deterioration. It is recommended to reject the changes."
    else:
        recommendation = "The test results show no statistically significant changes. It is recommended to continue the experiment or consider other options."
    
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