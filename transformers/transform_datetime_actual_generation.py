"""
Data Transformer Module

This module provides a function to transform datetime data and add additional columns to a DataFrame.
"""

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
import pandas as pd
from pandas import DataFrame

@transformer
def transform(df: DataFrame, *args, **kwargs) -> DataFrame:
    """
    Transform datetime and add additional columns.

    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: The transformed DataFrame with additional columns.
    """
    # Transform datetime
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d %H:%M:%S.%f')

    # Extract date and time components
    df['day'] = df['date'].dt.day
    df['month'] = df['date'].dt.month
    df['year'] = df['date'].dt.year
    df['hour'] = df['date'].dt.hour
    df['minute'] = df['date'].dt.minute

    return df
