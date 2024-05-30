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
    df['DateTime'] = pd.to_datetime(df['DateTime'], format='%Y-%m-%d %H:%M:%S.%f')

    # Extract date and time components
    df['Day'] = df['DateTime'].dt.day
    df['Month'] = df['DateTime'].dt.month
    df['Year'] = df['DateTime'].dt.year
    df['Hour'] = df['DateTime'].dt.hour
    df['Minute'] = df['DateTime'].dt.minute

    return df
