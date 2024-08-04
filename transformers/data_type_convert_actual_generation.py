"""
Data Transformer Module

This module provides a function to transform integer data types in a DataFrame.
"""

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
from pandas import DataFrame
import pandas as pd

@transformer
def transform(df: DataFrame, **kwargs) -> DataFrame:
    """
    Convert integer data types.

    Args:
        df (DataFrame): The input DataFrame.
        **kwargs: Additional keyword arguments.

    Returns:
        DataFrame: The converted DataFrame.
    """
    df['month'] = df['month'].astype('int8')
    df['year'] = df['year'].astype('int16')
    df['hour'] = df['hour'].astype('int8')
    df['minute'] = df['minute'].astype('int8')
    df['day'] = df['day'].astype('int8')

    return df