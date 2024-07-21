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
    df['Month'] = df['Month'].astype('int8')
    df['Year'] = df['Year'].astype('int16')
    df['Hour'] = df['Hour'].astype('int8')
    df['Minute'] = df['Minute'].astype('int8')
    df['Day'] = df['Day'].astype('int8')

    return df