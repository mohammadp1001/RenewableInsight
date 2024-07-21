"""
Data Transformer Module

This module provides a function to filter a DataFrame for data related to Germany.
"""

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

@transformer
def transform(df, **kwargs):
    """
    Filter dataframe for Germany.

    Args:
        df (DataFrame): The input DataFrame.
        **kwargs: Additional keyword arguments.

    Returns:
        DataFrame: Filtered dataframe containing data related to Germany.
    """
    return df[df['MapCode'].str.startswith('DE')]
