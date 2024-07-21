"""
Data Transformer Module

This module provides a function to remove specified columns from a DataFrame.
"""

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

import logging

@transformer
def transform(df, **kwargs):
    """
    Remove specified columns from a DataFrame.

    Args:
        df (DataFrame): The input DataFrame.
        **kwargs: Additional keyword arguments.
            columns_list_rm (list): List of column names to remove from the DataFrame.

    Returns:
        DataFrame: DataFrame with specified columns removed.
    """
    columns_list_rm = kwargs.get('columns_list_rm', [])

    if not columns_list_rm:
        logging.warning("No columns provided to remove.")

    logging.info(f"The columns {columns_list_rm} are removed from the DataFrame.")
    
    df.drop(columns=columns_list_rm, inplace=True)

    return df
