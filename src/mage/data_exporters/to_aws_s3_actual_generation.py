"""
Data Exporter to S3 Module

This module provides a function to export data to an S3 bucket.
"""

import logging
from datetime import datetime
from os import path
from pandas import DataFrame
import pandas as pd
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.s3 import S3
from RenewableInsight.src.utils import create_s3_keys_dates

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_s3(df: DataFrame, **kwargs) -> None:
    """
    Export data to a S3 bucket.

    Args:
        df (DataFrame): The DataFrame to be exported.
        **kwargs: Additional keyword arguments.
            Required:
                - bucket_name (str): The name of the S3 bucket.
                - export_mode (str): The export mode ('daily' or other).
                - data_item_name (str): The name of the data item.
                - data_item_no (int): The number of data items.

    Docs: https://docs.mage.ai/design/data-loading#s3
    """
    # Ensure required keys are present in kwargs
    required_keys = ['bucket_name', 'export_mode', 'data_item_name', 'data_item_no']
    for key in required_keys:
        if key not in kwargs:
            raise ValueError(f"Missing required key '{key}' in kwargs.")

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    bucket_name = kwargs['bucket_name']
    
    if kwargs['export_mode'] == 'daily':
        for object_key, date in create_s3_keys_dates(kwargs['data_item_name'], kwargs['data_item_no']):
            df_date = df[(df.Day == date.day) & (df.Month == date.month) & (df.Year == date.year)]
            logging.info(f"File {object_key} has been written to aws s3.")
            S3.with_config(ConfigFileLoader(config_path, config_profile)).export(
                df_date.reset_index(drop=True),
                bucket_name,
                object_key,
            )


