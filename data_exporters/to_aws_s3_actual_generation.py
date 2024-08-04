"""
Data Exporter to S3 Module

This module provides a function to export data to an S3 bucket.
"""

import logging
import pandas as pd
import boto3

from os import path
from pandas import DataFrame
from datetime import datetime
from mage_ai.io.s3 import S3
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader

from src.utilities.utils import create_s3_keys_generation, check_s3_key_exists, generate_random_string

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
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    bucket_name = kwargs['bucket_name']
    
    s3 = boto3.client(
    's3',
    aws_access_key_id='your_access_key_id',
    aws_secret_access_key='your_secret_access_key'
    )

    for object_key, date in create_s3_keys_generation():
        df_date = df[(df.day == date.day) & (df.month == date.month) & (df.year == date.year)]
        filename = object_key + f"/{generate_random_string(10)}.parquet"
        if not check_s3_key_exists(bucket_name=bucket_name, object_key=object_key):
            S3.with_config(ConfigFileLoader(config_path, config_profile)).export(
                df_date.reset_index(drop=True),
                bucket_name,
                filename,
                )
            logging.info(f"File has been written to s3 {bucket_name} inside {object_key}.")
        else:
            logging.info(f"File {object_key} already exists.")



