from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.s3 import S3
from pandas import DataFrame
from os import path
import boto3
from RenewableInsight.src.utils import create_s3_keys_historical_weather

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_s3(df: DataFrame, **kwargs) -> None:
    """
    Exporting data to a S3 bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#s3
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    station_code = kwargs['station_code']
    weather_param = weather_param = kwargs['weather_param']

    # Base bucket name and path construction
    bucket_name = kwargs.get('bucket_name', 'renewableinsightbucket')
   
    for object_key,date in create_s3_keys_historical_weather(weather_param,station_code):
        df_date = df[(df.day==date.day)&(df.month==date.month)&(df.year==date.year)]
        S3.with_config(ConfigFileLoader(config_path, config_profile)).export(
            df_date.reset_index(drop=True),
            bucket_name,
            object_key,
        )
