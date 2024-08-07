

from RenewableInsight.src.WeatherDataDownloader import WeatherDataDownloader,WeatherParameter


@task
def load_data():
    """
    Load weather data for a given station and weather parameter.
    
    :param station_code: The code of the weather station.
    :param weather_param: The WeatherParameter enum member specifying the type of weather data.
    :return: A pandas DataFrame with the loaded and cleaned weather data.
    """
    station_code = kwargs['station_code']
    weather_param = WeatherParameter[kwargs['weather_param']] 
    downloader = WeatherDataDownloader()
    data = downloader.download_and_load_weather_data(station_code,weather_param)

    
    return data




@task
def transform(data):
    """
    """
    weather_param = WeatherParameter[kwargs['weather_param']] 

    # Remove the unnecessary columns specified in the WeatherParameter enum
    data = data.drop(columns=weather_param.columns_rm, errors='ignore')
    
    
    return data

@task
def transform(data):
    """
    """
    data.MESS_DATUM = data.MESS_DATUM.astype('str')
    if kwargs['weather_param'] == "ST":
        data['MESS_DATUM'] = pd.to_datetime(data['MESS_DATUM'].str.slice(stop=10), format='%Y%m%d%H')
    else:    
        data.MESS_DATUM = pd.to_datetime(data.MESS_DATUM, format='%Y%m%d%H')
    data = data.rename(columns = {"MESS_DATUM":"measurment_time"})
    data = data.drop(columns="STATIONS_ID")

    return data


@task
def transform(data):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    data.loc[:,'day'] = data.measurment_time.dt.day
    data.loc[:,'month'] = data.measurment_time.dt.month
    data.loc[:,'year'] = data.measurment_time.dt.year
    data.loc[:,'hour'] = data.measurment_time.dt.hour

    return data



if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd
@transformer
def transform(data, *args, **kwargs):
    """
    """
    # Iterate over each column in the DataFrame
    for col in data.columns:
        # If the column is of integer type
        if pd.api.types.is_integer_dtype(data[col]):
            # Convert the column to int16
            data[col] = data[col].astype('int16')
        # If the column is of float type    
        if pd.api.types.is_float_dtype(data[col]):
            # Convert the column to float32
            data[col] = data[col].astype('float32')    
    return data

    return data


from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.s3 import S3
from pandas import DataFrame
from os import path
import boto3
from RenewableInsight.src.utils import create_s3_keys_historical_weather


@task
def export_data_to_s3(data: DataFrame, **kwargs) -> None:
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
        data_date = data[(data.day==date.day)&(data.month==date.month)&(data.year==date.year)]
        S3.with_config(ConfigFileLoader(config_path, config_profile)).export(
            data_date.reset_index(drop=True),
            bucket_name,
            object_key,
        )
