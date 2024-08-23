import sys
import json
import boto3
import logging
import datetime
import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq

if '/home/mohammad/RenewableInsight' not in sys.path:
    sys.path.append('/home/mohammad/RenewableInsight')

from io import BytesIO
from pandas import DataFrame
from prefect import task, flow

from src.config import Config
from src.api.parameters import WeatherParameter
from src.api.weather import WeatherDataDownloader
from src.utilities.utils import create_s3_keys_historical_weather, generate_random_string, check_s3_key_exists

@task
def load_data():
    """
    Load weather data for a given station and weather parameter.
    
    :param station_code: The code of the weather station.
    :param weather_param: The WeatherParameter enum member specifying the type of weather data.
    :return: A pandas DataFrame with the loaded and cleaned weather data.
    """

    
    weather_param = WeatherParameter.from_name(Config.WEATHER_PARAM) 

    downloader = WeatherDataDownloader()
    data = downloader.download_and_load_data(Config.STATION_CODE,weather_param)

    return data

@task
def transform(data):
    """
    """
    weather_param = WeatherParameter.from_name(Config.WEATHER_PARAM) 

    data = data.drop(columns=weather_param.columns_rm, errors='ignore')
    
    data.MESS_DATUM = data.MESS_DATUM.astype('str')
    if "ST" in Config.WEATHER_PARAM:
        data['MESS_DATUM'] = pd.to_datetime(data['MESS_DATUM'].str.slice(stop=10), format='%Y%m%d%H')
    else:    
        data.MESS_DATUM = pd.to_datetime(data.MESS_DATUM, format='%Y%m%d%H')
    data = data.rename(columns = {"MESS_DATUM":"measurment_time"})
    data = data.drop(columns="STATIONS_ID")

    data.loc[:,'day'] = data.measurment_time.dt.day
    data.loc[:,'month'] = data.measurment_time.dt.month
    data.loc[:,'year'] = data.measurment_time.dt.year
    data.loc[:,'hour'] = data.measurment_time.dt.hour

    for col in data.columns:
        if pd.api.types.is_integer_dtype(data[col]):
            data[col] = data[col].astype('int16')
        if pd.api.types.is_float_dtype(data[col]):
            data[col] = data[col].astype('float32')
    print(data.shape)            
    return data

@task(log_prints=True)
def export_data_to_s3(data: DataFrame) -> None:

    parquet_buffer = BytesIO()
    

    bucket_name = Config.BUCKET_NAME
    
    s3 = boto3.client('s3', aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY)

    for object_key, date in create_s3_keys_historical_weather(Config.WEATHER_PARAM,Config.STATION_CODE):
        data_ = data[(data.day == date.day) & (data.month == date.month) & (data.year == date.year)]
        if data_.empty:
            logger.info("The dataframe is empty.")
            break
        table = pa.Table.from_pandas(data_)
        pq.write_table(table, parquet_buffer)
        filename = object_key + f"/{generate_random_string(10)}.parquet"
        if not check_s3_key_exists(s3,bucket_name,object_key):
            s3.put_object(
                Bucket=bucket_name,
                Key=filename,
                Body=parquet_buffer.getvalue()
                )
            logging.info(f"File has been written to s3 {bucket_name} inside {object_key}.")
            print(f"File has been written to s3 {bucket_name} inside {object_key}.")
        else:
            print(f"File {object_key} already exists.")    
            logging.info(f"File {object_key} already exists.")

# Defining the flow
@flow(log_prints=True)
def etl():
    data = load_data()
    transformed_data = transform(data)
    export_data_to_s3(transformed_data)

# Running the flow
if __name__ == "__main__":
    etl()