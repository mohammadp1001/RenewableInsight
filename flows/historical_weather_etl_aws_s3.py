import sys
import json
import boto3
import logging
import datetime
import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq

from io import BytesIO
from pandas import DataFrame
from prefect import task, flow
from prefect import get_run_logger

if '/home/mohammad/RenewableInsight' not in sys.path:
    sys.path.append('/home/mohammad/RenewableInsight')

from src.config import Config
from src.api.parameters import WeatherParameter
from src.api.weather import WeatherDataDownloader
from src.utilities.utils import create_s3_keys_historical_weather, generate_random_string, check_s3_key_exists, generate_task_name, generate_flow_name

try:
    config = Config()
    print("configuration loaded successfully!")
except ValidationError as e:
    print("configuration error:", e)
    
@task(task_run_name=generate_task_name)
def load_data() -> pd.DataFrame:
    """
    Load weather data for a given station and weather parameter.

    :return: A pandas DataFrame with the loaded and cleaned weather data.
    """
    weather_param = WeatherParameter.from_name(config.WEATHER_PARAM)

    downloader = WeatherDataDownloader()
    data = downloader.download_and_load_data(config.STATION_CODE, weather_param)

    return data

@task(task_run_name=generate_task_name)
def transform(data: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the weather data by cleaning and processing it.

    :param data: The raw weather data as a pandas DataFrame.
    :return: A cleaned and transformed pandas DataFrame.
    """
    weather_param = WeatherParameter.from_name(config.WEATHER_PARAM)

    data = data.drop(columns=weather_param.columns_rm, errors='ignore')

    data['MESS_DATUM'] = data['MESS_DATUM'].astype('str')
    if "ST" in config.WEATHER_PARAM:
        data['MESS_DATUM'] = pd.to_datetime(data['MESS_DATUM'].str.slice(stop=10), format='%Y%m%d%H')
    else:
        data['MESS_DATUM'] = pd.to_datetime(data['MESS_DATUM'], format='%Y%m%d%H')

    data = data.rename(columns={"MESS_DATUM": "measurement_time"})
    data = data.drop(columns="STATIONS_ID")

    data['day'] = data['measurement_time'].dt.day
    data['month'] = data['measurement_time'].dt.month
    data['year'] = data['measurement_time'].dt.year
    data['hour'] = data['measurement_time'].dt.hour

    for col in data.columns:
        if pd.api.types.is_integer_dtype(data[col]):
            data[col] = data[col].astype('int16')
        if pd.api.types.is_float_dtype(data[col]):
            data[col] = data[col].astype('float32')

    return data

@task(task_run_name=generate_task_name)
def export_data_to_s3(data: DataFrame) -> None:
    """
    Export the transformed weather data to an S3 bucket in Parquet format.

    :param data: The transformed weather data as a pandas DataFrame.
    :return: None
    """
    logger = get_run_logger()
    bucket_name = config.BUCKET_NAME

    s3 = boto3.client('s3', aws_access_key_id=config.AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY)

    for object_key, date in create_s3_keys_historical_weather(config.WEATHER_PARAM, config.STATION_CODE):
        data_ = data[(data['day'] == date.day) & (data['month'] == date.month) & (data['year'] == date.year)]
    
        if data_.empty:
            logger.info("The dataframe is empty.")
            continue

        parquet_buffer = BytesIO()
        table = pa.Table.from_pandas(data_)
        pq.write_table(table, parquet_buffer)

        parquet_buffer.seek(0)

        filename = object_key + f"/{generate_random_string(10)}.parquet"

        if not check_s3_key_exists(s3, bucket_name, object_key):
            s3.put_object(
                Bucket=bucket_name,
                Key=filename,
                Body=parquet_buffer.getvalue()
            )
            logger.info(f"File has been written to s3 {bucket_name} inside {object_key}.")
        else:
            logger.info(f"File {object_key} already exists.")
      
        parquet_buffer.close()    

@flow(log_prints=True,name="historical_weather_etl_aws_s3",flow_run_name=generate_flow_name)
def etl() -> None:
    """
    The ETL flow that orchestrates the loading, transforming, and exporting of historical weather data.

    :return: None
    """
    data = load_data()
    transformed_data = transform(data)
    export_data_to_s3(transformed_data)

if __name__ == "__main__":
    etl()
