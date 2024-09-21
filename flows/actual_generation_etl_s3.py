import sys
import boto3
import pytz
import logging
import datetime
import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq

from io import BytesIO
from pandas import DataFrame
from prefect import task, flow
from prefect import get_run_logger

from src.config import Config
from src.api.entsoe_api import ENTSOEAPI
from src.utilities.utils import create_s3_keys_generation, check_s3_key_exists, generate_random_string, generate_task_name, generate_flow_name

try:
    config = Config()
except ValidationError as e:
    print("configuration error:", e)
    
@task(task_run_name=generate_task_name)
def load_data(year: int, month: int, country_code: str, data_type: str) -> DataFrame:
    """
    Load data from the ENTSO-E API.

    :param: year
    :param: month
    :param: country_code
    :param: data_type
    :return: A pandas DataFrame containing the loaded data.
    """
    data_downloader = ENTSOEAPI(
        year=year,
        month=month,
        country_code=country_code,
        api_key=config.ENTSOE_API_KEY
    )
    data_downloader.fetch_data(data_type=data_type)
    data = data_downloader.data

    return data

@task(task_run_name=generate_task_name)
def transform(data: DataFrame) -> DataFrame:
    """
    Transform the DataFrame by converting datetime columns and adding additional time-based columns.

    :param data: The input DataFrame to be transformed.
    :return: The transformed DataFrame with additional columns.
    """
    berlin_tz = pytz.timezone(config.TIMEZONE)

    data['date'] = pd.to_datetime(data['date'], format="ISO8601")
    data['date'] = data['date'].dt.tz_convert(berlin_tz)
    
    data['day'] = data['date'].dt.day
    data['month'] = data['date'].dt.month
    data['year'] = data['date'].dt.year
    data['hour'] = data['date'].dt.hour
    data['minute'] = data['date'].dt.minute

    data['year'] = data['year'].astype('int16')
    data['month'] = data['month'].astype('int8')
    data['hour'] = data['hour'].astype('int8')
    data['minute'] = data['minute'].astype('int8')
    data['day'] = data['day'].astype('int8')

    return data

@task(task_run_name=generate_task_name)
def export_data_to_s3(data: DataFrame) -> None:
    """
    Export the transformed data to an S3 bucket in Parquet format.

    :param data: The transformed DataFrame to be exported.
    :return: None
    """
    
    logger = get_run_logger()
    bucket_name = config.BUCKET_NAME
    s3 = boto3.client(
        's3', 
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY
    )

    for object_key, date in create_s3_keys_generation():
        data_ = data[(data.day == date.day) & 
                     (data.month == date.month) & 
                     (data.year == date.year)]
        if data_.empty:
            logger.info("The dataframe is empty possibly due to lack of messages.")
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

@flow(log_prints=True, name="actual_generation_etl_s3", flow_run_name=generate_flow_name)
def actual_generation_etl_flow(year: int, month: int, country_code: str, data_type: str) -> None:
    """
    The ETL flow that orchestrates the loading, transforming, and exporting of generation data.
    
    :param: year
    :param: month
    :param: country_code
    :param: data_type
    :return: None
    """
    raw_data = load_data(year, month, country_code, data_type)
    transformed_data = transform(raw_data)
    export_data_to_s3(transformed_data)

if __name__ == "__main__":
    pass
