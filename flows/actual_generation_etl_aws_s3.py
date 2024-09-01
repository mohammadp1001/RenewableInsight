import sys
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

from src.config import Config
from src.api.entsoe_api import ENTSOEAPI
from src.utilities.utils import create_s3_keys_generation, check_s3_key_exists, generate_random_string, generate_task_name, generate_flow_name

try:
    config = Config()
    print("configuration loaded successfully!")
except ValidationError as e:
    print("configuration error:", e)
    
@task(task_run_name=generate_task_name)
def load_data() -> DataFrame:
    """
    Load data from the ENTSO-E API.

    :return: A pandas DataFrame containing the loaded data.
    """
    data_downloader = ENTSOEAPI(
        year=datetime.datetime.now().year,
        month=datetime.datetime.now().month,
        country_code=config.COUNTRY_CODE,
        api_key=config.ENTSOE_API_KEY
    )
    data_downloader.fetch_data(data_type=config.DATA_TYPE_GEN)
    data = data_downloader.data

    return data

@task(task_run_name=generate_task_name)
def transform(data: DataFrame) -> DataFrame:
    """
    Transform the DataFrame by converting datetime columns and adding additional time-based columns.

    :param data: The input DataFrame to be transformed.
    :return: The transformed DataFrame with additional columns.
    """
    data['date'] = pd.to_datetime(data['date'], format='%Y-%m-%d %H:%M:%S.%f')

    # Extract date and time components
    data['day'] = data['date'].dt.day
    data['month'] = data['date'].dt.month
    data['year'] = data['date'].dt.year
    data['hour'] = data['date'].dt.hour
    data['minute'] = data['date'].dt.minute

    # Convert to appropriate data types for efficiency
    data['month'] = data['month'].astype('int8')
    data['year'] = data['year'].astype('int16')
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
    parquet_buffer = BytesIO()
    logger = get_run_logger()
    bucket_name = config.BUCKET_NAME
    s3 = boto3.client(
        's3', 
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY
    )

    for object_key, date in create_s3_keys_generation():
        data_ = data[(data.hour == date.hour) & 
                     (data.day == date.day) & 
                     (data.month == date.month) & 
                     (data.year == date.year)]
        if not data_.empty:
            table = pa.Table.from_pandas(data_)
            pq.write_table(table, parquet_buffer)
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
        else:
            logger.info("The dataframe is empty for the specified date.")

@flow(log_prints=True,name="actual_generation_etl_aws_s3",flow_run_name=generate_flow_name)
def etl() -> None:
    """
    The ETL flow that orchestrates the loading, transforming, and exporting of generation data.

    :return: None
    """
    raw_data = load_data()
    transformed_data = transform(raw_data)
    export_data_to_s3(transformed_data)

if __name__ == "__main__":
    etl()
