import sys
import boto3
import logging
import datetime
import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq

print(sys.path)

if '/home/mohammad/RenewableInsight' not in sys.path:
    sys.path.append('/home/mohammad/RenewableInsight')

from io import BytesIO
from prefect import task, flow
from pandas import DataFrame

from src.utilities.utils import create_s3_keys_generation, check_s3_key_exists, generate_random_string
from src.config import Config
from src.api.entsoe_api import ENTSOEAPI


@task
def load_data() -> DataFrame:
    """
    Load data from the ENTSO-E API.

    Args:

            month (int): The month of the data.
            year (int): The year of the data.
            data_type (str): The data_type name.
            
    Returns:
        DataFrame: The loaded DataFrame.
    """
    data_type = Config.DATA_TYPE

    data_downloader = ENTSOEAPI(year=datetime.datetime.now().year,month=datetime.datetime.now().month,country_code=Config.COUNTRY_CODE,api_key=Config.ENTSOE_API_KEY)
    data_downloader.fetch_data(data_type=data_type)
    data = data_downloader.data

    return data

# Transform data
@task
def transform(data: DataFrame) -> DataFrame:
    """
    Transform datetime and add additional columns.

    Args:
        data (DataFrame): The input DataFrame.

    Returns:
        DataFrame: The transformed DataFrame with additional columns.
    """
    # Transform datetime
    data['date'] = pd.to_datetime(data['date'], format='%Y-%m-%d %H:%M:%S.%f')

    # Extract date and time components
    data['day'] = data['date'].dt.day
    data['month'] = data['date'].dt.month
    data['year'] = data['date'].dt.year
    data['hour'] = data['date'].dt.hour
    data['minute'] = data['date'].dt.minute
    data['month'] = data['month'].astype('int8')
    data['year'] = data['year'].astype('int16')
    data['hour'] = data['hour'].astype('int8')
    data['minute'] = data['minute'].astype('int8')
    data['day'] = data['day'].astype('int8')


    return data


# Load data
@task
def export_data_to_s3(data: DataFrame) -> None:
    """
    Export data to a S3 bucket.

    Args:
        data (DataFrame): The DataFrame to be exported.
            Required:
                - bucket_name (str): The name of the S3 bucket.
                - export_mode (str): The export mode ('daily' or other).
                - data_item_name (str): The name of the data item.
                - data_item_no (int): The number of data items.

    Docs: https://docs.mage.ai/design/data-loading#s3
    """
    parquet_buffer = BytesIO()
    table = pa.Table.from_pandas(data)
    pq.write_table(table, parquet_buffer)

    bucket_name = Config.BUCKET_NAME
    
    s3 = boto3.client('s3', aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY)

    for object_key, date in create_s3_keys_generation():
        data_date = data[(data.day == date.day) & (data.month == date.month) & (data.year == date.year)]
        filename = object_key + f"/{generate_random_string(10)}.parquet"
        if not check_s3_key_exists(clinet=s3, bucket_name=bucket_name, object_key=object_key):
            s3.put_object(
                Bucket=bucket_name,
                Key=filename,
                Body=parquet_buffer.getvalue()
                )
            logging.info(f"File has been written to s3 {bucket_name} inside {object_key}.")
        else:
            logging.info(f"File {object_key} already exists.")



# Defining the flow
@flow(log_prints=True)
def etl():
    raw_data = load_data()
    transformed_data = transform(raw_data)
    export_data_to_s3(transformed_data)

# Running the flow
if __name__ == "__main__":
    etl()