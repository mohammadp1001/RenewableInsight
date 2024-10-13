
import pytz
import boto3
import string
import datetime
import secrets
import logging
import subprocess
import pandas as pd

from pathlib import Path
from typing import List
from google.cloud import bigquery
from pydantic import ValidationError
from prefect.runtime import flow_run, task_run
from typing import Generator, Tuple, List, Optional
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

from src.api.parameters import WeatherParameter

TIMEZONE = pytz.timezone('Europe/Berlin')

def generate_random_string(n: int = 10) -> str:
    """
    Generate a random string of lowercase letters and digits.

    :param n: Length of the random string to generate. Default is 10.
    :return: Random string of specified length.
    """
    return ''.join(secrets.choice(string.ascii_lowercase + string.digits) for _ in range(n))


def create_s3_keys_generation() -> Generator[Tuple[str, datetime.datetime], None, None]:
    """
    Generate S3 object keys with embedded dates, formatted specifically for use as filenames.

    :yield: A tuple containing the S3 object key and the corresponding date object for each key.
    """
    today = datetime.datetime.now(TIMEZONE)
    start_date = today - datetime.timedelta(days=5)
    for i in range(5):
        date = start_date + datetime.timedelta(days=i)
        object_key = f"electricity/generation/generation_{date.day:02}_{date.month:02}_{date.year}"
        yield object_key, date


def create_s3_keys_historical_weather(
    weather_param: WeatherParameter, 
    station_code: str
) -> Generator[Tuple[str, datetime.datetime], None, None]:
    """
    Generate S3 object keys with embedded dates for the same day and month for the last 5 years,
    formatted specifically for use as filenames.

    :param weather_param: The weather parameter used to categorize the folder structure.
    :param station_code: The station code for categorizing the folder structure.
    :yield: A tuple containing the S3 object key and the corresponding date object for each key.
    """
    today = datetime.datetime.now(TIMEZONE)
    for i in range(5):  
        date = datetime.datetime(today.year - i, today.month, today.day)
        object_key = f"historical_weather/{WeatherParameter[weather_param].category}/{station_code}/{date.day:02}_{date.month:02}_{date.year}"
        yield object_key, date


def create_s3_keys_weather_forecast(
    n_day: int, 
    station_name: str
) -> Generator[Tuple[str, datetime.datetime], None, None]:
    """
    Generate S3 object keys with embedded dates and a random string, formatted specifically for use as filenames.

    :param n_day: The number of days to forecast.
    :param station_name: The name of the weather station.
    :yield: A tuple containing the S3 object key and the corresponding date object for each key.
    """
    today = datetime.datetime.now(TIMEZONE)
    last_day = today + datetime.timedelta(days=n_day)
    for i in range(n_day):
        date = last_day - datetime.timedelta(days=i)
        object_key = f"weather_forecast/{station_name}/{date.day:02}_{date.month:02}_{date.year}"
        yield object_key, date


def create_s3_keys_load() -> Generator[Tuple[str, datetime], None, None]:
    """
    Generate S3 object keys for the current date with hours before the current time.

    :yield: A tuple containing the S3 object key and the corresponding date object for each key.
    """
    now = datetime.now()
    date = now.replace(minute=0, second=0, microsecond=0)

    for hour in range(now.hour):
        date_with_hour = date.replace(hour=hour)
        object_key = f"electricity/load/load_{date_with_hour.year}_{date_with_hour.month:02}_{date_with_hour.day:02}_{date_with_hour.hour:02}"
        yield object_key, date_with_hour


def create_s3_keys_gas() -> Generator[Tuple[str, datetime.datetime], None, None]:
    """
    Generate S3 object keys with embedded dates and hours, formatted specifically for gas price data.

    :yield: A tuple containing the S3 object key and the corresponding date object for each key.
    """
    date_to_read = datetime.datetime.now(TIMEZONE)
    if date_to_read.weekday() == 5:  
        date_to_read = date_to_read - datetime.timedelta(days=1)
        date_to_read = date_to_read.replace(hour=23)
    elif date_to_read.weekday() == 6:  
        date_to_read = date_to_read - datetime.timedelta(days=2)
        date_to_read = date_to_read.replace(hour=23)
    
    for hour in range(date_to_read.hour):
        object_key = f"others/gas/gas_price_{date_to_read.year}_{date_to_read.month:02}_{date_to_read.day:02}_{hour:02}"
        date = date_to_read.replace(hour=hour)
        yield object_key, date


def runcmd(cmd: str, verbose: bool = False, *args, **kwargs) -> None:
    """
    Run a shell command.

    :param cmd: The command to run.
    :param verbose: If True, logs the standard output and error. Default is False.
    :param args: Variable length argument list.
    :param kwargs: Arbitrary keyword arguments.
    """
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        shell=True
    )
    std_out, std_err = process.communicate()
    if verbose:
        logging.info(std_out.strip(), std_err)


def download_kmz_file(url: str, save_dir: Path, filename: str) -> Path:
    """
    Download a KMZ file from the specified URL and save it to the specified directory with the given filename.

    :param url: The URL of the KMZ file to download.
    :param save_dir: The directory where the KMZ file should be saved.
    :param filename: The name to save the KMZ file as.
    :return: The full path to the saved KMZ file.
    """
    runcmd(f"wget --directory-prefix={save_dir} {url}", verbose=False)
    logging.info(f"KMZ file downloaded and saved to {save_dir}")
    save_path = save_dir / filename
    return save_path


def check_s3_key_exists(
    client: boto3.client, 
    bucket_name: str, 
    object_key: str
) -> bool:
    """
    Check if a specific key already exists in an S3 bucket.

    :param client: The S3 client.
    :param bucket_name: The name of the S3 bucket.
    :param object_key: The key of the S3 object to check.
    :return: True if the key exists, False otherwise.
    """
    try:
        result = client.list_objects_v2(
            Bucket=bucket_name, Prefix=object_key, Delimiter='/'
        )
        return 'CommonPrefixes' in result
    except client.exceptions.NoSuchKey:
        return False
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Credentials error: {e}")
        return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False


def get_bq_schema_from_df(df: pd.DataFrame, partition_column: str) -> List[bigquery.SchemaField]:
    """
    Generates BigQuery schema from a pandas DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame containing the data.
        partition_column (str): The column to be used for partitioning.

    Returns:
        List[bigquery.SchemaField]: The list of BigQuery schema fields.
    """
    schema = []
    for column, dtype in df.dtypes.items():
        if column == partition_column:
            field_type = 'DATE'  # Ensure partition_column is DATE
        elif pd.api.types.is_integer_dtype(dtype):
            field_type = 'INTEGER'
        elif pd.api.types.is_float_dtype(dtype):
            field_type = 'FLOAT'
        elif pd.api.types.is_bool_dtype(dtype):
            field_type = 'BOOLEAN'
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            field_type = 'TIMESTAMP'
        else:
            field_type = 'STRING'
        
        mode = 'REQUIRED' if not df[column].isnull().any() else 'NULLABLE'
        schema.append(bigquery.SchemaField(column, field_type, mode=mode))
    return schema

def generate_task_name()-> str:
    """
    Generate task_run_name based on flow_name and task_run parameters.
    
    :return task_run_name
    """
    flow_name = flow_run.flow_name
    task_name = task_run.task_name


    task_run_name = f"{flow_name}-{task_name}-{datetime.datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')}"

    return task_run_name

def generate_flow_name()-> str:
    """
    Generate flow_run_name based on date.
    
    :return flow_run_name
    """
    flow_name = flow_run.flow_name
        
    flow_run_name = f"{flow_name}-{datetime.datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')}"

    return flow_run_name

