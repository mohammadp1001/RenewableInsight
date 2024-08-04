import secrets
import string
import datetime
import logging
import subprocess
import boto3

from pathlib import Path
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

from src.api.parameters import WeatherParameter
from src.config import Config


def generate_random_string(n=10):
    """
    Generate a random string of lowercase letters and digits.

    :param n: Length of the random string to generate. Default is 10.
    :type n: int
    :return: Random string of specified length.
    :rtype: str
    """
    return ''.join(secrets.choice(string.ascii_lowercase + string.digits) for _ in range(n))


def create_s3_keys_generation():
    """
    Generate S3 object keys with embedded dates, formatted specifically for use as filenames.

    Yields:
        tuple: A tuple containing the S3 object key and the corresponding date object for each key.

    The function calculates dates from 5 days ago up to today and generates five S3 keys, one for each day starting from
    '5 days ago' to '1 day ago'. Each key includes the date (day, month, year).

    The generated keys follow the format:
    electricity/generation_DD_MM_YYYY
    where DD is the day, MM is the month, YYYY is the year.
    """
    today = datetime.datetime.now()
    start_date = today - datetime.timedelta(days=5)
    for i in range(5):
        date = start_date + datetime.timedelta(days=i)
        object_key = f"electricity/generation_{date.day:02}_{date.month:02}_{date.year}"
        yield object_key, date


def create_s3_keys_historical_weather(weather_param, station_code):
    """
    Generate S3 object keys with embedded dates for the same day and month for the last 5 years,
    formatted specifically for use as filenames.

    :param weather_param: The weather parameter used to categorize the folder structure.
    :type weather_param: WeatherParameter
    :param station_code: The station code for categorizing the folder structure.
    :type station_code: str
    :yield: A tuple containing the S3 object key and the corresponding date object for each key.
    :rtype: tuple

    The function calculates dates for the same day and month from the current year and for the four preceding years.
    Each key includes the category, day, month, the weather parameter, and is stored in a .csv file format in respective date folders.
    """
    today = datetime.datetime.now()
    for i in range(5):  # Last 5 years including the current year
        date = datetime.datetime(today.year - i, today.month, today.day)
        object_key = f"historical_weather/{WeatherParameter[weather_param].category}/{station_code}/{today.day}_{today.month}/{weather_param}_{today.day}_{today.month}_{date.year}.csv"
        yield object_key, date


def create_s3_keys_weather_forecast(n_day, station_name):
    """
    Generate S3 object keys with embedded dates and a random string, formatted specifically for use as filenames.

    :param n_day: The number of days to forecast.
    :type n_day: int
    :param station_name: The name of the weather station.
    :type station_name: str
    :yield: A tuple containing the S3 object key and the corresponding date object for each key.
    :rtype: tuple

    The function calculates dates for the next n_day days and generates n_day S3 keys, one for each day starting from
    today. Each key includes the date (day, month, year), and a random string suffix, stored in a .parquet file 
    format in respective date folders.
    """
    today = datetime.datetime.now()
    last_day = today + datetime.timedelta(days=n_day)
    for i in range(n_day):
        date = last_day - datetime.timedelta(days=i)
        object_key = f"weather_forcast/{station_name}/{date.day:02}_{date.month:02}_{date.year}/{generate_random_string(10)}.parquet"
        yield object_key, date

def create_s3_keys_load():
    date = datetime.datetime.today()
    for hour in range(24):
        object_key = f"electricity/load_{date.year}_{date.month:02}_{date.day:02}_{hour:02}"
        yield object_key, date

def runcmd(cmd, verbose=False, *args, **kwargs):
    """
    Run a shell command.

    :param cmd: The command to run.
    :type cmd: str
    :param verbose: If True, logs the standard output and error. Default is False.
    :type verbose: bool
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
    :type url: str
    :param save_dir: The directory where the KMZ file should be saved.
    :type save_dir: Path
    :param filename: The name to save the KMZ file as.
    :type filename: str
    :return: The full path to the saved KMZ file.
    :rtype: Path
    """
    runcmd(f"wget --directory-prefix={save_dir} {url}", verbose=False)
    logging.info(f"KMZ file downloaded and saved to {save_dir}")
    save_path = save_dir / filename
    return save_path


def check_s3_key_exists(client,bucket_name,object_key):
    """
    Check if a specific key already exists in an S3 bucket.

    :param bucket_name: The name of the S3 bucket.
    :type bucket_name: str
    :param object_key: The key of the S3 object to check.
    :type object_key: str
    :return: True if the key exists, False otherwise.
    :rtype: bool
    """
    try:
        result = client.list_objects_v2(
            Bucket=bucket_name, Prefix=object_key, Delimiter='/')
        return 'CommonPrefixes' in result
    except client.exceptions.NoSuchKey:
        return False
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Credentials error: {e}")
        return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False
