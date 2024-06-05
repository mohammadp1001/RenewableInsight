import secrets
import string
from datetime import datetime, timedelta
from ..utilities.weather_data_downloader import WeatherParameter
from pathlib import Path
import requests
import logging
import subprocess


def generate_random_string(n=10):
    """
    Generate a random string of lowercase letters and digits.
    
    Parameters:
        n (int): Length of the random string to generate. Default is 10.
        
    Returns:
        str: Random string of specified length.
    """
    return ''.join(secrets.choice(string.ascii_lowercase + string.digits) for _ in range(n))

def create_s3_keys_dates(data_item_name,data_item_no):
    """
    Generate S3 object keys with embedded dates and a random string, formatted specifically for use as filenames.
    
    Parameters:
        data_item_name (str): Base name of the data item to include in the key.
        data_item_no (int): Data item number to include in the key.

    Yields:
        tuple: A tuple containing the S3 object key and the corresponding date object for each key.
    
    The function calculates dates from 5 days ago and generates five S3 keys, one for each day starting from
    'last_day' (5 days ago) to 'last_day-4' (9 days ago). Each key includes the `data_item_name`, `data_item_no`,
    the date (day, month, year), and a random string suffix, stored in a .parquet file format in respective date folders.
    """
    today = datetime.now()
    last_day = today - timedelta(days=5)
    for i in range(5):
        date = last_day - timedelta(days=i)
        object_key = f"electricity/{data_item_name}_{data_item_no}_{date.day:02}_{date.month:02}_{date.year}/{data_item_name}_{data_item_no}.parquet"
        yield object_key,date

def create_s3_keys_historical_weather(weather_param,station_code):
    """
    Generate S3 object keys with embedded dates for the same day and month for the last 5 years, 
    formatted specifically for use as filenames.

    Parameters:
        weather_param (WeatherParameter): The weather parameter used to categorize the folder structure.
        station_code

    Yields:
        tuple: A tuple containing the S3 object key and the corresponding date object for each key.
    
    The function calculates dates for the same day and month from the current year and for the four preceding years.
    Each key includes the category, day, month, the weather parameter, and is stored in a .csv file format in respective date folders.
    """
    today = datetime.now()
    for i in range(5):  # Last 5 years including the current year
        date = datetime(today.year - i, today.month, today.day)
        object_key = f"historical_weather/{WeatherParameter[weather_param].category}/{station_code}/{today.day}_{today.month}/{weather_param}_{today.day}_{today.month}_{date.year}.csv"
        yield object_key, date  

def create_s3_keys_weather_forecast(n_day,station_name):
    """
    Generate S3 object keys with embedded dates and a random string, formatted specifically for use as filenames.
    
    Yields:
        tuple: A tuple containing the S3 object key and the corresponding date object for each key.
    
    The function calculates dates for next n_day days and generates n_day S3 keys, one for each day starting from
    today. Each key includes the, the date (day, month, year), and a random string suffix, stored in a .parquet file 
    format in respective date folders.
    """
    today = datetime.now()
    last_day = today + timedelta(days = n_day)
    for i in range(n_day):
        date = last_day - timedelta(days=i)
        object_key = f"weather_forcast/{station_name}/{date.day:02}_{date.month:02}_{date.year}/{generate_random_string(10)}.parquet"
        yield object_key,date



# https://www.scrapingbee.com/blog/python-wget/
def runcmd(cmd, verbose = False, *args, **kwargs):

    process = subprocess.Popen(
        cmd,
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE,
        text = True,
        shell = True
    )
    std_out, std_err = process.communicate()
    if verbose:
        logging.info(std_out.strip(), std_err)
    pass


def download_kmz_file(url: str, save_dir: Path, filename: str) -> Path:
    """
    Downloads a KMZ file from the specified URL and saves it to the specified directory with the given filename.

    Parameters:
    - url: The URL of the KMZ file to download.
    - save_dir: The directory where the KMZ file should be saved.
    - filename: The name to save the KMZ file as.

    Returns:
    - The full path to the saved KMZ file.
    """
    runcmd(f"wget --directory-prefix={save_dir} {url}", verbose = False)

    
    logging.info(f"KMZ file downloaded and saved to {save_dir}")
    
    save_dir = save_dir + filename
    save_dir = Path(save_dir)
    
    return save_dir

