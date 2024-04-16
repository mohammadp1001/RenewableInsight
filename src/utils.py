import secrets
import string
from datetime import datetime, timedelta
from RenewableInsight.src.WeatherDataDownloader import WeatherParameter

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
        object_key = f"{data_item_name}_{data_item_no}_{date.day:02}_{date.month:02}_{date.year}/{data_item_name}_{data_item_no}.parquet"
        yield object_key,date

def create_s3_keys_annual(weather_param,station_code):
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
        object_key = f"{WeatherParameter[weather_param].category}/{station_code}/{today.day}_{today.month}/{weather_param}_{today.day}_{today.month}_{date.year}.csv"
        yield object_key, date        