import secrets
import string
from datetime import datetime, timedelta

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
        random_part = generate_random_string(10)
        object_key = f"{data_item_name}_{data_item_no}_{date.day}_{date.month}_{date.year}/{random_part}.parquet"
        yield object_key,date