import boto3
import pandas as pd
from io import BytesIO

from src.config import Config
config = Config()
from src.utilities.utils import create_s3_keys_load


def read_s3_file_to_dataframe(key_object):
    """
    Reads a file from S3 using the specified key object and returns it as a pandas DataFrame.
    
    Args:
    - key_object (str): The key (path) of the object in the S3 bucket.
    
    Returns:
    - DataFrame: A pandas DataFrame containing the data from the file.
    """
    bucket_name = config.BUCKET_NAME
    s3 = boto3.client(
        's3', 
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY
    )

    # Download the file object
    try:
        response = s3.get_object(Bucket=config.BUCKET_NAME, Key=key_object)
    
        parquet_file = BytesIO(response['Body'].read())
        df = pd.read_parquet(parquet_file)
        return df
    except Exception as e:
        raise Exception(f"Error reading S3 object: {e}")
if __name__ == '__main__':
    for object_key, date_with_hour in create_s3_keys_load():
        
        print(date_with_hour.hour)
        print(date_with_hour.day)