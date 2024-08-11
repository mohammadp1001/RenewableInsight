import sys
import boto3
import pandas as pd

from google.cloud import bigquery
from google.oauth2 import service_account

from io import BytesIO

if '/home/mohammad/RenewableInsight' not in sys.path:
    sys.path.append('/home/mohammad/RenewableInsight')

from src.config import Config

def process_and_upload_parquet_to_bigquery(s3_key: str, bigquery_table_id: str) -> None:
    
    s3 = boto3.client('s3', aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY)
    response = s3.get_object(Bucket=Config.BUCKET_NAME, Key=s3_key)
    parquet_file = BytesIO(response['Body'].read())
    df = pd.read_parquet(parquet_file)
    print(df.shape)
    credentials = service_account.Credentials.from_service_account_file(Config.GOOGLE_APPLICATION_CREDENTIALS)
    bigquery_client = bigquery.Client(credentials=credentials, project=Config.PROJECT_ID)

    table_ref = bigquery_client.dataset(bigquery_table_id.split('.')[0]).table(bigquery_table_id.split('.')[1])
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Replace table content
    )
    job = bigquery_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  

    print(f"Data successfully uploaded to {bigquery_table_id}")


def list_s3_files(bucket_name, prefix=''):

    s3 = boto3.client('s3', aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY)
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    list_files = []
    if 'Contents' in response:
        print(f"Files in {bucket_name}/{prefix}:")
        for obj in response['Contents']:
            print(obj['Key'])
            list_files.append(obj['Key'])
    else:
        print(f"No files found in {bucket_name}/{prefix}.")

    return list_files

list_s3_files(Config.BUCKET_NAME, prefix='electricity/generation/')

def read_s3_files(bucket_name, prefix=''):

    s3 = boto3.client('s3', aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY)
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    list_files = []
    if 'Contents' in response:
        print(f"Files in {bucket_name}/{prefix}:")
        for obj in response['Contents']:
            # print(obj['Key'])
            list_files.append(obj['Key'])
    else:
        print(f"No files found in {bucket_name}/{prefix}.")
    
    for file in list_files:
        response = s3.get_object(Bucket=Config.BUCKET_NAME, Key=file)
        parquet_file = BytesIO(response['Body'].read())
        df = pd.read_parquet(parquet_file,engine='pyarrow')
        print(df.shape)

    return list_files

read_s3_files(Config.BUCKET_NAME, prefix='electricity/generation/')


#for file in list_s3_files(Config.BUCKET_NAME, prefix='historical_weather/'):
#    process_and_upload_parquet_to_bigquery( 
#        s3_key=file,
#        bigquery_table_id=Config.BIGQUERY_DATASET_ID+'.historical_weather'
#    )
