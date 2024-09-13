import sys
import boto3
import datetime
import pandas as pd

from io import BytesIO
from prefect import task, flow
from typing import List, Optional
from prefect import get_run_logger
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import BadRequest  

if '/home/mohammad/RenewableInsight' not in sys.path:
    sys.path.append('/home/mohammad/RenewableInsight')

from src.config import Config
from src.utilities.utils import get_bq_schema_from_df, generate_task_name, generate_flow_name

try:
    config = Config()
except ValidationError as e:
    print("configuration error:", e)

@task(task_run_name=generate_task_name)
def upload_parquet_to_bigquery(
    s3_key: str, 
    bigquery_table_id: str, 
    expiration_time: Optional[int] = None
) -> None:
    """
    Uploads a Parquet file from S3 to a BigQuery table.

    
    :param s3_key: The S3 key for the Parquet file.
    :param bigquery_table_id: The BigQuery table ID.
    :param expiration_time: Expiration time in days for the BigQuery table.
    :return None
    """
    logger = get_run_logger()
    s3 = boto3.client(
        's3', 
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY
    )
    response = s3.get_object(Bucket=config.BUCKET_NAME, Key=s3_key)
    
    parquet_file = BytesIO(response['Body'].read())
    df = pd.read_parquet(parquet_file)
    schema = get_bq_schema_from_df(df)

    expiration_ms = None
    if expiration_time:
        expiration_time = datetime.datetime.now(datetime.timezone.utc)  + datetime.timedelta(days=expiration_time)
        expiration_ms = int(expiration_time.timestamp() * 1000)

    credentials = service_account.Credentials.from_service_account_file(config.GOOGLE_APPLICATION_CREDENTIALS)
    bigquery_client = bigquery.Client(credentials=credentials, project=config.PROJECT_ID)
    
    project_id = config.BIGQUERY_PROJECT_ID
    dataset_id = config.BIGQUERY_DATASET_ID
    table_id = bigquery_table_id

    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    try:
        bigquery_client.get_table(table_ref)
        logger.info("Table already exists.")
    except Exception as e:
        logger.info(f"Table does not exist. Creating table: {e}")
        table = bigquery.Table(table_ref, schema=schema)
        if expiration_ms:
            table.expires = expiration_time
        bigquery_client.create_table(table)
        logger.info(f"Table {bigquery_table_id} created successfully.")
    

    job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND, 
            schema=schema,
            source_format=bigquery.SourceFormat.PARQUET,
    )
    try:
        job = bigquery_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  
        logger.info(f"Data successfully uploaded to {bigquery_table_id}")
    
    except BadRequest as e:
        logger.error(f"BadRequest error: {e.message}")
        logger.error(f"Details: {e.errors}")
        
    
    except Exception as e:
        logger.error(f"An unexpected error occurred while uploading to BigQuery: {e}")

@task(task_run_name="list_s3_files")
def list_s3_files(prefix: str = '') -> List[str]:
    """
    Lists files in an S3 bucket under a specified prefix and returns the list of file keys.

   
    :param prefix: The prefix under which to list files.
    :return: A list of file keys.
    """
    bucket_name = config.BUCKET_NAME

    s3 = boto3.client(
        's3', 
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY
    )
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

@flow(log_prints=True, name="s3_to_bigquery", flow_run_name=generate_flow_name)
def s3_to_bigquery_flow(prefix: str, bigquery_table_id: str, expiration_time: int) -> None:
    """
    Transfer flow that lists all files in the S3 bucket under a prefix and uploads them to BigQuery.

    :param prefix: The prefix under which to list files.
    :param bigquery_table_id: The BigQuery table ID where data will be uploaded.
    :param expiration_time: Expiration time in days for the BigQuery table.
    """
    logger = get_run_logger()
    
    
    s3_keys = list_s3_files(prefix)

    
    for s3_key in s3_keys:
        logger.info(f"Processing file: {s3_key}")
        upload_parquet_to_bigquery(
            s3_key=s3_key, 
            bigquery_table_id=bigquery_table_id,
            time_column= time_column, 
            expiration_time=expiration_time
        )

if __name__ == "__main__":
    pass
