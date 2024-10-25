import sys
import os
import boto3
import datetime
import pandas as pd

from io import BytesIO
from prefect import task, flow
from typing import List, Optional
from prefect import get_run_logger
from pydantic import ValidationError
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import BadRequest, NotFound

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
    partition_column: str,  
    expiration_time: Optional[int] = None
) -> None:
    """
    Uploads a Parquet file from S3 to a BigQuery table with configurable partitioning.
    
    Args:
        s3_key (str): The S3 key for the Parquet file.
        bigquery_table_id (str): The BigQuery table ID where data will be uploaded.
        partition_column (str): The column to use for partitioning in BigQuery.
        expiration_time (Optional[int], optional): Number of days until table expiration. Defaults to None.
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
    
    # Ensure the partition_column exists in the DataFrame
    if partition_column not in df.columns:
        logger.error(f"Partition column '{partition_column}' does not exist in the data.")
        raise ValueError(f"Partition column '{partition_column}' does not exist in the data.")
    
    # Convert the partition_column to DATE type if it's not already
    if not pd.api.types.is_datetime64_any_dtype(df[partition_column]):
        logger.error(f"Partition column '{partition_column}' must be of datetime type.")
        raise TypeError(f"Partition column '{partition_column}' must be of datetime type.")
    
    # Convert to UTC and extract the date component
    df[partition_column] = df[partition_column].dt.tz_convert('UTC').dt.date
    
    # No need to add year and month columns as they already exist
    # Ensure 'year' and 'month' columns are present
    if not {'year', 'month'}.issubset(df.columns):
        logger.error("Data must contain 'year' and 'month' columns for clustering.")
        raise ValueError("Data must contain 'year' and 'month' columns for clustering.")
    
    schema = get_bq_schema_from_df(df, partition_column)
    
    expiration_ms = None
    if expiration_time:
        expiration_time_dt = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=expiration_time)
        expiration_ms = int(expiration_time_dt.timestamp() * 1000)

    credentials = service_account.Credentials.from_service_account_file(config.GOOGLE_APPLICATION_CREDENTIALS)
    bigquery_client = bigquery.Client(credentials=credentials, project=config.PROJECT_ID)
    
    dataset_id = config.BIGQUERY_DATASET_ID
    table_id = bigquery_table_id

    table_ref = bigquery_client.dataset(dataset_id).table(table_id)

    try:
        table = bigquery_client.get_table(table_ref)
        logger.info("Table already exists.")
    except NotFound:
        logger.info(f"Table does not exist. Creating table {table_id}.")
        table = bigquery.Table(table_ref, schema=schema)
        if expiration_ms:
            table.expires = expiration_time_dt
        
        # Define partitioning based on the provided partition_column
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_column,  # Use the provided partition_column
        )
        
        # Define clustering on existing 'year' and 'month' columns
        table.clustering_fields = ["year", "month"]
        
        bigquery_client.create_table(table)
        logger.info(f"Table {bigquery_table_id} created successfully.")
    except Exception as e:
        logger.error(f"Error checking table existence: {e}")
        raise

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND, 
        schema=schema,
        source_format=bigquery.SourceFormat.PARQUET,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_column,  # Ensure the partitioning field is set
        ),
        clustering_fields=["year", "month"]  # Use existing 'year' and 'month' columns
    )
    try:
        job = bigquery_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  
        logger.info(f"Data successfully uploaded to {bigquery_table_id}")
    except BadRequest as e:
        logger.error(f"BadRequest error: {e.message}")
        logger.error(f"Details: {e.errors}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred while uploading to BigQuery: {e}")
        raise


@task(task_run_name="list_s3_files")
def list_s3_files(prefix: str = '') -> List[str]:
    """
    Lists files in an S3 bucket under a specified prefix and returns the list of file keys.
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

@task(task_run_name="check_processed")
def check_processed_files(s3_key: str) -> bool:
    """
    Checks if the given S3 key has already been processed.
    """
    logger = get_run_logger()
    credentials = service_account.Credentials.from_service_account_file(config.GOOGLE_APPLICATION_CREDENTIALS)
    bigquery_client = bigquery.Client(credentials=credentials, project=config.PROJECT_ID)
    
    query = """
        SELECT COUNT(1) as count
        FROM `{}.{}.{}` 
        WHERE s3_key = @s3_key
    """.format(
        config.BIGQUERY_PROJECT_ID,
        config.BIGQUERY_DATASET_ID,
        'processed_files'
    )
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("s3_key", "STRING", s3_key)
        ]
    )
    query_job = bigquery_client.query(query, job_config=job_config)
    results = query_job.result()
    for row in results:
        if row.count > 0:
            logger.info(f"S3 key {s3_key} has already been processed.")
            return True
    return False

@task(task_run_name="record_processed")
def record_processed_file(s3_key: str) -> None:
    """
    Records the processed S3 key in the metadata table.
    """
    logger = get_run_logger()
    credentials = service_account.Credentials.from_service_account_file(config.GOOGLE_APPLICATION_CREDENTIALS)
    bigquery_client = bigquery.Client(credentials=credentials, project=config.PROJECT_ID)
    
    table_id = f"{config.BIGQUERY_PROJECT_ID}.{config.BIGQUERY_DATASET_ID}.processed_files"
    
    
    processed_at = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    
    rows_to_insert = [
        {"s3_key": s3_key, "processed_at": processed_at}
    ]
    
    errors = bigquery_client.insert_rows_json(table_id, rows_to_insert)
    if not errors:
        logger.info(f"Recorded processed file: {s3_key}")
    else:
        logger.error(f"Failed to record processed file {s3_key}: {errors}")
        raise Exception(f"Failed to record processed file {s3_key}")

@task(task_run_name="ensure_processed_files_table")
def ensure_processed_files_table() -> None:
    """
    Ensures that the 'processed_files' table exists in BigQuery. If not, creates it.
    """
    logger = get_run_logger()
    credentials = service_account.Credentials.from_service_account_file(config.GOOGLE_APPLICATION_CREDENTIALS)
    bigquery_client = bigquery.Client(credentials=credentials, project=config.PROJECT_ID)
    
    dataset_id = config.BIGQUERY_DATASET_ID
    table_id = 'processed_files'
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)
    
    try:
        bigquery_client.get_table(table_ref)
        logger.info(f"Table '{table_id}' already exists.")
    except NotFound:
        logger.info(f"Table '{table_id}' does not exist. Creating table.")
        schema = [
            bigquery.SchemaField("s3_key", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("processed_at", "TIMESTAMP", mode="REQUIRED"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        bigquery_client.create_table(table)
        logger.info(f"Table '{table_id}' created successfully.")
    except Exception as e:
        logger.error(f"Error ensuring 'processed_files' table exists: {e}")
        raise

@flow(log_prints=True, name="s3_to_bigquery", flow_run_name=generate_flow_name)
def s3_to_bigquery_flow(prefix: str, bigquery_table_id: str, partition_column: str, expiration_time: int) -> None:
    """
    Transfer flow that lists all files in the S3 bucket under a prefix and uploads them to BigQuery.

    Args:
        prefix (str): The prefix path in the S3 bucket to list files.
        bigquery_table_id (str): The BigQuery table ID where data will be uploaded.
        partition_column (str): The column to use for partitioning in BigQuery.
        expiration_time (int): Number of days until table expiration.
    """
    logger = get_run_logger()
    
    ensure_processed_files_table()
    
    s3_keys = list_s3_files(prefix)
    
    for s3_key in s3_keys:
        logger.info(f"Processing file: {s3_key}")
        
        already_processed = check_processed_files(s3_key)
        if already_processed:
            logger.info(f"Skipping already processed file: {s3_key}")
            continue
        
        try:
            upload_parquet_to_bigquery(
                s3_key=s3_key, 
                bigquery_table_id=bigquery_table_id,
                partition_column=partition_column,  # Pass partition_column
                expiration_time=expiration_time
            )
            
            record_processed_file(s3_key)
        except Exception as e:
            logger.error(f"Failed to process file {s3_key}: {e}")
            
