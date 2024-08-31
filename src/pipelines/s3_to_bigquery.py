import sys
import datetime
from io import BytesIO
from typing import List, Optional
import boto3
import pandas as pd
from prefect import task, flow
from prefect import get_run_logger
from google.cloud import bigquery
from google.oauth2 import service_account

if '/home/mohammad/RenewableInsight' not in sys.path:
    sys.path.append('/home/mohammad/RenewableInsight')

from src.config import Config
from src.utilities.bigquery_schema import get_bq_schema_from_df

@task
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
        aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY
    )
    response = s3.get_object(Bucket=Config.BUCKET_NAME, Key=s3_key)
    parquet_file = BytesIO(response['Body'].read())
    df = pd.read_parquet(parquet_file)
    schema = get_bq_schema_from_df(df)

    expiration_ms = None
    if expiration_time:
        expiration_time = datetime.datetime.utcnow() + datetime.timedelta(days=expiration_time)
        expiration_ms = int(expiration_time.timestamp() * 1000)

    credentials = service_account.Credentials.from_service_account_file(Config.GOOGLE_APPLICATION_CREDENTIALS)
    bigquery_client = bigquery.Client(credentials=credentials, project=Config.PROJECT_ID)

    dataset_ref = bigquery_client.dataset(bigquery_table_id.split('.')[0])
    table_ref = dataset_ref.table(bigquery_table_id.split('.')[1])

    try:
        bigquery_client.get_table(table_ref)
        logger("Table already exists.")
    except Exception as e:
        logger(f"Table does not exist. Creating table: {e}")
        table = bigquery.Table(table_ref, schema=schema)
        if expiration_ms:
            table.expires = expiration_time
        bigquery_client.create_table(table)
        logger(f"Table {bigquery_table_id} created successfully.")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND, 
        schema=schema,
        source_format=bigquery.SourceFormat.PARQUET
    )
    job = bigquery_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    logger(f"Data successfully uploaded to {bigquery_table_id}")

@flow(log_prints=True)
def etl():
    logger = get_run_logger()
    upload_parquet_to_bigquery(
        "others/gas/gas_price_2024_08_23_15/8tzkv3hto4.parquet",
        "renewableinsight_dataset.gas_price_2024_08_23_15",
        expiration_time=7
    )

if __name__ == "__main__":
    etl()
    