import sys
import boto3
import os
from pandas import DataFrame
from prefect import task, flow
from prefect import get_run_logger
from datetime import datetime, timezone, timedelta

path_to_append = os.getenv('PYTHON_APP_PATH')
if path_to_append:
    sys.path.append(path_to_append)

from src.config import Config
from src.utilities.utils import generate_task_name, generate_flow_name


try:
    config = Config()
except ValidationError as e:
    print("configuration error:", e)


@task(task_run_name=generate_task_name)
def cleanup_s3_files(time_span_days: int) -> None:
    """
    Cleans up S3 files that have been processed and recorded longer ago than the specified time_span.
    
    :param time_span_days: The number of days after which processed files should be deleted from S3.
    """
    logger = get_run_logger()
    credentials = service_account.Credentials.from_service_account_file(config.GOOGLE_APPLICATION_CREDENTIALS)
    bigquery_client = bigquery.Client(credentials=credentials, project=config.PROJECT_ID)
    
    s3 = boto3.client(
        's3', 
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY
    )
    
    dataset_id = config.BIGQUERY_DATASET_ID
    table_id = 'processed_files'
    
    
    cutoff_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=time_span_days)
    cutoff_time_iso = cutoff_time.isoformat()
    
    logger.info(f"Cleaning up files processed before {cutoff_time_iso}")
    
    
    query = """
        SELECT s3_key
        FROM `{}.{}.{}` 
        WHERE processed_at < @cutoff_time
    """.format(
        config.BIGQUERY_PROJECT_ID,
        config.BIGQUERY_DATASET_ID,
        table_id
    )
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("cutoff_time", "TIMESTAMP", cutoff_time_iso)
        ]
    )
    
    try:
        query_job = bigquery_client.query(query, job_config=job_config)
        results = query_job.result()
        s3_keys_to_delete = [row.s3_key for row in results]
        
        if not s3_keys_to_delete:
            logger.info("No files to clean up based on the specified time_span.")
            return
        
        logger.info(f"Found {len(s3_keys_to_delete)} files to delete from S3.")
        
       
        MAX_DELETE = 1000
        for i in range(0, len(s3_keys_to_delete), MAX_DELETE):
            batch = s3_keys_to_delete[i:i + MAX_DELETE]
            delete_requests = {'Objects': [{'Key': key} for key in batch]}
            response = s3.delete_objects(Bucket=config.BUCKET_NAME, Delete=delete_requests)
            
            deleted = response.get('Deleted', [])
            errors = response.get('Errors', [])
            
            for obj in deleted:
                logger.info(f"Deleted S3 object: {obj['Key']}")
            
            for error in errors:
                logger.error(f"Failed to delete S3 object: {error.get('Key')} - {error.get('Message')}")
        
        logger.info("Cleanup of S3 files completed successfully.")
        
    except Exception as e:
        logger.error(f"An error occurred during cleanup: {e}")
        raise

@flow(log_prints=True,name="cleanup_s3",flow_run_name=generate_flow_name)
def cleanup_flow(time_span_days: int) -> None:
    """
    The flow that clean up the s3.
    
    :param: time_span_days
    :return: None
    """
    cleanup_s3_files(time_span_days)
 
if __name__ == "__main__":
    pass
