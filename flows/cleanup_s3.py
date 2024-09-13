import sys
import boto3

from pandas import DataFrame
from prefect import task, flow
from prefect import get_run_logger
from datetime import datetime, timezone, timedelta

if '/home/mohammad/RenewableInsight' not in sys.path:
    sys.path.append('/home/mohammad/RenewableInsight')

from src.config import Config
from src.utilities.utils import generate_task_name, generate_flow_name


try:
    config = Config()
except ValidationError as e:
    print("configuration error:", e)

@task(task_run_name=generate_task_name)
def remove_old_s3_files(prefix: str, days_old: int = 5) -> None:
    """
    Removes files from an S3 bucket that are older than the specified number of days.

    :param prefix: The S3 key prefix to filter files (can be empty for all files).
    :param days_old: The age of files in days to remove (default is 5 days).
    """
    bucket_name = config.BUCKET_NAME
    
    logger = get_run_logger()  
    s3 = boto3.client(
        's3',
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY
    )

    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_old)

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if 'Contents' in response:
        for file in response['Contents']:
            file_key = file['Key']
            file_last_modified = file['LastModified']

            if file_last_modified < cutoff_date:
                logger.info(f"Deleting file {file_key} last modified on {file_last_modified}")
                
                s3.delete_object(Bucket=bucket_name, Key=file_key)
                logger.info(f"Deleted {file_key}")
    else:
        logger.info("No files found for the given prefix.")

@flow(log_prints=True,name="cleanup_s3",flow_run_name=generate_flow_name)
def cleanup_flow(prefix: str, days_old: int = 5) -> None:
    """
    The flow that clean up the s3.
    
    :param: prefix
    :param: days_old
    :return: None
    """
    remove_old_s3_files(prefix, days_old)
 
if __name__ == "__main__":
    cleanup_flow(prefix="")
