import sys
import os
import json
import boto3
import pytz
import logging
import datetime
import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq

from io import BytesIO
from pandas import DataFrame
from prefect import task, flow
from prefect import get_run_logger
from confluent_kafka import Consumer, KafkaError

path_to_append = os.getenv('PYTHON_APP_PATH')
if path_to_append:
    sys.path.append(path_to_append)

from src.config import Config
from src.api.entsoe_api import ENTSOEAPI
from src.utilities.utils import create_s3_keys_load, check_s3_key_exists, generate_random_string, generate_task_name, generate_flow_name

try:
    config = Config()
except ValidationError as e:
    print("configuration error:", e)

@task(task_run_name=generate_task_name)
def consume_data(wait_time: int) -> pd.DataFrame:
    """
    Consume messages from a Kafka topic for a specified duration and return them as a DataFrame.

    :param wait_time waiting time for consuming messages in minutes.
    :return: A pandas DataFrame containing the consumed messages.
    """
    logger = get_run_logger()
    consumer_config = {
        'bootstrap.servers': config.BOOTSTRAP_SERVERS_CONS,
        'group.id': config.GROUP_ID_LOAD,
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(consumer_config)
    consumer.subscribe([config.PRODUCE_TOPIC_ACTUALLOAD_CSV])
    
    messages = []
    start_time = datetime.datetime.now()

    try:
        logger.info(f"The consumer starts for {wait_time} minutes.")
        while datetime.datetime.now() - start_time < datetime.timedelta(minutes=wait_time):
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                    break
            message_value = msg.value().decode('utf-8')
            message_dict = json.loads(message_value)
            messages.append(message_dict)

    except KeyboardInterrupt:
        logger.info("Consumer interrupted.")
    finally:
        consumer.close()

    logger.info("Create DataFrame from the accumulated messages.")
    data = pd.DataFrame(messages, columns=['date', 'load', 'key_id'])
    return data

@task(task_run_name=generate_task_name)
def transform(data: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the consumed data by parsing dates, converting data types, 
    and extracting additional time-based features.

    :param data: The consumed data as a pandas DataFrame.
    :return: A transformed pandas DataFrame.
    """
    logger = get_run_logger()
    berlin_tz = pytz.timezone(config.TIMEZONE)

    data['date'] = pd.to_datetime(data['date'], format="ISO8601")
   
    data['date'] = data['date'].dt.tz_localize('UTC')  
    data['date'] = data['date'].dt.tz_convert(berlin_tz) 

    data['load'] = data['load'].astype('float32')
    data = data.drop(columns=['key_id'])

    logger.info("Extract date and time components.")

    data['day'] = data['date'].dt.day
    data['month'] = data['date'].dt.month
    data['year'] = data['date'].dt.year
    data['hour'] = data['date'].dt.hour
    data['minute'] = data['date'].dt.minute


    data['month'] = data['month'].astype('int8')
    data['year'] = data['year'].astype('int16')
    data['hour'] = data['hour'].astype('int8')
    data['minute'] = data['minute'].astype('int8')
    data['day'] = data['day'].astype('int8')

    return data

@task(task_run_name=generate_task_name)
def export_data_to_s3(data: pd.DataFrame) -> None:
    """
    Export the transformed data to an S3 bucket in Parquet format.

    :param data: The transformed data as a pandas DataFrame.
    :return: None
    """
    logger = get_run_logger()
    bucket_name = config.BUCKET_NAME
    s3 = boto3.client(
        's3', 
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY
    )

    for object_key, date in create_s3_keys_load():
        data_ = data[(data.hour == date.hour) & 
                     (data.day == date.day) & 
                     (data.month == date.month) & 
                     (data.year == date.year)]

        if data_.empty:
            logger.info("The dataframe is empty, possibly due to lack of messages.")
            continue

        parquet_buffer = BytesIO()
        table = pa.Table.from_pandas(data_)
        pq.write_table(table, parquet_buffer)

        parquet_buffer.seek(0)

        filename = object_key + f"/{generate_random_string(10)}.parquet"

        if not check_s3_key_exists(s3, bucket_name, object_key):
            s3.put_object(
                Bucket=bucket_name,
                Key=filename,
                Body=parquet_buffer.getvalue()
            )
            logger.info(f"File has been written to s3 {bucket_name} inside {object_key}.")
        else:
            logger.info(f"File {object_key} already exists.")
      
        parquet_buffer.close()

@flow(log_prints=True,name="load_streaming_s3",flow_run_name=generate_flow_name)
def load_streaming_s3_flow(wait_time: int) -> None:
    """
    The ETL flow that orchestrates the consuming, transforming, and exporting of load data.

    :param wait_time waiting time for consuming messages in minutes.
    :return: None
    """
    data = consume_data(wait_time)
    transformed_data = transform(data)
    export_data_to_s3(transformed_data)

if __name__ == "__main__":
    load_streaming_s3_flow(wait_time=1) 
