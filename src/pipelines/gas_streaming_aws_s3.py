import sys
import json
import boto3
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

if '/home/mohammad/RenewableInsight' not in sys.path:
    sys.path.append('/home/mohammad/RenewableInsight')

from src.utilities.utils import create_s3_keys_gas, check_s3_key_exists, generate_random_string
from src.config import Config

def create_dataframe(messages: list) -> pd.DataFrame:
    """
    Create a pandas DataFrame from a list of messages.

    :param messages: List of message dictionaries.
    :return: DataFrame created from the messages.
    """
    df = pd.DataFrame(messages)
    return df

@task
def consume_data() -> pd.DataFrame:
    """
    Consumes messages from a Kafka topic for a specified duration and returns them as a DataFrame.

    :return: A pandas DataFrame containing the consumed messages.
    :rtype: pd.DataFrame
    """
    logger = get_run_logger()
    consumer_config = {
        'bootstrap.servers': Config.BOOTSTRAP_SERVERS_CONS,
        'group.id': Config.GROUP_ID_GAS,
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(consumer_config)
    consumer.subscribe([Config.PRODUCE_TOPIC_GAS_PRICE])
    
    messages = []
    start_time = datetime.datetime.now()

    try:
        logger.info("The consumer starts for 1 minute.")
        while datetime.datetime.now() - start_time < datetime.timedelta(seconds=60):
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
    data = pd.DataFrame(messages, columns=['date', 'open_price', 'close_price', 'key_id'])
    return data

@task
def transform(data: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the consumed data by parsing dates, converting data types, 
    and extracting additional time-based features.

    :param data: The consumed data as a pandas DataFrame.
    :return: A transformed pandas DataFrame.
    """
    logger = get_run_logger()

    data['date'] = pd.to_datetime(data['date'])
    data['open_price'] = data['open_price'].astype('float32')
    data['close_price'] = data['close_price'].astype('float32')

    data = data.drop(columns=['key_id'])

    logger.info("Extract date and time components.")

    data['day'] = data['date'].dt.day
    data['year'] = data['date'].dt.year
    data['hour'] = data['date'].dt.hour
    data['month'] = data['date'].dt.month
    data['minute'] = data['date'].dt.minute

    data['day'] = data['day'].astype('int8')
    data['hour'] = data['hour'].astype('int8')
    data['year'] = data['year'].astype('int16')
    data['month'] = data['month'].astype('int8')
    data['minute'] = data['minute'].astype('int8')

    return data

@task
def export_data_to_s3(data: pd.DataFrame) -> None:
    """
    Exports the transformed data to an S3 bucket in Parquet format.

    :param data: The transformed data as a pandas DataFrame.
    :return: None
    """
    logger = get_run_logger()
    bucket_name = Config.BUCKET_NAME
    s3 = boto3.client('s3', 
                      aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY)

    for object_key, date in create_s3_keys_gas():
        data_ = data[(data.hour == date.hour) & 
                     (data.day == date.day) & 
                     (data.month == date.month) & 
                     (data.year == date.year)]

        if data_.empty:
            logger.info("The dataframe is empty possibly due to lack of messages.")
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

@flow(log_prints=True)
def etl() -> None:
    """
    The ETL flow that orchestrates the consuming, transforming, and exporting of gas price data.

    :return: None
    """
    data = consume_data()
    transformed_data = transform(data)
    export_data_to_s3(transformed_data)

if __name__ == "__main__":
    etl()
