
import sys
import json
import boto3
import logging
import datetime
import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq

if '/home/mohammad/RenewableInsight' not in sys.path:
    sys.path.append('/home/mohammad/RenewableInsight')

from io import BytesIO
from prefect import task, flow
from pandas import DataFrame
from confluent_kafka import Consumer, KafkaError

from src.utilities.utils import create_s3_keys_generation, check_s3_key_exists, generate_random_string
from src.config import Config
from src.api.entsoe_api import ENTSOEAPI

def create_dataframe(messages):
    df = pd.DataFrame(messages)
    return df

@task
def consume_data():
    consumer_config = {
        'bootstrap.servers': Config.BOOTSTRAP_SERVERS,
        'group.id': Config.GROUP_ID_LOAD,
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(consumer_config)
    consumer.subscribe([Config.PRODUCE_TOPIC_ACTUALLOAD_CSV])
    
    messages = []
    start_time = datetime.datetime.now()

    try:
        while datetime.datetime.now() - start_time < datetime.timedelta(seconds=15):
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    break
            
            message_value = msg.value().decode('utf-8')
            message_dict = json.loads(message_value)
            messages.append(message_dict)

    except KeyboardInterrupt:
        print("Consumer interrupted.")
    finally:
        consumer.close()

    # Create DataFrame from the accumulated messages
    df = create_dataframe(messages)
    return df

@task
def process_data(df):
    # Example processing: Just print the DataFrame
    print(df)
    # You can add more processing steps here
    return df

# Defining the flow
@flow(log_prints=True)
def etl():
    data = consume_data()
    processed_data = process_data(data)

# Run the flow
if __name__ == "__main__":
    etl()