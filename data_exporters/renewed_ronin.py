from mage_ai.streaming.sinks.base_python import BasePythonSink
from typing import Callable

if 'streaming_sink' not in globals():
    from mage_ai.data_preparation.decorators import streaming_sink

from datetime import datetime
from os import path
from pandas import DataFrame
import pandas as pd
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.s3 import S3
from src.utilities.utils import create_s3_keys_dates_actual_load

config_path = path.join(get_repo_path(), 'io_config.yaml')
config_profile = 'default'
bucket_name = kwargs['bucket_name']

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@streaming_sink
class CustomSink(BasePythonSink):
    def init_client(self):
        """
        Implement the logic of initializing the client.
        """
        self.df = None  

    def batch_write(self, messages):
        """
        Batch write the messages to the sink.

        For each message, the message format could be one of the following ones:
        1. message is the whole data to be written into the sink
        2. message contains the data and metadata with the format {"data": {...}, "metadata": {...}}
            The data value is the data to be written into the sink. The metadata is used to store
            extra information that can be used in the write method (e.g. timestamp, index, etc.).
        """
        for msg in messages:
            if "data" in msg:
                new_df = pd.DataFrame(msg["data"])
                if self.df is not None:
                    self.df  = pd.concat([self.df , new_df], ignore_index=True)
                else:
                    self.df = new_df
                
            else:
                print(f"Message missing 'data' key: {msg}")
            for object_key, date in create_s3_keys_dates_actual_load(kwargs['data_item_name'], kwargs['data_item_no'],kwargs['date_to_read']):
                df_date = df[(df.day == date.day) & (df.month == date.month) & (df.year == date.year)]
                logging.info(f"File {object_key} has been written to aws s3.")
                S3.with_config(ConfigFileLoader(config_path, config_profile)).export(
                df_date.reset_index(drop=True),
                bucket_name,
                object_key,
                )




    
    
    

            