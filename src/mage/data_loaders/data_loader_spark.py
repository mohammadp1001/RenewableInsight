from mage_ai.streaming.sources.base_python import BasePythonSource
from typing import Callable
from RenewableInsight.utilities.streaming import read_from_kafka,parse_ride_from_kafka_message
from pyspark.sql import SparkSession
from RenewableInsight.utilities.settings import RECORDS_SCHEMA



if 'streaming_source' not in globals():
    from mage_ai.data_preparation.decorators import streaming_source



@streaming_source
class CustomSource(BasePythonSource):
    def init_client(self):
        """
        Implement the logic of initializing the client.
        """
        self.CONSUME_TOPIC_ACTUALLOAD_CSV  = 'actualload'

    def batch_read(self, handler: Callable):
        """
        Batch read the messages from the source and use handler to process the messages.
        """
        while True:
            df_consume_stream = read_from_kafka(consume_topic=self.CONSUME_TOPIC_ACTUALLOAD_CSV)
            df_records = parse_ride_from_kafka_message(df_consume_stream, RECORDS_SCHEMA)
            
            # Implement the logic of fetching the records
            if df_records:
                handler(df_records)
