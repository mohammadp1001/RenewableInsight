import csv
import logging
from time import sleep
from typing import Dict, Generator
from kafka import KafkaProducer
import json
import pandas as pd

class KafkaProducerService:
    """
    Class to handle the production of data to Kafka.
    """
    
    def __init__(self, props: Dict):
        """
        Initialize the KafkaProducerService instance.

        Args:
            props (Dict): Kafka producer properties.
        """
        if 'value_serializer' in props:
            props.pop('value_serializer')
        self.producer = KafkaProducer(value_serializer=self.serializer, **props)
        self._index = 0

    @staticmethod
    def serializer(message):
        """
        Serialize a message to JSON format.

        Args:
            message (dict): The message to serialize.

        Returns:
            bytes: The serialized message.
        """
        return json.dumps(message).encode('utf-8')

    def read_records(self, filename: str, filters: Dict[str, Any], fields: List[str]) -> Generator[Dict, None, None]:
        """
        Read records from the downloaded CSV file.

        Args:
            filename (str): The name of the file to read from.
            filters (Dict[str, Any]): A dictionary of filters to apply on the data.
            fields (List[str]): A list of fields to include in the output.

        Yields:
            dict: A record dictionary.
        """
        df = pd.read_csv(filename)
        for _, row in df.iterrows():
            if all(row[k] == v for k, v in filters.items()):
                record = {field: row[field] for field in fields}
                record['key_id'] = f'record_{self._index}'
                self._index += 1
                yield record

    def publish(self, topic: str, records: Generator[Dict, None, None], batch_size: int = 100):
        """
        Publish records to the specified Kafka topic in batches.

        Args:
            topic (str): The Kafka topic to publish to.
            records (Generator): A generator of records to publish.
            batch_size (int): The number of records to publish in each batch.
        """
        batch = []
        for record in records:
            batch.append(record)
            if len(batch) >= batch_size:
                self._send_batch(topic, batch)
                batch.clear()

        if batch:
            self._send_batch(topic, batch)

    def _send_batch(self, topic: str, batch: list):
        """
        Helper method to send a batch of records to Kafka.

        Args:
            topic (str): The Kafka topic to publish to.
            batch (list): A list of records to publish.
        """
        for record in batch:
            try:
                self.producer.send(topic=topic, value=record)
                logging.info(f"Producing record for key_id: {record['key_id']}")
            except KeyboardInterrupt:
                break
            except Exception as e:
                logging.error(f"Exception while producing record - {record}: {e}", exc_info=True)
        self.producer.flush()
        sleep(1)