import logging
import json
import pandas as pd
from datetime import datetime
from time import sleep
from typing import Dict, Generator, Any, List, Optional

from kafka import KafkaProducer

class KafkaProducerService:
    """
    Class to handle the production of data to Kafka.
    """
    
    def __init__(self, props: Dict, field_name: str, last_published_field_value: Optional[str] = None):
        """
        Initialize the KafkaProducerService instance.

        Args:
            props (Dict): Kafka producer properties.
            field_name (str): The column name to use as filter (e.g. datetime).
        """
        if 'value_serializer' in props:
            props.pop('value_serializer')
        self.producer = KafkaProducer(value_serializer=self.serializer, **props)
        self._index = 0
        if last_published_field_value:
            self._last_published_field_value = datetime.fromisoformat(last_published_field_value)
        else:
            self._last_published_field_value = None
        self._field_name = field_name

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

    def is_record_processed(self, record: Dict) -> bool:
        if self._last_published_field_value is None:
            return False
        record_value = record[self._field_name]
        return record_value <= self._last_published_field_value

    def get_last_published_field_value(self) -> str:
        return self._last_published_field_value.isoformat()

    def _mark_record_as_processed(self, record: Dict) -> None:
        self._last_published_field_value = record[self._field_name]

    def read_records(self, filename: str, filter_funcs: Dict[str, Any], fields: List[str]) -> Generator[Dict, None, None]:
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
            if all(filter_func(row) for filter_func in filter_funcs.values()):
                record = {field: row[field] for field in fields}
                record['key_id'] = f'record_{self._index}'
                if self.is_record_processed(record):
                    continue
                self._index += 1
                self._mark_record_as_processed(record)
                yield record    

    def read_records_from_dataframe(self, dataframe: pd.DataFrame, filter_funcs: Dict[str, Any], fields: List[str]) -> Generator[Dict, None, None]:
        """
        Read records from a DataFrame.

        Args:
            dataframe (pd.DataFrame): The DataFrame to read from.
            filter_funcs (Dict[str, Any]): A dictionary of filters to apply on the data.
            fields (List[str]): A list of fields to include in the output.

        Yields:
            dict: A record dictionary.
        """
        for _, row in dataframe.iterrows():
            if all(filter_func(row) for filter_func in filter_funcs.values()):
                record = {field: row[field] for field in fields}
                record['key_id'] = f'record_{self._index}'
                if self.is_record_processed(record):
                    continue
                self._index += 1
                self._mark_record_as_processed(record)
                record['date'] = str(record['date'])
                yield record  
                
    def publish(self, topic: str, records: Generator[Dict, None, None], batch_size: int = 5):
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

if __name__ == "__main__":
    pass
