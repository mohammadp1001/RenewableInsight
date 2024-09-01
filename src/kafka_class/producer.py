import json
import logging
import pandas as pd

from time import sleep
from datetime import datetime
from kafka import KafkaProducer
from typing import Dict, Generator, Any, List, Optional


class KafkaProducerService:
    """
    A service class for handling the production of data to Kafka.
    """

    def __init__(self, props: Dict[str, Any], field_name: str, last_published_field_value: Optional[str] = None):
        """
        Initialize the KafkaProducerService instance.

        :param props: Kafka producer properties.
        :param field_name: The column name to use as a filter (e.g., datetime).
        :param last_published_field_value: The last published field value to start processing from, in ISO format.
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
    def serializer(message: Dict[str, Any]) -> bytes:
        """
        Serialize a message to JSON format.

        :param message: The message to serialize.
        :return: The serialized message as bytes.
        """
        return json.dumps(message).encode('utf-8')

    def is_record_processed(self, record: Dict[str, Any]) -> bool:
        """
        Check if a record has already been processed.

        :param record: The record to check.
        :return: True if the record has been processed, False otherwise.
        """
        if self._last_published_field_value is None:
            return False
        record_value = record[self._field_name]
        return record_value <= self._last_published_field_value

    def get_last_published_field_value(self) -> str:
        """
        Get the last published field value as an ISO-formatted string.

        :return: The last published field value.
        """
        return self._last_published_field_value.isoformat()

    def _mark_record_as_processed(self, record: Dict[str, Any]) -> None:
        """
        Mark a record as processed by updating the last published field value.

        :param record: The record to mark as processed.
        """
        self._last_published_field_value = record[self._field_name]

    def read_records(self, filename: str, filter_funcs: Dict[str, Any], fields: List[str]) -> Generator[Dict[str, Any], None, None]:
        """
        Read records from a CSV file and filter them based on provided functions.

        :param filename: The name of the CSV file to read from.
        :param filter_funcs: A dictionary of filter functions to apply on the data.
        :param fields: A list of fields to include in the output records.
        :yield: A filtered record dictionary.
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

    def read_records_from_dataframe(self, dataframe: pd.DataFrame, filter_funcs: Dict[str, Any], fields: List[str]) -> Generator[Dict[str, Any], None, None]:
        """
        Read records from a pandas DataFrame and filter them based on provided functions.

        :param dataframe: The DataFrame to read from.
        :param filter_funcs: A dictionary of filter functions to apply on the data.
        :param fields: A list of fields to include in the output records.
        :yield: A filtered record dictionary.
        """
        for _, row in dataframe.iterrows():
            if all(filter_func(row) for filter_func in filter_funcs.values()):
                record = {field: row[field] for field in fields}
                record['key_id'] = f'record_{self._index}'
                if self.is_record_processed(record):
                    continue
                self._index += 1
                self._mark_record_as_processed(record)
                record['date'] = str(record['date'])  # Ensuring the date is string-encoded
                yield record

    def publish(self, topic: str, records: Generator[Dict[str, Any], None, None], batch_size: int = 5) -> None:
        """
        Publish records to the specified Kafka topic in batches.

        :param topic: The Kafka topic to publish to.
        :param records: A generator of records to publish.
        :param batch_size: The number of records to publish in each batch. Default is 5.
      
        """
        batch = []
        for record in records:
            batch.append(record)
            if len(batch) >= batch_size:
                self._send_batch(topic, batch)
                batch.clear()

        if batch:
            self._send_batch(topic, batch)

    def _send_batch(self, topic: str, batch: List[Dict[str, Any]]) -> None:
        """
        Helper method to send a batch of records to Kafka.

        :param topic: The Kafka topic to publish to.
        :param batch: A list of records to publish.
        """
        logger = logging.getLogger(__name__)
        for record in batch:
            try:
                self.producer.send(topic=topic, value=record)
                logger.info(f"Producing record for key_id: {record['key_id']}")
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Exception while producing record - {record}: {e}", exc_info=True)
        self.producer.flush()
        sleep(1)

if __name__ == "__main__":
    pass
