import csv
import logging
import os
from time import sleep
from typing import Dict, Generator
from kafka import KafkaProducer
from entsoe import EntsoePandasClient
import json
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential
from config import Config

class ActualLoadProd:
    """
    Class to handle the production of actual load data to Kafka.
    """
    
    def __init__(self, year, month, date_to_read, country_code, props: Dict):
        """
        Initialize the ActualLoadProd instance.

        Args:
            year (int): The year of the data.
            month (int): The month of the data.
            date_to_read (str): The date to filter the records.
            country_code (str): The country code to filter the records.
            props (Dict): Kafka producer properties.
        """
        if 'value_serializer' in props:
            props.pop('value_serializer')
        self.producer = KafkaProducer(value_serializer=self.serializer, **props)
        self.year = year
        self.month = month
        self.resource_path = Config.RESOURCE_PATH
        self.api_key = Config.ENTSOE_API_KEY
        self.date_to_read = date_to_read
        self.country_code = country_code
        self._index = 0
        self.client = EntsoePandasClient(api_key=self.api_key)

    @staticmethod
    def get_end_date(year: int, month: int) -> pd.Timestamp:
        """
        Get the end date of the month.

        Args:
            year (int): The year.
            month (int): The month.

        Returns:
            pd.Timestamp: The end date of the month.
        """
        return pd.Timestamp(f'{year}-{month+1}-01', tz='Europe/Brussels') - pd.Timedelta(days=1)

    def validate_data(self, data: pd.DataFrame) -> bool:
        """
        Validate the fetched data.

        Args:
            data (pd.DataFrame): The data to validate.

        Returns:
            bool: True if data is valid, False otherwise.
        """
        return not data.empty

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def download_actual_load(self):
        """
        Download the actual load data using the ENTSO-E API.

        Returns:
            str: The filename of the downloaded data.
        """
        start = pd.Timestamp(f'{self.year}-{self.month}-01', tz='Europe/Brussels')
        end = self.get_end_date(self.year, self.month)

        try:
            ts = self.client.query_load(self.country_code, start=start, end=end)
            if not self.validate_data(ts):
                raise ValueError("Invalid data received from the API")
            ts = self.transform_data(ts)
            filename = os.path.join(self.resource_path, f"{self.year}_{self.month}_ActualTotalLoad.csv")
            ts.to_csv(filename)
            return filename

        except Exception as e:
            logging.error(f"Error fetching data from ENTSO-E API: {e}")
            return None

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

    def read_records(self, filename: str) -> Generator[Dict, None, None]:
        """
        Read records from the downloaded CSV file.

        Args:
            filename (str): The name of the file to read from.

        Yields:
            dict: A record dictionary.
        """
        df = pd.read_csv(filename)
        for _, row in df.iterrows():
            if row['AreaName'] == self.country_code and row['DateTime'][:10] == self.date_to_read:
                record = {
                    'DateTime': row['DateTime'],
                    'AreaName': row['AreaName'],
                    'TotalLoadValue': row['TotalLoadValue'],
                    'key_id': f'record_{self._index}'
                }
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

if __name__ == "__main__":
    pass
