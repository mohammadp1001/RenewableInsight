"""
Actual Load Producer Module

This module provides functionality to download actual load data from an SFTP server,
read the data, and publish it to a Kafka topic.
"""

import csv
import logging
import os
from time import sleep
from typing import Dict, Generator
from urllib.parse import urlparse
from kafka import KafkaProducer
from ssh import SSH
import json

class ActualLoadProd:
    """
    Class to handle the production of actual load data to Kafka.
    """
    
    def __init__(self, year, month, resource_path, sftp_url, rsa_key, date_to_read, props: Dict):
        """
        Initialize the ActualLoadProd instance.

        Args:
            year (int): The year of the data.
            month (int): The month of the data.
            resource_path (str): The path to the resource directory.
            sftp_url (str): The URL of the SFTP server.
            rsa_key (str): The RSA key for SFTP authentication.
            date_to_read (str): The date to filter the records.
            props (Dict): Kafka producer properties.
        """
        # Remove value_serializer from props if it exists to avoid conflict
        if 'value_serializer' in props:
            props.pop('value_serializer')
        self.producer = KafkaProducer(value_serializer=self.serializer, **props)
        self.year = year
        self.month = month
        self.resource_path = resource_path
        self.data_item_name = "ActualTotalLoad"
        self.data_item_no = "6.1.A"
        self.query_filename = f"/TP_export/{self.data_item_name}_{self.data_item_no}/{self.year}_{self.month}_{self.data_item_name}_{self.data_item_no}.csv"
        self.sftp_url = sftp_url
        self.rsa_key = rsa_key
        self.date_to_read = date_to_read
        self._index = 0

    def download_actual_load(self):
        """
        Download the actual load data from the SFTP server.

        Returns:
            str: The filename of the downloaded data.
        """
        if not self.sftp_url:
            logging.error("First, please set environment variable SFTPTOGO_URL and try again.")
            exit(1)
        parsed_url = urlparse(self.sftp_url)
    
        ssh = SSH(
            hostname=parsed_url.hostname,
            username=parsed_url.username,
            password=parsed_url.password,
            hostkey=self.rsa_key
        )
        ssh.connect()
        ssh.open_sftp()
        logging.info(f"Trying to download {self.query_filename}")
        ssh.download_file(self.query_filename, self.resource_path)
        ssh.disconnect()
        return f"{self.year}_{self.month}_{self.data_item_name}_{self.data_item_no}.csv"

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
        with open(os.path.join(self.resource_path, filename), 'r') as f:
            reader = csv.reader(f)
            header = next(reader)
            for row in reader:
                row = row[0].split('\t')
                if row[5] == 'DE' and row[0][:10] == self.date_to_read:
                    record = {
                        'DateTime': row[0],
                        'AreaName': row[4],
                        'TotalLoadValue': row[6],
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

        # Send any remaining records
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
    # logging.basicConfig(level=logging.INFO)
    # Example usage
    # props = {'bootstrap.servers': 'localhost:9092'}
    # actual_load_prod = ActualLoadProd(2023, 5, '/path/to/resource', 'sftp://example.com', 'rsa_key', '2023-05-01', props)
    # filename = actual_load_prod.download_actual_load()
    # records = actual_load_prod.read_records(filename)
    # actual_load_prod.publish('actual_load_topic', records)
