import csv
from time import sleep
from typing import Dict
from kafka import KafkaProducer
import sys
import os
from dotenv import load_dotenv
from urllib.parse import urlparse

# Ensure src directory is in the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.SSH import SSH
from settings import BOOTSTRAP_SERVERS, INPUT_DATA_PATH, PRODUCE_TOPIC_ACTUALLOAD_CSV, YEAR , MONTH

# Load environment variables from a specified .env file
load_dotenv('/home/mohammadp/RenewableInsight/.env')

def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for record {}: {}".format(msg.key(), err))
        return
    print('Record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


class ActualLoadProd:
    def __init__(self, props: Dict):
        self.producer = KafkaProducer(**props)

    @staticmethod
    def download_actual_load(month: str, year: str):

        data_item_name = "ActualTotalLoad" 
        data_item_no = "6.1.A"
        sftp_url = os.environ.get('SFTPTOGO_URL')
        rsa_key = os.environ.get('RSA_KEY')
        if not sftp_url:
            print("First, please set environment variable SFTPTOGO_URL and try again.")
            exit(0)
        parsed_url = urlparse(sftp_url)
        ssh = SSH(
            hostname=parsed_url.hostname,
            username=parsed_url.username,
            password=parsed_url.password,
            hostkey=rsa_key)
        base_directory = '/tmp/'
        query_filename = f"/TP_export/{data_item_name}_{data_item_no}/{year}_{month}_{data_item_name}_{data_item_no}.csv"
        ssh.connect()
        ssh.open_sftp()
        ssh.download_file(query_filename,base_directory)
        ssh.disconnect()
        filename = f"{year}_{month}_{data_item_name}_{data_item_no}.csv"

        return filename

    @staticmethod
    def read_records(resource_path: str):
        filename = ActualLoadProd.download_actual_load(MONTH,YEAR)
        records, record_keys = [], []
        i = 0
        with open(resource_path + filename, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)  # skip the header
            for row in reader:
                row = row[0].split("\t")
                if row[5] == "DE":
                    # DateTime, ResolutionCode, AreaCode, AreaTypeCode, AreaName, MapCode,TotalLoadValue
                    records.append(f'{row[0]}, {row[1]}, {row[2]}, {row[3]}, {row[4]}, {row[5]}, {row[6]}')
                    record_keys.append(str(i))
                    i += 1
                    # if i == 5:
                    #    break
        return zip(record_keys, records)

    def publish(self, topic: str, records: [str, str]):
        for key_value in records:
            key, value = key_value
            try:
                self.producer.send(topic=topic, key=key, value=value)
                print(f"Producing record for <key: {key}, value:{value}>")
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Exception while producing record - {value}: {e}")

        self.producer.flush()
        sleep(1)

if __name__ == "__main__":
    pass
