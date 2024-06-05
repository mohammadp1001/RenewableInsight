import logging
import os
import sys
from time import sleep
from actual_load_producer import ActualLoadProd
from dotenv import load_dotenv
from setup_logging import setup_logging

load_dotenv('/home/mohammadp/RenewableInsight/docker_compose/.env')
setup_logging('/home/mohammadp/RenewableInsight/logs')

SFTP_URL = os.environ.get('SFTPTOGO_URL')
RSA_KEY = os.environ.get('RSA_KEY')

BOOTSTRAP_SERVERS = 'localhost:9092'
PRODUCE_TOPIC_ACTUALLOAD_CSV  = 'actualload'
MONTH = '05'
YEAR = '2024'
RESOURCE_PATH = '/home/mohammadp/RenewableInsight/data'
DATE_TO_READ = '2024-05-05'


config = {
    'bootstrap_servers': [BOOTSTRAP_SERVERS]}



producer = ActualLoadProd(YEAR,MONTH,RESOURCE_PATH,SFTP_URL,RSA_KEY,DATE_TO_READ,props=config)
while True:
    filename = producer.download_actual_load()
    records = producer.read_records(filename)
    producer.publish(topic=PRODUCE_TOPIC_ACTUALLOAD_CSV, records = records)
    logging.info("producer will sleep for 15 minutes.")
    sleep(15*60)