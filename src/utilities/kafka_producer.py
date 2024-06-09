import logging
import os
import sys
from time import sleep
from actual_load_producer import ActualLoadProd
from dotenv import load_dotenv
from setup_logging import setup_logging

load_dotenv('.env')
setup_logging(os.environ.get('LOGGING_PATH'))

SFTP_URL = os.environ.get('SFTPTOGO_URL')
RSA_KEY = os.environ.get('RSA_KEY')

BOOTSTRAP_SERVERS =  os.environ.get('BOOTSTRAP_SERVERS')
PRODUCE_TOPIC_ACTUALLOAD_CSV  =  os.environ.get('PRODUCE_TOPIC_ACTUALLOAD_CSV')
MONTH =  os.environ.get('MONTH')
YEAR =  os.environ.get('YEAR')
RESOURCE_PATH =  os.environ.get('RESOURCE_PATH')
DATE_TO_READ =  os.environ.get('DATE_TO_READ')


config = {
    'bootstrap_servers': [BOOTSTRAP_SERVERS]}



producer = ActualLoadProd(YEAR,MONTH,RESOURCE_PATH,SFTP_URL,RSA_KEY,DATE_TO_READ,props=config)
while True:
    filename = producer.download_actual_load()
    records = producer.read_records(filename)
    producer.publish(topic=PRODUCE_TOPIC_ACTUALLOAD_CSV, records = records)
    logging.info("producer will sleep for 15 minutes.")
    sleep(15*60)