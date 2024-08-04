import os
from dotenv import load_dotenv, find_dotenv, set_key

# Load .env file
load_dotenv(find_dotenv())

class Config:
    """class for config"""
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    TICKER_LABEL = os.getenv('TICKER_LABEL')
    LOG_DIR = os.getenv('LOG_DIR')
    ENTSOE_API_KEY = os.getenv('ENTSOE_API_KEY')
    RESOURCE_PATH = os.getenv('RESOURCE_PATH')
    BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS')
    PRODUCE_TOPIC_GAS_PRICE = os.getenv('PRODUCE_TOPIC_GAS_PRICE')
    PRODUCE_TOPIC_ACTUALLOAD_CSV = os.getenv('PRODUCE_TOPIC_ACTUALLOAD_CSV')
    MONTH = os.getenv('MONTH')
    YEAR = os.getenv('YEAR')
    TIME_OF_SLEEP_PRODUCER_GAS = os.getenv('TIME_OF_SLEEP_PRODUCER_GAS')
    TIME_OF_SLEEP_PRODUCER_LOAD = os.getenv('TIME_OF_SLEEP_PRODUCER_LOAD')
    DATA_TYPE = os.getenv('DATA_TYPE')
    DATE_TO_READ = os.getenv('DATE_TO_READ')
    COUNTRY_CODE = os.getenv('COUNTRY_CODE')
    LAST_PUBLISHED_FIELD_VALUE_LOAD = os.getenv('LAST_PUBLISHED_FIELD_VALUE_LOAD')
    LAST_PUBLISHED_FIELD_VALUE_GAS = os.getenv('LAST_PUBLISHED_FIELD_VALUE_GAS')
    FIELDS_LOAD = ['date', 'load']
    FIELDS_GAS = ['date', 'open_price', 'close_price']

    @staticmethod
    def set_env_variable(variable,value):
        set_key('.env', variable, value)

