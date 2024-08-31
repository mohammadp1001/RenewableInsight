import os
from dotenv import load_dotenv, find_dotenv, set_key


load_dotenv(find_dotenv())

class Config:
    """
    A class to represent and manage configuration settings for the application.

    The configuration settings are loaded from environment variables, typically set in a .env file.
    """

    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    BIGQUERY_TABEL_ID = os.getenv('BIGQUERRY_TABEL_ID')
    GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    BIGQUERY_DATASET_ID = os.getenv('BIGQUERY_DATASET_ID')
    PROJECT_ID = os.getenv('PROJECT_ID')
    TICKER_LABEL_GAS = os.getenv('TICKER_LABEL_GAS')
    TICKER_LABEL_OIL = os.getenv('TICKER_LABEL_OIL')
    LOG_DIR = os.getenv('LOG_DIR')
    CONFIG_DIR = os.getenv('CONFIG_DIR')
    ENTSOE_API_KEY = os.getenv('ENTSOE_API_KEY')
    RESOURCE_PATH = os.getenv('RESOURCE_PATH')
    BOOTSTRAP_SERVERS_CONS = os.getenv('BOOTSTRAP_SERVERS_CONS')
    BOOTSTRAP_SERVERS_PROD = os.getenv('BOOTSTRAP_SERVERS_PROD')
    GROUP_ID_LOAD = os.getenv('GROUP_ID_LOAD')
    GROUP_ID_GAS = os.getenv('GROUP_ID_GAS')
    PRODUCE_TOPIC_GAS_PRICE = os.getenv('PRODUCE_TOPIC_GAS_PRICE')
    PRODUCE_TOPIC_ACTUALLOAD_CSV = os.getenv('PRODUCE_TOPIC_ACTUALLOAD_CSV')
    TIME_OF_SLEEP_PRODUCER_GAS = os.getenv('TIME_OF_SLEEP_PRODUCER_GAS')
    TIME_OF_SLEEP_PRODUCER_LOAD = os.getenv('TIME_OF_SLEEP_PRODUCER_LOAD')
    DATA_TYPE_LOA = os.getenv('DATA_TYPE_LOA')
    DATA_TYPE_GEN = os.getenv('DATA_TYPE_GEN')
    BUCKET_NAME = os.getenv('BUCKET_NAME')
    COUNTRY_CODE = os.getenv('COUNTRY_CODE')
    LAST_PUBLISHED_FIELD_VALUE_LOAD = os.getenv('LAST_PUBLISHED_FIELD_VALUE_LOAD')
    LAST_PUBLISHED_FIELD_VALUE_GAS = os.getenv('LAST_PUBLISHED_FIELD_VALUE_GAS')
    FIELDS_LOAD = ['date', 'load']
    FIELDS_GAS = ['date', 'open_price', 'close_price']
    STATION_NAME = os.getenv('STATION_NAME')
    STATION_CODE = os.getenv('STATION_CODE')
    WEATHER_PARAM = os.getenv('WEATHER_PARAM')
    N_DAY = os.getenv('N_DAY')

    @staticmethod
    def set_env_variable(variable, value):
        """
        Set an environment variable in the .env file.

        :param variable: The name of the environment variable to set.
        :param value: The value to set for the environment variable.
        :return: True if the environment variable was successfully set, False otherwise.
        """
        dotenv_path = find_dotenv()
        if dotenv_path:
            set_key(dotenv_path, variable, value)
            return True
        return False

