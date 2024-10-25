import os
from dotenv import load_dotenv, find_dotenv
from pydantic_settings import BaseSettings

load_dotenv(find_dotenv())

class Config(BaseSettings):
    """
    A class to represent and manage configuration settings for the application.
    The configuration settings are loaded from environment variables, typically set in a .env file.
    """

    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    BIGQUERY_DATASET_ID: str
    BIGQUERY_PROJECT_ID: str
    GOOGLE_APPLICATION_CREDENTIALS: str
    BIGQUERY_DATASET_ID: str
    PROJECT_ID: str
    TICKER_LABEL_GAS: str
    TICKER_LABEL_OIL: str
    ENTSOE_API_KEY: str
    RESOURCE_PATH: str
    BOOTSTRAP_SERVERS_CONS: str
    BOOTSTRAP_SERVERS_PROD: str
    GROUP_ID_LOAD: str
    GROUP_ID_GAS: str
    PRODUCE_TOPIC_GAS_PRICE: str
    PRODUCE_TOPIC_ACTUALLOAD_CSV: str
    TIME_OF_SLEEP_PRODUCER_GAS: int
    TIME_OF_SLEEP_PRODUCER_LOAD: int
    DATA_TYPE_LOA: str
    DATA_TYPE_GEN: str
    BUCKET_NAME: str
    COUNTRY_CODE: str
    LAST_PUBLISHED_FIELD_VALUE_LOAD: str
    LAST_PUBLISHED_FIELD_VALUE_GAS: str
    FIELDS_LOAD: list[str] = ['date', 'load']
    FIELDS_GAS: list[str] = ['date', 'open_price', 'close_price']
    STATION_NAME: str
    STATION_CODE: str
    WEATHER_PARAM: str
    PROJECT_NAME: str  
    TIMEZONE: str

    class Config:
        env_file = '.env'  
        env_file_encoding = 'utf-8'
        case_sensitive = True

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
            from dotenv import set_key
            set_key(dotenv_path, variable, value)
            return True
        return False
