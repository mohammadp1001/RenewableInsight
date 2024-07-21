import os
from dotenv import load_dotenv,find_dotenv,set_key

# Load .env file
load_dotenv(find_dotenv())

class Config:
    
    LOG_DIR = os.getenv('LOG_DIR')
    ENTSOE_API_KEY = os.getenv('ENTSOE_API_KEY')
    RESOURCE_PATH = os.getenv('RESOURCE_PATH')
    BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS')
    PRODUCE_TOPIC_ACTUALLOAD_CSV = os.getenv('PRODUCE_TOPIC_ACTUALLOAD_CSV')
    MONTH = os.getenv('MONTH')
    YEAR = os.getenv('YEAR')
    TIME_OF_SLEEP_PRODUCER = os.getenv('TIME_OF_SLEEP_PRODUCER')
    DATA_TYPE = os.getenv('DATA_TYPE')
    DATE_TO_READ = os.getenv('DATE_TO_READ')
    COUNTRY_CODE = os.getenv('COUNTRY_CODE')
    LAST_PUBLISHED_FIELD_VALUE = os.getenv('LAST_PUBLISHED_FIELD_VALUE')
    FIELDS = ['date','load']
     
    def set_env_variable(slef,variable,value):

        set_key('.env', variable, value)

        