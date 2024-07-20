import os
from dotenv import load_dotenv,find_dotenv

# Load .env file
load_dotenv(find_dotenv())

class Config:
    
    LOG_DIR = os.getenv('LOG_DIR')
    KAFKA_BROKER = os.getenv('KAFKA_BROKER')
    ENTSOE_API_KEY = os.getenv('ENTSOE_API_KEY')
    RESOURCE_PATH = os.getenv('RESOURCE_PATH')