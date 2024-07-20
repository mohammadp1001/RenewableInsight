import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

class Config:
    KAFKA_BROKER = os.getenv('KAFKA_BROKER')
    ENTSOE_API_KEY = os.getenv('ENTSOE_API_KEY')
    RESOURCE_PATH = os.getenv('RESOURCE_PATH', './resources')