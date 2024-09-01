import time
import logging

import src.api.yahoo
from src.config import Config
import src.kafka_class.producer

try:
    config = Config()
    print("configuration loaded successfully!")
except ValidationError as e:
    print("configuration error:", e)

def main(field_gas: list[str]) -> None:
    """
    Main function to continuously fetch, transform, and publish gas price data from Yahoo Finance to a Kafka topic.

    The function fetches data in 5-minute intervals for the last 5 days, transforms it, and publishes it to a Kafka topic.
    The last published timestamp is stored and updated in an environment variable to prevent reprocessing of old data.
    """

 
    kafka_props = {
        'bootstrap_servers': [config.BOOTSTRAP_SERVERS_PROD]
    }

    logger = logging.getLogger(__name__)

  
    producer_service = src.kafka_class.producer.KafkaProducerService(
        props=kafka_props, 
        field_name='date', 
        last_published_field_value=config.LAST_PUBLISHED_FIELD_VALUE_GAS
    )


    yahoo_finance_api = src.api.yahoo.YahooAPI(symbol=config.TICKER_LABEL_GAS)

    while True:
      
        yahoo_finance_api.fetch_data(period='5d', interval='5m')
        yahoo_finance_api.transform_data()
        data = yahoo_finance_api.data

        
        if not data.empty:
            filter_funcs = {}  

            
            records = producer_service.read_records_from_dataframe(data, filter_funcs, field_gas)
            producer_service.publish(topic=config.PRODUCE_TOPIC_GAS_PRICE, records=records)

           
            config.set_env_variable('LAST_PUBLISHED_FIELD_VALUE_GAS', producer_service.get_last_published_field_value())
        else:
            logger.error("The gas data is empty.")
        
        logger.info(f"Producer will sleep for {config.TIME_OF_SLEEP_PRODUCER_GAS} minutes.")
        time.sleep(int(config.TIME_OF_SLEEP_PRODUCER_GAS) * 60)

if __name__ == '__main__':
    field_gas = ['date', 'open_price', 'close_price']
    main(field_gas)
