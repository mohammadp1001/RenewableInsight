import time
import logging

import src.api.yahoo
from src.config import Config
import src.kafka_class.producer

def main():
    """
    Main function to continuously fetch, transform, and publish gas price data from Yahoo Finance to a Kafka topic.

    The function fetches data in 5-minute intervals for the last 5 days, transforms it, and publishes it to a Kafka topic.
    The last published timestamp is stored and updated in an environment variable to prevent reprocessing of old data.
    """

 
    kafka_props = {
        'bootstrap_servers': [Config.BOOTSTRAP_SERVERS_PROD]
    }

    logger = logging.getLogger(__name__)

  
    producer_service = src.kafka_class.producer.KafkaProducerService(
        props=kafka_props, 
        field_name='date', 
        last_published_field_value=Config.LAST_PUBLISHED_FIELD_VALUE_GAS
    )


    yahoo_finance_api = src.api.yahoo.YahooAPI(symbol=Config.TICKER_LABEL_GAS)

    while True:
      
        yahoo_finance_api.fetch_data(period='5d', interval='5m')
        yahoo_finance_api.transform_data()
        data = yahoo_finance_api.data

        
        if not data.empty:
            filter_funcs = {}  

            
            records = producer_service.read_records_from_dataframe(data, filter_funcs, Config.FIELDS_GAS)
            producer_service.publish(topic=Config.PRODUCE_TOPIC_GAS_PRICE, records=records)

           
            Config.set_env_variable('LAST_PUBLISHED_FIELD_VALUE_GAS', producer_service.get_last_published_field_value())
        else:
            logger.error("The gas data is empty.")
        
        logger.info(f"Producer will sleep for {Config.TIME_OF_SLEEP_PRODUCER_GAS} minutes.")
        time.sleep(int(Config.TIME_OF_SLEEP_PRODUCER_GAS) * 60)

if __name__ == '__main__':
    main()
