import logging
import time 

import src.setup_logging 
import src.kafka_class.producer 
import src.api.yahoo 

from src.config import Config

def main():
    src.setup_logging.SetupLogging(Config.LOG_DIR)
    
    kafka_props = {
        'bootstrap_servers': [Config.BOOTSTRAP_SERVERS]
    }

    producer_service = src.kafka_class.producer.KafkaProducerService(props=kafka_props, field_name='date', last_published_field_value=Config.LAST_PUBLISHED_FIELD_VALUE_GAS)
    yahoo_finance_api = src.api.yahoo.YahooAPI(symbol=Config.TICKER_LABEL)

    while True:
        
        # Fetch the latest data
        yahoo_finance_api.fetch_data(period='1d',interval='5m')
        yahoo_finance_api.transform_data()
        data = yahoo_finance_api.data
        if not data.empty:
            # Define filter functions if needed (e.g., filter by date)
            filter_funcs = {}
            
            # Read and publish the records
            records = producer_service.read_records_from_dataframe(data, filter_funcs, Config.FIELDS_GAS)
            producer_service.publish(topic= Config.PRODUCE_TOPIC_GAS_PRICE, records=records)
            
            # Update the last published field value in the environment
            Config.set_env_variable('LAST_PUBLISHED_FIELD_VALUE_GAS', producer_service.get_last_published_field_value())
        
        logging.info(f"Producer will sleep for {Config.TIME_OF_SLEEP_PRODUCER_GAS} minutes.")
        time.sleep(int(Config.TIME_OF_SLEEP_PRODUCER_GAS) * 60)

if __name__ == '__main__':
    main()
