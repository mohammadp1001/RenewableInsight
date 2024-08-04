import logging
import datetime

from time import sleep

import src.api.entsoe_api 
import src.setup_logging 
import src.kafka_class.producer
from src.config import Config 


def main():
    src.setup_logging.SetupLogging(Config.LOG_DIR)
    
    kafka_props = {
        'bootstrap_servers': [Config.BOOTSTRAP_SERVERS]
    }

    producer_service = src.kafka_class.producer.KafkaProducerService(props= kafka_props,field_name= 'date',last_published_field_value= Config.LAST_PUBLISHED_FIELD_VALUE_LOAD)
    data_downloader = src.api.entsoe_api.ENTSOEAPI(
        year=datetime.datetime.now().year, 
        month=datetime.datetime.now().month, 
        country_code=Config.COUNTRY_CODE,
        api_key=Config.ENTSOE_API_KEY
    )

    filter_funcs = {'datetome_to_publish': lambda row: row['date'].strftime('%Y-%m-%d') == datetime.datetime.now().strftime('%Y-%m-%d')}
    
    while True:
        data_downloader.fetch_data(data_type=Config.DATA_TYPE)
        data = data_downloader.data
        if not data.empty:
            records = producer_service.read_records_from_dataframe(data, filter_funcs, Config.FIELDS_LOAD)
            producer_service.publish(topic=Config.PRODUCE_TOPIC_ACTUALLOAD_CSV, records=records)

            Config.set_env_variable('LAST_PUBLISHED_FIELD_VALUE_LOAD', producer_service.get_last_published_field_value())
            
            logging.info(f"Producer will sleep for {Config.TIME_OF_SLEEP_PRODUCER_LOAD} minutes.")
            sleep(int(Config.TIME_OF_SLEEP_PRODUCER_LOAD) * 60)

if __name__ == '__main__':
    main()
