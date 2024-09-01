import logging
import datetime
from time import sleep

import src.api.entsoe_api
import src.kafka_class.producer
from src.config import Config

def main():
    """
    Main function to fetch, transform, and publish load data from the ENTSO-E API to a Kafka topic.

    The function continuously fetches load data, filters it based on the current date, 
    and publishes it to a Kafka topic. The last published timestamp is stored and updated 
    in an environment variable to prevent reprocessing of old data.
    """
    logger = logging.getLogger(__name__)


    kafka_props = {
        'bootstrap_servers': [Config.BOOTSTRAP_SERVERS_PROD]
    }


    producer_service = src.kafka_class.producer.KafkaProducerService(
        props=kafka_props,
        field_name='date',
        last_published_field_value=Config.LAST_PUBLISHED_FIELD_VALUE_LOAD
    )


    data_downloader = src.api.entsoe_api.ENTSOEAPI(
        year=datetime.datetime.now().year,
        month=datetime.datetime.now().month,
        country_code=Config.COUNTRY_CODE,
        api_key=Config.ENTSOE_API_KEY
    )


    filter_funcs = {
        'datetome_to_publish': lambda row: row['date'].strftime('%Y-%m-%d') == datetime.datetime.now().strftime('%Y-%m-%d')
    }

    while True:
        data_downloader.fetch_data(data_type=Config.DATA_TYPE_LOA)
        data = data_downloader.data

        if not data.empty:
            
            records = producer_service.read_records_from_dataframe(data, filter_funcs, Config.FIELDS_LOAD)
            producer_service.publish(topic=Config.PRODUCE_TOPIC_ACTUALLOAD_CSV, records=records)

            
            Config.set_env_variable('LAST_PUBLISHED_FIELD_VALUE_LOAD', producer_service.get_last_published_field_value())
        else:
            logger.error("The load data is empty.")
        
        logger.info(f"Producer will sleep for {Config.TIME_OF_SLEEP_PRODUCER_LOAD} minutes.")
        sleep(int(Config.TIME_OF_SLEEP_PRODUCER_LOAD) * 60)

if __name__ == '__main__':
    main()
