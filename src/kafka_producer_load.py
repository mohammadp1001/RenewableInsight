import logging
import datetime

from time import sleep
from pydantic import ValidationError

from src.config import Config

import src.api.entsoe_api
import src.kafka_class.producer

try:
    config = Config()
except ValidationError as e:
    print("configuration error:", e)


def main(field_load: list[str]) -> None:
    """
    Main function to fetch, transform, and publish load data from the ENTSO-E API to a Kafka topic.

    The function continuously fetches load data, filters it based on the current date, 
    and publishes it to a Kafka topic. The last published timestamp is stored and updated 
    in an environment variable to prevent reprocessing of old data.
    """
    logger = logging.getLogger(__name__)


    kafka_props = {
        'bootstrap_servers': [config.BOOTSTRAP_SERVERS_PROD]
    }


    producer_service = src.kafka_class.producer.KafkaProducerService(
        props=kafka_props,
        field_name='date',
        last_published_field_value=config.LAST_PUBLISHED_FIELD_VALUE_LOAD
    )


    data_downloader = src.api.entsoe_api.ENTSOEAPI(
        year=datetime.datetime.now().year,
        month=datetime.datetime.now().month,
        country_code=config.COUNTRY_CODE,
        api_key=config.ENTSOE_API_KEY
    )


    filter_funcs = {
        'datetome_to_publish': lambda row: row['date'].strftime('%Y-%m-%d') == datetime.datetime.now().strftime('%Y-%m-%d')
    }

    while True:
        data_downloader.fetch_data(data_type=config.DATA_TYPE_LOA)
        data = data_downloader.data

        if not data.empty:
            
            records = producer_service.read_records_from_dataframe(data, filter_funcs, field_load)
            producer_service.publish(topic=config.PRODUCE_TOPIC_ACTUALLOAD_CSV, records=records)

            
            config.set_env_variable('LAST_PUBLISHED_FIELD_VALUE_LOAD', producer_service.get_last_published_field_value())
        else:
            logger.error("The load data is empty.")
        
        logger.info(f"Producer will sleep for {config.TIME_OF_SLEEP_PRODUCER_LOAD} minutes.")
        sleep(int(config.TIME_OF_SLEEP_PRODUCER_LOAD) * 60)

if __name__ == '__main__':
    field_load = ['date', 'load']
    main(field_load)
