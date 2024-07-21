import logging
from time import sleep
from config import Config
from setup_logging import SetupLogging
from kafka_class.producer import KafkaProducerService
from api.entsoe import EntsoeDataDownloader

def main():
    SetupLogging(Config.LOG_DIR)
    
    kafka_props = {
        'bootstrap_servers': [Config.BOOTSTRAP_SERVERS]
    }

    producer_service = KafkaProducerService(props= kafka_props,field_name= 'date',last_published_field_value= Config.LAST_PUBLISHED_FIELD_VALUE)
    data_downloader = EntsoeDataDownloader(
        year=int(Config.YEAR), 
        month=int(Config.MONTH), 
        country_code=Config.COUNTRY_CODE
    )

    filter_funcs = {'datetome_to_publish': lambda row: row['date'][:10] == Config.DATE_TO_READ}
    
    while True:
        filename = data_downloader.download_data(data_type=Config.DATA_TYPE)
        if filename:
            records = producer_service.read_records(filename, filter_funcs, Config.FIELDS)
            producer_service.publish(topic=Config.PRODUCE_TOPIC_ACTUALLOAD_CSV, records=records)
            Config.set_env_variable('LAST_PUBLISHED_FIELD_VALUE', producer_service.get_last_published_field_value())
            logging.info(f"Producer will sleep for {Config.TIME_OF_SLEEP_PRODUCER} minutes.")
            sleep(int(Config.TIME_OF_SLEEP_PRODUCER) * 60)

if __name__ == '__main__':
    main()
