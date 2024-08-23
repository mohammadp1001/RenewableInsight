import sys
import json
import boto3
import logging
import datetime
import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq

if '/home/mohammad/RenewableInsight' not in sys.path:
    sys.path.append('/home/mohammad/RenewableInsight')

from io import BytesIO
from pathlib import Path
from pandas import DataFrame
from prefect import task, flow
from prefect import get_run_logger
from confluent_kafka import Consumer, KafkaError


from src.config import Config
from src.api.forecast import DwdMosmixParser, kml_reader
from src.utilities.utils import create_s3_keys_weather_forecast, check_s3_key_exists, generate_random_string, download_kmz_file


@task
def load_data():
    """
    Extracts forecast data for a given station from a KMZ file and returns it as a pandas DataFrame.

    Parameters:
    - station_name: The name of the station to extract the forecast data for.
    - kmz_file: Path to the KMZ file containing the forecast data.

    Returns:
    - A pandas DataFrame containing the forecast data for the specified station, or None if the station is not found.
    """
    logger = get_run_logger()            
    url = "https://opendata.dwd.de/weather/local_forecasts/mos/MOSMIX_S/all_stations/kml/MOSMIX_S_LATEST_240.kmz" 
   
    save_dir = Path("/tmp") 
    filename = "MOSMIX_S_LATEST_240.kmz"
    station_name = Config.STATION_NAME
    
    kmz_file_path = download_kmz_file(url, save_dir, filename)

    parser = DwdMosmixParser()

 
    with kml_reader(kmz_file_path) as fp:
        timestamps = list(parser.parse_timestamps(fp))
        fp.seek(0)  
        forecasts = list(parser.parse_forecasts(fp, {station_name}))

    if not forecasts:
        logger.warning(f"No data found for station: {station_name}")
        
    data = parser.convert_to_dataframe(forecasts, station_name)

    data["forecast_time"] = timestamps
    
    return data


@task
def transform(data):
    """
    """
    columns = ["forecast_time","TTT","TX","TN","DD","FF","Rad1h","ww","N","SunD1"]

    data = data[columns]

    data.loc[:,'day'] = data.forecast_time.dt.day
    data.loc[:,'month'] = data.forecast_time.dt.month
    data.loc[:,'year'] = data.forecast_time.dt.year
   

    data = data.rename(columns={"TTT": "temperature_2m", "TX": "temperature_max", "TN": "temperature_min", "DD": "wind_direction",
    "FF": "wind_speed", "Rad1h": "global_irradiance","ww": "significant_weather", "N": "total_cloud_cover","SunD1":"sunshine_dur"})

   
    for col in data.columns:
        if pd.api.types.is_integer_dtype(data[col]):
            data[col] = data[col].astype('int16')
        if pd.api.types.is_float_dtype(data[col]):
            data[col] = data[col].astype('float32')    


    data.temperature_2m = data.temperature_2m - 273.15
    data.temperature_max = data.temperature_max - 273.15
    data.temperature_min = data.temperature_min - 273.15
    

    return data

@task
def export_data_to_s3(data: DataFrame) -> None:

    parquet_buffer = BytesIO()
    logger = get_run_logger()
    bucket_name = Config.BUCKET_NAME
    s3 = boto3.client('s3', aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY)

    for object_key, date in create_s3_keys_weather_forecast(int(Config.N_DAY),Config.STATION_NAME):
        data_ = data[(data.day == date.day) & (data.month == date.month) & (data.year == date.year)]
        table = pa.Table.from_pandas(data_)
        pq.write_table(table, parquet_buffer)
        filename = object_key + f"/{generate_random_string(10)}.parquet"
        if not check_s3_key_exists(s3,bucket_name,object_key):
            s3.put_object(
                Bucket=bucket_name,
                Key=filename,
                Body=parquet_buffer.getvalue()
                )
            logger.info(f"File has been written to s3 {bucket_name} inside {object_key}.")
        else:

            logger.info(f"File {object_key} already exists.")

# Defining the flow
@flow(log_prints=True)
def etl():
    data = load_data()
    transformed_data = transform(data)
    export_data_to_s3(transformed_data)


# Run the flow
if __name__ == "__main__":
    etl()       