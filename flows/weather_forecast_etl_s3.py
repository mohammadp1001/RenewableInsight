import sys
import json
import boto3
import pytz
import logging
import datetime
import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq

from io import BytesIO
from pathlib import Path
from pandas import DataFrame
from prefect import task, flow
from prefect import get_run_logger
from pydantic import ValidationError
from confluent_kafka import Consumer, KafkaError

if '/home/mohammad/RenewableInsight' not in sys.path:
    sys.path.append('/home/mohammad/RenewableInsight')

from src.config import Config
from src.api.forecast import DwdMosmixParser, kml_reader
from src.utilities.utils import create_s3_keys_weather_forecast, check_s3_key_exists, generate_random_string, download_kmz_file, generate_task_name, generate_flow_name

try:
    config = Config()
except ValidationError as e:
    print("configuration error:", e)

@task(task_run_name=generate_task_name)
def load_data(station_name: str) -> pd.DataFrame:
    """
    Extracts forecast data for a given station from a KMZ file and returns it as a pandas DataFrame.

    :return: A pandas DataFrame containing the forecast data for the specified station.
    """
    logger = get_run_logger()
    url = "https://opendata.dwd.de/weather/local_forecasts/mos/MOSMIX_S/all_stations/kml/MOSMIX_S_LATEST_240.kmz"

    save_dir = Path("/tmp")
    filename = "MOSMIX_S_LATEST_240.kmz"
   
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

@task(task_run_name=generate_task_name)
def transform(data: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the forecast data by selecting specific columns, renaming them, 
    and adjusting data types for efficient storage.

    :param data: The original forecast data as a pandas DataFrame.
    :return: A transformed pandas DataFrame with selected columns and adjusted data types.
    """
    columns = ["forecast_time", "TTT", "TX", "TN", "DD", "FF", "Rad1h", "ww", "N", "SunD1"]
    berlin_tz = pytz.timezone('Europe/Berlin')
    data['forecast_time'] = data['forecast_time'].dt.tz_convert(berlin_tz)
    data = data[columns]

    data = data.assign(
        day=data.forecast_time.dt.day,
        month=data.forecast_time.dt.month,
        year=data.forecast_time.dt.year
    )


    data = data.rename(columns={
        "TTT": "temperature_2m", "TX": "temperature_max", "TN": "temperature_min",
        "DD": "wind_direction", "FF": "wind_speed", "Rad1h": "global_irradiance",
        "ww": "significant_weather", "N": "total_cloud_cover", "SunD1": "sunshine_dur"
    })

    for col in data.columns:
        if pd.api.types.is_integer_dtype(data[col]):
            data[col] = data[col].astype('int16')
        if pd.api.types.is_float_dtype(data[col]):
            data[col] = data[col].astype('float32')

    data.temperature_2m = data.temperature_2m - 273.15
    data.temperature_max = data.temperature_max - 273.15
    data.temperature_min = data.temperature_min - 273.15

    return data

@task(task_run_name=generate_task_name)
def export_data_to_s3(data: DataFrame, station_name: str, n_day: int) -> None:
    """
    Exports the transformed forecast data to an S3 bucket in Parquet format.

    :param data: The transformed forecast data as a pandas DataFrame.
    :return: None
    """
    logger = get_run_logger()
    bucket_name = config.BUCKET_NAME
    s3 = boto3.client('s3', aws_access_key_id=config.AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY)

    for object_key, date in create_s3_keys_weather_forecast(n_day, station_name):
        data_ = data[(data.day == date.day) & (data.month == date.month) & (data.year == date.year)]
        
        if data_.empty:
            logger.info("The dataframe is empty possibly due to lack of messages.")
            continue
        
        parquet_buffer = BytesIO()
        table = pa.Table.from_pandas(data_)
        pq.write_table(table, parquet_buffer)

        parquet_buffer.seek(0)

        filename = object_key + f"/{generate_random_string(10)}.parquet"
        if not check_s3_key_exists(s3, bucket_name, object_key):
            s3.put_object(
                Bucket=bucket_name,
                Key=filename,
                Body=parquet_buffer.getvalue()
            )
            logger.info(f"File has been written to s3 {bucket_name} inside {object_key}.")
        else:
            logger.info(f"File {object_key} already exists.")
        
        parquet_buffer.close()    

@flow(log_prints=True, name="weather_forecast_etl_s3", flow_run_name=generate_flow_name)
def weather_forecast_etl_flow(station_name: str, n_day: int) -> None:
    """
    The ETL flow that orchestrates the loading, transforming, and exporting of weather forecast data.

    :return: None
    """
    data = load_data(station_name)
    transformed_data = transform(data)
    export_data_to_s3(transformed_data,station_name, n_day)
    
if __name__ == "__main__":
    weather_forecast_etl_flow(station_name= config.STATION_NAME, n_day = 5)