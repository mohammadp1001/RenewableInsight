import sys
import os
from datetime import datetime, timedelta
from prefect import flow, serve
from prefect import get_run_logger

path_to_append = os.getenv('PYTHON_APP_PATH')
if path_to_append:
    sys.path.append(path_to_append)

from src.config import Config
from flows.cleanup_s3 import cleanup_flow
from src.api.parameters import WeatherParameter
from flows.s3_to_bigquery import s3_to_bigquery_flow
from flows.weather_forecast_etl_s3 import weather_forecast_etl_flow
from flows.actual_generation_etl_s3 import actual_generation_etl_flow
from flows.gas_streaming_s3 import gas_streaming_s3_flow
from flows.load_streaming_s3 import load_streaming_s3_flow
from flows.historical_weather_etl_s3 import historical_weather_etl_flow
from flows.cleanup_s3 import cleanup_flow

from src.utilities.utils import generate_flow_name

try:
    config = Config()
except ValidationError as e:
    print("configuration error:", e)


start_time = datetime.combine(datetime.now() + timedelta(days=1), datetime.min.time())

@flow(log_prints=True,name="orchestrator_weather_forecast",flow_run_name=generate_flow_name) 
def orchestrator_weather_forecast_flow(station_name: str, n_day: int, prefix: str, bigquery_table_id: str, partition_column: str, expiration_time: int):
    logger = get_run_logger()

    weather_state = weather_forecast_etl_flow(station_name, n_day, return_state=True)

    if weather_state.is_completed():
        s3_to_bigquery_flow(prefix, bigquery_table_id, partition_column, expiration_time)
    else:
        logger.error(f"Flow failed with state: {weather_state.type}")

@flow(log_prints=True,name="orchestrator_actual_generation",flow_run_name=generate_flow_name) 
def orchestrator_actual_generation_flow(year: int, month: int, country_code: str, data_type: str,prefix: str, bigquery_table_id: str, partition_column: str, expiration_time: int):
    logger = get_run_logger()

    actual_generation_state = actual_generation_etl_flow(year, month, country_code, data_type, return_state=True)

    if actual_generation_state.is_completed():
        s3_to_bigquery_flow(prefix, bigquery_table_id, partition_column, expiration_time)
    else:
        logger.error(f"Flow failed with state: {actual_generation_state.type}")

@flow(log_prints=True,name="orchestrator_gas_streaming",flow_run_name=generate_flow_name) 
def orchestrator_gas_streaming_flow(wait_time: int,prefix: str, bigquery_table_id: str, partition_column: str, expiration_time: int):
    logger = get_run_logger()

    gas_streaming_state = gas_streaming_s3_flow(wait_time, return_state=True)

    if gas_streaming_state.is_completed():
        s3_to_bigquery_flow(prefix, bigquery_table_id, partition_column, expiration_time)
    else:
        logger.error(f"Flow failed with state: {gas_streaming_state.type}")


@flow(log_prints=True,name="orchestrator_load_streaming",flow_run_name=generate_flow_name)  
def orchestrator_load_streaming_flow(wait_time: int,prefix: str, bigquery_table_id: str, partition_column: str, expiration_time: int):
    logger = get_run_logger()

    load_streaming_state = load_streaming_s3_flow(wait_time, return_state=True)

    if load_streaming_state.is_completed():
        s3_to_bigquery_flow(prefix, bigquery_table_id, partition_column, expiration_time)
    else:
        logger.error(f"Flow failed with state: {load_streaming_state.type}")

@flow(log_prints=True,name="orchestrator_historical_weather",flow_run_name=generate_flow_name) 
def orchestrator_historical_weather_flow(weather_param: str, station_code: str,prefix: str, bigquery_table_id: str, partition_column: str, expiration_time: int):
    logger = get_run_logger()

    historical_weather_state = historical_weather_etl_flow(weather_param,station_code, return_state=True)

    if historical_weather_state.is_completed():
        s3_to_bigquery_flow(prefix, bigquery_table_id, partition_column, expiration_time)
    else:
        logger.error(f"Flow failed with state: {historical_weather_state.type}")



if __name__ == "__main__":
    
    orchestrator_weather_forecast_deploy = orchestrator_weather_forecast_flow.to_deployment(
        name="weather_forecast_etl",
        # cron="55 23 * * *",
        cron="0/15 21 * * *",
        parameters={
            "station_name": config.STATION_NAME,
            "n_day": 3,  
            "prefix":f"weather_forecast/{config.STATION_NAME}/",
            "bigquery_table_id": "weather_forecast_stuttgart",
            "partition_column": "forecast_time",
            "expiration_time": 7
        },
        tags=["forecast", "aws", "etl"],
      
    )

    orchestrator_actual_generation_deploy = orchestrator_actual_generation_flow.to_deployment(
        name="actual_generation_etl",
        # cron="0 0 */5 * *",
        cron="*/10 * * * *",  
        parameters={
            "year": 2024,
            "month": 10,
            "country_code": config.COUNTRY_CODE ,
            "data_type": config.DATA_TYPE_GEN,
            "prefix": "electricity/generation/",
            "bigquery_table_id": "actual_generation",
            "partition_column": "date",
            "expiration_time": 7
        },
        tags=["generation", "aws", "etl"],
    
    )

    orchestrator_historical_weather_deploy = orchestrator_historical_weather_flow.to_deployment(
        name="historical_weather_etl",
        # cron="5 0 * * *",
        cron="*/10 * * * *",  
        parameters={
            "weather_param": config.WEATHER_PARAM, 
            "station_code": config.STATION_CODE,
            "prefix": f"historical_weather/{WeatherParameter[config.WEATHER_PARAM].category}/{config.STATION_CODE}",
            "bigquery_table_id": f"historical_weather_{config.WEATHER_PARAM}",
            "partition_column": "measurement_time",
            "expiration_time": 7
        },
        tags=["historical", "aws", "etl"],
      
    )


    orchestrator_load_streaming_deploy = orchestrator_load_streaming_flow.to_deployment(
        name="load_streaming",
        # cron="*/16 * * * *",  
        cron="*/10 * * * *",
        parameters={
            "wait_time": 5, 
            "prefix": "electricity/load/",
            "bigquery_table_id": "load",
            "partition_column": "date",
            "expiration_time": 7
        },
        tags=["load", "aws", "streaming"],
 
    )

    orchestrator_gas_streaming_deploy = orchestrator_gas_streaming_flow.to_deployment(
        name="gas_streaming",
        # cron="*/16 * * * *",
        cron="*/10 * * * *",  
        parameters={
            "wait_time": 5, 
            "prefix": "others/gas",
            "bigquery_table_id": "gas",
            "partition_column": "date",
            "expiration_time": 7
        },
        tags=["gas", "aws", "streaming"],
        
    )

    orchestrator_cleanup_deploy = cleanup_flow.to_deployment(
        name="cleanup",
        # cron="0 0 */7 * *",
        cron="0/45 22 * * *",  
        parameters={
        "time_span_days": 7   
        },
        tags=["aws", "cleanup"],
      
    )

    serve(
        
        orchestrator_weather_forecast_deploy,
        orchestrator_actual_generation_deploy,
        orchestrator_historical_weather_deploy,
        orchestrator_load_streaming_deploy,
        orchestrator_gas_streaming_deploy,
        orchestrator_cleanup_deploy
        )
