import sys
from datetime import datetime, timedelta
from prefect import flow, serve

if '/home/mohammad/RenewableInsight' not in sys.path:
    sys.path.append('/home/mohammad/RenewableInsight')

from src.config import Config
from flows.cleanup_s3 import cleanup_flow
from flows.s3_to_bigquery import s3_to_bigquery_flow
from flows.weather_forecast_etl_s3 import weather_forecast_etl_flow
from flows.actual_generation_etl_s3 import actual_generation_etl_flow
from flows.gas_streaming_s3 import gas_streaming_s3_flow
from flows.historical_weather_etl_s3 import historical_weather_etl_flow

try:
    config = Config()
except ValidationError as e:
    print("configuration error:", e)


start_time = datetime.combine(datetime.now() + timedelta(days=1), datetime.min.time())

@flow
def orchestrator_weather_forecast_flow(station_name: str, n_day: int, prefix: str, bigquery_table_id: str, time_column: str, expiration_time: int):
    
    weather_state = weather_forecast_etl_flow(station_name, n_day, return_state=True)

    if weather_state.is_completed():
        s3_to_bigquery_flow(prefix, bigquery_table_id, time_column, expiration_time)
    else:
        print(f"Flow failed with state: {weather_state.type}")

@flow
def orchestrator_actual_generation_flow(year: int, month: int, country_code: str, data_type: str,prefix: str, bigquery_table_id: str, time_column: str, expiration_time: int):
    
    actual_generation_state = actual_generation_etl_flow(year, month, country_code, data_type, return_state=True)

    if actual_generation_state.is_completed():
        s3_to_bigquery_flow(prefix, bigquery_table_id, time_column, expiration_time)
    else:
        print(f"Flow failed with state: {actual_generation_state.type}")

@flow 
def orchestrator_gas_streaming_flow(wait_time: int,prefix: str, bigquery_table_id: str, time_column: str, expiration_time: int):
    
    gas_streaming_state = gas_streaming_s3_flow(wait_time, return_state=True)

    if gas_streaming_state.is_completed():
        s3_to_bigquery_flow(prefix, bigquery_table_id, time_column, expiration_time)
    else:
        print(f"Flow failed with state: {gas_streaming_state.type}")


@flow 
def orchestrator_load_streaming_flow(wait_time: int,prefix: str, bigquery_table_id: str, time_column: str, expiration_time: int):
    
    load_streaming_state = load_streaming_s3_flow(wait_time, return_state=True)

    if load_streaming_state.is_completed():
        s3_to_bigquery_flow(prefix, bigquery_table_id, time_column, expiration_time)
    else:
        print(f"Flow failed with state: {load_streaming_state.type}")

@flow 
def orchestrator_historical_weather_flow(weather_param: str, station_code: str,prefix: str, bigquery_table_id: str, time_column: str, expiration_time: int):
    
    historical_weather_state = historical_weather_etl_flow(weather_param,station_code, return_state=True)

    if historical_weather_state.is_completed():
        s3_to_bigquery_flow(prefix, bigquery_table_id, time_column, expiration_time)
    else:
        print(f"Flow failed with state: {historical_weather_state.type}")



if __name__ == "__main__":
    
    orchestrator_weather_forecast_deploy = orchestrator_weather_forecast_flow.to_deployment(
        name="weather_forecast_etl",
        interval= 60,  
        parameters={
            "station_name": config.STATION_NAME,
            "n_day": 3,  
            "prefix":f"weather_forecast/{config.STATION_NAME}/",
            "bigquery_table_id": "weather_forecast_stuttgart",
            "time_column": None,
            "expiration_time": 1
        },
        tags=["forecast", "aws", "etl"],
    )

    orchestrator_actual_generation_deploy = orchestrator_actual_generation_flow.to_deployment(
        name="actual_generation_etl",
        interval= 120,  
        parameters={
            "year": 2024,
            "month": 3,
            "country_code": config.COUNTRY_CODE ,
            "data_type": config.DATA_TYPE_GEN,
            "prefix": "electricity/generation/",
            "bigquery_table_id": "weather_forecast_stuttgart",
            "time_column": None,
            "expiration_time": 1
        },
        tags=["generation", "aws", "etl"],
    )

    serve(
        orchestrator_weather_forecast_deploy,
        orchestrator_actual_generation_deploy
    )
