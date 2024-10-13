import sys
import os
from datetime import datetime, timedelta
from prefect import flow, serve
from prefect import get_run_logger
from pydantic import ValidationError


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