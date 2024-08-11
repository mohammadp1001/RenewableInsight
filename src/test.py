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
from prefect import task, flow
from pandas import DataFrame
from src.config import Config
from src.api.parameters import WeatherParameter

print(Config.WEATHER_PARAM)
print(WeatherParameter(Config.WEATHER_PARAM))