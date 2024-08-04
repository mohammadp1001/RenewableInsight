"""
Data Loader Module

This module provides a function to load data from the ENTSO-E API.
"""
import os
import sys
import datetime
import pandas as pd

if '/home/src/RenewableInsight' not in sys.path:
    sys.path.append('/home/src/RenewableInsight')

from src.config import Config

from src.api.entsoe_api import ENTSOEAPI
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load_data(**kwargs):
    """
    Load data from the ENTSO-E API.

    Args:
        **kwargs: Additional keyword arguments.

            month (int): The month of the data.
            year (int): The year of the data.
            data_type (str): The data_type name.
            
    Returns:
        DataFrame: The loaded DataFrame.
    """
    data_type = kwargs['data_type']

    data_downloader = ENTSOEAPI(year=datetime.datetime.now().year,month=datetime.datetime.now().month,country_code=Config.COUNTRY_CODE,api_key=Config.ENTSOE_API_KEY)
    data_downloader.fetch_data(data_type=data_type)
    data = data_downloader.data

    return data
