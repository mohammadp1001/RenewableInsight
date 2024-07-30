"""
Data Loader Module

This module provides a function to load data from the ENTSO-E API.
"""

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


from RenewableInsight.src.api.entsoe import ENTSOEAPI
from RenewableInsight.src.config import Config
import logging
import pandas as pd
import os


@data_loader
def load_data(**kwargs):
    """
    Load data from the ENTSO-E API.

    Args:
        **kwargs: Additional keyword arguments.

            month (int): The month of the data.
            year (int): The year of the data.
            
    Returns:
        DataFrame: The loaded DataFrame.
    """
    setup_logging(kwargs['LOG_DIR'])
    logger = logging.getLogger(__name__)
    
    # Using snake_case for variable names
    month = kwargs['month']
    year = kwargs['year']
    data_type = kwargs['generation']
   

    if not api_key:
        logger.error("First, please set environment variable ENTSOE_API_KEY and try again.")
        exit(0)

    data_downloader = ENTSOEAPI(year=year,month=month,country_code=Config.COUNTRY_CODE,api_key=Config.ENTSOE_API_KEY)
    

    try:
        data_downloader.fetch_data(data_type=data_type)
        data = data_downloader.data
        logger.info(f"{filename} has been successfully downloaded.")
        return data
    except Exception as e:
        logger.error(f"Error fetching data from ENTSO-E API: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error
