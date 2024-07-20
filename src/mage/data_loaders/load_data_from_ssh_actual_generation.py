"""
Data Loader Module

This module provides a function to load data from the ENTSO-E API.
"""

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

from entsoe import EntsoePandasClient
from RenewableInsight.utilities.setup_logging import setup_logging

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
            data_item_name (str): The name of the data item.
            data_item_no (int): The number of the data item.
            base_directory (str): The base directory to save the downloaded file.

    Returns:
        DataFrame: The loaded DataFrame.
    """
    setup_logging(kwargs['LOG_DIR'])
    logger = logging.getLogger(__name__)
    
    # Using snake_case for variable names
    month = kwargs['month']
    year = kwargs['year']
    base_directory = kwargs['base_directory']
    api_key = os.environ.get('ENTSOE_API_KEY')

    if not api_key:
        logger.error("First, please set environment variable ENTSOE_API_KEY and try again.")
        exit(0)

    client = EntsoePandasClient(api_key=api_key)
    
    start = pd.Timestamp(f'{year}-{month}-01', tz='Europe/Brussels')
    end = pd.Timestamp(f'{year}-{month}-28', tz='Europe/Brussels')  #TODO # Adjust for the actual month length

    # TODO 
    # Fetch the data for Germany (DE)
    country_code = 'DE'

    try:
        ts = client.query_load(country_code, start=start, end=end)
        filename = os.path.join(base_directory, f"{year}_{month}_ActualTotalLoad.csv")
        ts.to_csv(filename)
        logger.info(f"{filename} has been successfully downloaded.")
        df = pd.read_csv(filename)
        return df
    except Exception as e:
        logger.error(f"Error fetching data from ENTSO-E API: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error
