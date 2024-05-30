"""
Data Loader Module

This module provides a function to load data from an SFTP server.
"""

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

from RenewableInsight.utilities.ssh import SSH
import os 
import logging
from urllib.parse import urlparse

import pandas as pd


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@data_loader
def load_data(**kwargs):
    """
    Load data from an SFTP server.

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
    # Using snake_case for variable names
    month = kwargs['month']
    year = kwargs['year']
    data_item_name = kwargs['data_item_name']
    data_item_no = kwargs['data_item_no']
    sftp_url = os.environ.get('SFTPTOGO_URL')
    rsa_key = os.environ.get('RSA_KEY')
    base_directory = kwargs['base_directory']
    query_filename = f"/TP_export/{data_item_name}_{data_item_no}/{year}_{month}_{data_item_name}_{data_item_no}.csv"

    if not sftp_url:
        logging.error("First, please set environment variable SFTPTOGO_URL and try again.")
        exit(0)

    parsed_url = urlparse(sftp_url)
    
    ssh = SSH(
        hostname=parsed_url.hostname,
        username=parsed_url.username,
        password=parsed_url.password,
        hostkey=rsa_key
    )
    
    ssh.connect()
    logging.info("ssh connection has been made.")
    ssh.open_sftp()
    ssh.download_file(query_filename, base_directory)
    ssh.disconnect()
    logging.info(f"{year}_{month}_{data_item_name}_{data_item_no}.csv has been successfully downloaded.")
    df = pd.read_csv(base_directory + f"{year}_{month}_{data_item_name}_{data_item_no}.csv", delimiter="\t")
    return df
