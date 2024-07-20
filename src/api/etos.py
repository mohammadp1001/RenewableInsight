import os
import logging
import pandas as pd
from entsoe import EntsoePandasClient
from tenacity import retry, stop_after_attempt, wait_exponential
from config import Config
from setup_logging import SetupLogging

@SetupLogging(log_dir=Config.LOG_DIR)
class EntsoeDataDownloader:
    """
    Class to handle downloading data from the ENTSO-E API.
    """
    
    def __init__(self, year: int, month: int, country_code: str, api_key: str = Config.ENTSOE_API_KEY):
        """
        Initialize the EntsoeDataDownloader instance.

        Args:
            year (int): The year of the data.
            month (int): The month of the data.
            country_code (str): The country code for the data.
            api_key (str): The API key for the ENTSO-E API.
        """
        self.year = year
        self.month = month
        self.country_code = country_code
        self.api_key = api_key
        self.client = EntsoePandasClient(api_key=self.api_key)

    @staticmethod
    def get_end_date(year: int, month: int) -> pd.Timestamp:
        """
        Get the end date of the month.

        Args:
            year (int): The year.
            month (int): The month.

        Returns:
            pd.Timestamp: The end date of the month.
        """
        return pd.Timestamp(f'{year}-{month+1}-01', tz='Europe/Brussels') - pd.Timedelta(days=1)

    def validate_data(self, data: pd.DataFrame) -> bool:
        """
        Validate the fetched data.

        Args:
            data (pd.DataFrame): The data to validate.

        Returns:
            bool: True if data is valid, False otherwise.
        """
        return not data.empty

    def transform_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Transform the fetched data if necessary.

        Args:
            data (pd.DataFrame): The data to transform.

        Returns:
            pd.DataFrame: The transformed data.
        """
        # Add your transformation logic here
        return data

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def download_data(self, data_type: str, resource_path: str = Config.RESOURCE_PATH) -> str:
        """
        Download data from the ENTSO-E API based on the specified data type.

        Args:
            data_type (str): The type of data to download (e.g., 'load', 'generation').
            resource_path (str): The path to save the downloaded data.

        Returns:
            str: The filename of the downloaded data.
        """
        start = pd.Timestamp(f'{self.year}-{self.month}-01', tz='Europe/Brussels')
        end = self.get_end_date(self.year, self.month)

        try:
            if data_type == 'load':
                ts = self.client.query_load(self.country_code, start=start, end=end)
            elif data_type == 'generation':
                ts = self.client.query_generation(self.country_code, start=start, end=end)
            # Add more data types as needed
            else:
                raise ValueError(f"Unsupported data type: {data_type}")

            if not self.validate_data(ts):
                raise ValueError("Invalid data received from the API")
            ts = self.transform_data(ts)
            filename = os.path.join(resource_path, f"{self.year}_{self.month}_{data_type}.csv")
            ts.to_csv(filename)
            return filename

        except Exception as e:
            logging.error(f"Error fetching data from ENTSO-E API: {e}")
            return None