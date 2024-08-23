import pandas as pd

from entsoe import EntsoePandasClient
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config import Config
from src.api.base import BaseAPI

class ENTSOEAPI(BaseAPI):
    """
    API interface for ENTSO-E Transparency Platform data.

    This class provides methods to fetch, transform, and save energy data from the ENTSO-E Transparency Platform.
    """

    def __init__(self, year: int, month: int, country_code: str, api_key: str):
        """
        Initialize the ENTSOEAPI class.

        Args:
            year (int): The year of the data.
            month (int): The month of the data.
            country_code (str): The country code for the data.
            api_key (str): The API key for the ENTSO-E API.
        """
        super().__init__()
        self._data = None
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

    def transform_data(self,data_type: str) -> None:
        """
        Transform the fetched data if necessary.

        Args:
            data (pd.DataFrame): The data to transform.

        Returns:
            pd.DataFrame: The transformed data.
        """
        self._data.reset_index(drop=False, inplace=True)
        if data_type == 'load': 
            self._data.rename(columns={'index':'date','Actual Load': 'load'}, inplace=True)
            self._data.dropna(inplace=True)
        if data_type == 'generation':
            self._data.rename(columns={'index':'date'}, inplace=True)
            self._data.columns = ['_'.join(filter(None, col)).lower().replace(' ', '_').replace('/', '_') for col in self._data.columns]   
        self._data.sort_values(by='date', inplace=True)  
        
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def fetch_data(self, data_type: str) -> None:
        """
        Download data from the ENTSO-E API based on the specified data type.

        Args:
            data_type (str): The type of data to download (e.g., 'load', 'generation').

        Returns:
            str: The filename of the downloaded data.
        """
        start = pd.Timestamp(f'{self.year}-{self.month}-01', tz='Europe/Brussels')
        end = self.get_end_date(self.year, self.month)
        try:
            if data_type == 'load':
                self._data = self.client.query_load(self.country_code, start=start, end=end)
                self.transform_data(data_type=data_type)
            elif data_type == 'generation':
                self._data = self.client.query_generation(self.country_code, start=start, end=end)
                self.transform_data(data_type=data_type)
            else:
                raise ValueError(f"Unsupported data type: {data_type}")

            if self._data.empty:
                raise ValueError("Invalid data received from the API")
            
        except Exception as e:
            raise Exception(f"Error fetching {data_type} data from ENTSO-E API: {e}")

    def save_data(self, filename: str) -> None:
        """
        Save the transformed data to a CSV file.

        :param filename: The name of the file to save the data.
        """
        self._data.to_csv(filename, index=False)

    @property
    def data(self) -> pd.DataFrame:
        """
        Get the fetched and transformed data.

        :return: A pandas DataFrame containing the data.
        """
        return self._data

    @data.setter
    def data(self, value: pd.DataFrame) -> None:
        """
        Set the data attribute.

        :param value: A pandas DataFrame to set as the data.
        """
        self._data = value
