import yfinance as yf
import pandas as pd
import pytz

from src.api.base import BaseAPI
from src.config import Config
from src.setup_logging import SetupLogging

@SetupLogging(log_dir=Config.LOG_DIR,config_dir=Config.CONFIG_DIR)
class YahooAPI(BaseAPI):
    """
    API interface for Yahoo Finance data.

    This class provides methods to fetch, transform, and save stock data from Yahoo Finance.
    """

    def __init__(self, symbol: str):
        """
        Initialize the YahooAPI class.

        :param symbol: The stock symbol to fetch data for.
        """
        super().__init__()
        self.symbol = symbol
        self.stock = yf.Ticker(symbol)
        self._data = None

    def fetch_data(self, start=None, end=None, interval='1d', period=None) -> None:
        """
        Get historical market data for the specified ticker.

        :param start: The start date of the historical data (YYYY-MM-DD).
        :param end: The end date of the historical data (YYYY-MM-DD).
        :param interval: Data interval (e.g., '1d', '1wk', '1mo').
        :param period: Data period (e.g., '5d', '1mo'). If provided, 'start' and 'end' will be ignored.
        """
        if period:
            self._data = self.stock.history(period=period, interval=interval)
        else:
            self._data = self.stock.history(start=start, end=end, interval=interval)

    def transform_data(self) -> None:
        """
        Transform the fetched data into the required format.

        This includes renaming columns and converting the timezone.
        """
        self._data.reset_index(drop=False, inplace=True)
        self._data = self._data.rename(columns={'Datetime': 'date', 'Close': 'close_price', 'Open': 'open_price'})
        berlin_tz = pytz.timezone('Europe/Berlin')
        self._data['date'] = self._data['date'].dt.tz_convert(berlin_tz)

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

    def get_summary(self):
        """
        Get the summary data of the stock.

        :return: A dictionary containing the summary data.
        """
        return self.stock.info

    def get_dividends(self) -> pd.DataFrame:
        """
        Get the dividends data of the stock.

        :return: A pandas DataFrame containing the dividends data.
        """
        return self.stock.dividends

    def get_splits(self) -> pd.DataFrame:
        """
        Get the stock splits data of the stock.

        :return: A pandas DataFrame containing the stock splits data.
        """
        return self.stock.splits

    def get_actions(self) -> pd.DataFrame:
        """
        Get the actions (dividends and splits) data of the stock.

        :return: A pandas DataFrame containing the actions data.
        """
        return self.stock.actions
