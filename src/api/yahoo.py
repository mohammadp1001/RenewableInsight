import yfinance as yf
import pandas as pd

class YahooFinanceAPI:
    def __init__(self, ticker):
        """
        Initialize the YahooFinanceAPI with a specific ticker symbol.

        :param ticker: The ticker symbol of the stock.
        """
        self.ticker = ticker
        self.stock = yf.Ticker(ticker)

    def get_historical_data(self, start=None, end=None, interval='1d'):
        """
        Get historical market data for the specified ticker.

        :param start: The start date of the historical data (YYYY-MM-DD).
        :param end: The end date of the historical data (YYYY-MM-DD).
        :param interval: Data interval (e.g., '1d', '1wk', '1mo').
        :return: A pandas DataFrame containing the historical market data.
        """
        historical_data = self.stock.history(start=start, end=end, interval=interval)
        return historical_data

    def get_summary(self):
        """
        Get the summary data of the stock.

        :return: A dictionary containing the summary data.
        """
        return self.stock.info

    def get_dividends(self):
        """
        Get the dividends data of the stock.

        :return: A pandas DataFrame containing the dividends data.
        """
        dividends = self.stock.dividends
        return dividends

    def get_splits(self):
        """
        Get the stock splits data of the stock.

        :return: A pandas DataFrame containing the stock splits data.
        """
        splits = self.stock.splits
        return splits

    def get_actions(self):
        """
        Get the actions (dividends and splits) data of the stock.

        :return: A pandas DataFrame containing the actions data.
        """
        actions = self.stock.actions
        return actions

# Example usage:
if __name__ == "__main__":
    # Create an instance of YahooFinanceAPI for the ticker 'AAPL'
    yf_api = YahooFinanceAPI('AAPL')

    # Get historical market data
    historical_data = yf_api.get_historical_data(start='2022-01-01', end='2023-01-01')
    print(historical_data)

    # Get summary data
    summary = yf_api.get_summary()
    print(summary)

    # Get dividends data
    dividends = yf_api.get_dividends()
    print(dividends)

    # Get splits data
    splits = yf_api.get_splits()
    print(splits)

    # Get actions data
    actions = yf_api.get_actions()
    print(actions)
