import abc
import pandas as pd

class BaseAPI(abc.ABC):
    """
    Base class for API interfaces.

    This class provides a standard interface for API calls, ensuring consistency across different API implementations.
    """

    def __init__(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def fetch_data(self, *args, **kwargs) -> pd.DataFrame:
        """
        Fetch data from the API.

        :param args: Arguments specific to the API call.
        :param kwargs: Keyword arguments specific to the API call.
        :return: A pandas DataFrame containing the fetched data.
        """
        pass

    @abc.abstractmethod
    def transform_data(self) -> None:
        """
        Transform the fetched data into the required format.

        This method should implement any data transformation logic necessary.
        """
        pass

    @abc.abstractmethod
    def save_data(self, filename: str) -> None:
        """
        Save the transformed data to a file.

        :param filename: The name of the file to save the data.
        """
        pass

    @property
    @abc.abstractmethod
    def data(self) -> pd.DataFrame:
        """
        Get the fetched and transformed data.

        :return: A pandas DataFrame containing the data.
        """
        pass

    @data.setter
    @abc.abstractmethod
    def data(self, value: pd.DataFrame) -> None:
        """
        Set the data attribute.

        :param value: A pandas DataFrame to set as the data.
        """
        pass
