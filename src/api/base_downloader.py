import abc
import pandas as pd

class BaseDownloader(abc.ABC):
    """
    Base class for data downloader interfaces.

    This class provides a standard interface for downloading, transforming, and saving data.
    """

    def __init__(self, base_url: str):
        """
        Initializes the BaseDownloader with a base URL.

        Args:
            base_url (str): The base URL for data downloads.
        """
        self.base_url = base_url

    @abc.abstractmethod
    def download_and_load_data(self, *args, **kwargs) -> pd.DataFrame:
        """
        Downloads and loads data.

        Args:
            args: Arguments specific to the data download.
            kwargs: Keyword arguments specific to the data download.

        Returns:
            pd.DataFrame: The downloaded data as a pandas DataFrame.
        """
        pass

    @abc.abstractmethod
    def _validate_parameters(self, *args, **kwargs):
        """
        Validates the parameters for the data download.

        Args:
            args: Arguments specific to the validation.
            kwargs: Keyword arguments specific to the validation.

        Raises:
            ValueError: If parameters are not valid.
        """
        pass

    @abc.abstractmethod
    def _construct_url(self, *args, **kwargs) -> str:
        """
        Constructs the URL for downloading the data.

        Args:
            args: Arguments specific to the URL construction.
            kwargs: Keyword arguments specific to the URL construction.

        Returns:
            str: The constructed URL.
        """
        pass

    @abc.abstractmethod
    def _download_content(self, url: str) -> bytes:
        """
        Downloads the content from the provided URL.

        Args:
            url (str): The URL to download the content from.

        Returns:
            bytes: The content of the download.

        Raises:
            Exception: If the download fails.
        """
        pass

    @abc.abstractmethod
    def _extract_data(self, content: bytes, *args, **kwargs) -> pd.DataFrame:
        """
        Extracts data from the downloaded content.

        Args:
            content (bytes): The content to extract data from.
            args: Arguments specific to the data extraction.
            kwargs: Keyword arguments specific to the data extraction.

        Returns:
            pd.DataFrame: The extracted data as a pandas DataFrame.
        """
        pass
