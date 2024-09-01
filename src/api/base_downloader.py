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

        :param base_url: The base URL for data downloads.
        """
        self.base_url = base_url

    @abc.abstractmethod
    def download_and_load_data(self, *args, **kwargs) -> pd.DataFrame:
        """
        Downloads and loads data.

        :param args: Arguments specific to the data download.
        :param kwargs: Keyword arguments specific to the data download.
        :return: The downloaded data as a pandas DataFrame.
        """
        pass

    @abc.abstractmethod
    def _validate_parameters(self, *args, **kwargs) -> None:
        """
        Validates the parameters for the data download.

        :param args: Arguments specific to the validation.
        :param kwargs: Keyword arguments specific to the validation.
        :raises ValueError: If parameters are not valid.
        """
        pass

    @abc.abstractmethod
    def _construct_url(self, *args, **kwargs) -> str:
        """
        Constructs the URL for downloading the data.

        :param args: Arguments specific to the URL construction.
        :param kwargs: Keyword arguments specific to the URL construction.
        :return: The constructed URL.
        """
        pass

    @abc.abstractmethod
    def _download_content(self, url: str) -> bytes:
        """
        Downloads the content from the provided URL.

        :param url: The URL to download the content from.
        :return: The content of the download as bytes.
        :raises Exception: If the download fails.
        """
        pass

    @abc.abstractmethod
    def _extract_data(self, content: bytes, *args, **kwargs) -> pd.DataFrame:
        """
        Extracts data from the downloaded content.

        :param content: The content to extract data from.
        :param args: Arguments specific to the data extraction.
        :param kwargs: Keyword arguments specific to the data extraction.
        :return: The extracted data as a pandas DataFrame.
        """
        pass
