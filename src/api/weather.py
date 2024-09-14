import re
import logging
import requests
import zipfile
import pandas as pd

from io import BytesIO
from typing import Optional, List

from src.setup_logging import SetupLogging
from src.api.parameters import WeatherParameter
from src.api.base_downloader import BaseDownloader

@SetupLogging()
class WeatherDataDownloader(BaseDownloader):
    """
    A class for downloading and loading weather data from the DWD open data platform.

    This class provides methods to download and extract weather data for specific stations and parameters.
    """

    def __init__(self, base_url: str = None):
        """
        Initialize the WeatherDataDownloader with a base URL for downloading weather data.

        :param base_url: The base URL for weather data. Defaults to the DWD open data URL if not provided.
        """
        super().__init__(base_url or "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/")
        self._logger = logging.getLogger(__name__)

    def download_and_load_data(self, station_code: str, weather_param: WeatherParameter) -> pd.DataFrame:
        """
        Download and load weather data for a given station and weather parameter from both 'recent' and 'historical' directories.

        :param station_code: The station code.
        :param weather_param: The weather parameter enum.
        :return: The merged weather data as a pandas DataFrame.
        :raises ValueError: If weather_param is not an instance of WeatherParameter Enum.
        :raises FileNotFoundError: If no matching files are found in either 'recent' or 'historical' directories.
        """
        self._validate_parameters(weather_param)

        directories = ['recent', 'historical']
        dataframes: List[pd.DataFrame] = []
        last_exception: Optional[Exception] = None

        for directory in directories:
            try:
                url = self._construct_url(station_code, weather_param, directory)
                zip_content = self._download_content(url)
                df = self._extract_data(zip_content, station_code, weather_param)
                dataframes.append(df)
                self._logger.info(f"Successfully downloaded and extracted data from '{directory}' directory.")
            except FileNotFoundError as e:
                self._logger.warning(f"Data not found in '{directory}' directory: {e}")
                last_exception = e
            except requests.RequestException as e:
                self._logger.error(f"Request error when accessing '{directory}' directory: {e}")
                last_exception = e
            except Exception as e:
                self._logger.error(f"Unexpected error when processing '{directory}' directory: {e}")
                last_exception = e

        if not dataframes:
            raise FileNotFoundError(
                f"Data for station '{station_code}' and parameter '{weather_param.name}' not found in any directory."
            ) from last_exception

       
        merged_df = pd.concat(dataframes, ignore_index=True)

       
        if 'MESS_DATUM' in merged_df.columns:  
                merged_df.drop_duplicates(subset=['MESS_DATUM'], inplace=True)  
                merged_df.sort_values(by='MESS_DATUM', inplace=True)  
                merged_df.reset_index(drop=True, inplace=True)  

        self._logger.info(f"Successfully merged data from {len(dataframes)} directories.")
        return merged_df

    def _validate_parameters(self, weather_param: WeatherParameter) -> None:
        """
        Validate that the provided weather parameter is an instance of WeatherParameter Enum.

        :param weather_param: The weather parameter to validate.
        :raises ValueError: If weather_param is not an instance of WeatherParameter Enum.
        """
        if not isinstance(weather_param, WeatherParameter):
            raise ValueError("weather_param must be an instance of WeatherParameter Enum.")

    def _construct_url(self, station_code: str, weather_param: WeatherParameter, directory: Optional[str] = None) -> str:
        """
        Construct the URL for downloading the weather data based on the station code, weather parameter, and directory.

        :param station_code: The station code.
        :param weather_param: The weather parameter enum.
        :param directory: The directory to use ('recent' or 'historical'). If None, no directory is appended.
        :return: The constructed URL.
        """
        category = weather_param.category
        suffix = weather_param.url_suffix

        if directory:
            url = f"{self.base_url}{category}/{directory}/stundenwerte_{weather_param.name}_{station_code}{suffix}.zip"
        else:
            url = f"{self.base_url}{category}/stundenwerte_{weather_param.name}_{station_code}{suffix}.zip"

        self._logger.debug(f"Constructed URL for '{directory}' directory: {url}")
        return url

    def _download_content(self, url: str) -> bytes:
        """
        Download the content of the ZIP file from the provided URL.

        :param url: The URL to download the ZIP file from.
        :return: The content of the downloaded ZIP file.
        :raises requests.RequestException: If the request to download the ZIP file fails.
        :raises FileNotFoundError: If the server returns a 404 status code.
        """
        self._logger.info(f"Attempting to download data from URL: {url}")
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 404:
                raise FileNotFoundError(f"URL not found: {url}")
            response.raise_for_status()
            self._logger.info(f"Successfully downloaded data from URL: {url}")
            return response.content
        except requests.RequestException as e:
            self._logger.error(f"Failed to download data from {url}: {e}")
            raise

    def _extract_data(self, zip_content: bytes, station_code: str, weather_param: WeatherParameter) -> pd.DataFrame:
        """
        Extract data from the ZIP file content and load it into a pandas DataFrame.

        :param zip_content: The content of the ZIP file.
        :param station_code: The station code.
        :param weather_param: The weather parameter enum.
        :return: The extracted weather data as a pandas DataFrame.
        :raises FileNotFoundError: If no matching file is found in the ZIP archive.
        """
        self._logger.debug("Extracting data from ZIP archive.")
        with zipfile.ZipFile(BytesIO(zip_content)) as thezip:
            pattern = re.compile(f"produkt_{weather_param.name.lower()}_stunde_.*_{station_code}.txt")
            matching_files = [s for s in thezip.namelist() if pattern.match(s)]

            if not matching_files:
                raise FileNotFoundError(f"No file matching pattern '{pattern.pattern}' found in the ZIP archive.")

            file_name = matching_files[0]  
            self._logger.info(f"Found file in ZIP archive: {file_name}")

            with thezip.open(file_name) as file:
                df = pd.read_csv(file, delimiter=';', encoding='latin1')

        self._logger.debug(f"Successfully extracted data from file: {file_name}")
        return df
