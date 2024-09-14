import re
import logging
import requests
import zipfile
import pandas as pd

from io import BytesIO


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
        self.logger = logging.getLogger(__name__)


    def download_and_load_data(self, station_code: str, weather_param: WeatherParameter) -> pd.DataFrame:
        """
        Download and load weather data for a given station and weather parameter.

        :param station_code: The station code.
        :param weather_param: The weather parameter enum.
        :return: The weather data as a pandas DataFrame.
        :raises ValueError: If weather_param is not an instance of WeatherParameter Enum.
        :raises FileNotFoundError: If no matching file is found in the ZIP archive.
        """
        self._validate_parameters(weather_param)
        url = self._construct_url(station_code, weather_param)
        zip_content = self._download_content(url)
        df = self._extract_data(zip_content, station_code, weather_param)
        return df

    def _validate_parameters(self, weather_param: WeatherParameter) -> None:
        """
        Validate that the provided weather parameter is an instance of WeatherParameter Enum.

        :param weather_param: The weather parameter to validate.
        :raises ValueError: If weather_param is not an instance of WeatherParameter Enum.
        """
        if not isinstance(weather_param, WeatherParameter):
            raise ValueError("weather_param must be an instance of WeatherParameter Enum.")

    def _construct_url(self, station_code: str, weather_param: WeatherParameter) -> str:
        """
        Construct the URL for downloading the weather data based on the station code and weather parameter.

        :param station_code: The station code.
        :param weather_param: The weather parameter enum.
        :return: The constructed URL.
        """
        self.logger = logging.getLogger(__name__)
        category = weather_param.category
        suffix = weather_param.url_suffix
        
        if weather_param == WeatherParameter.ST:
            url = f"{self.base_url}{category}/stundenwerte_{weather_param.name}_{station_code}{suffix}.zip"
        else:
            url = f"{self.base_url}{category}/recent/stundenwerte_{weather_param.name}_{station_code}{suffix}.zip"
        
        self.logger.info(f"Constructed URL: {url}")
        return url

    def _download_content(self, url: str) -> bytes:
        """
        Download the content of the ZIP file from the provided URL.

        :param url: The URL to download the ZIP file from.
        :return: The content of the downloaded ZIP file.
        :raises requests.RequestException: If the request to download the ZIP file fails.
        """
        self.logger = logging.getLogger(__name__)
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.content
        except requests.RequestException as e:
            self.logger.error(f"Failed to download data: {e}")
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
        self.logger = logging.getLogger(__name__)
        with zipfile.ZipFile(BytesIO(zip_content)) as thezip:
            pattern = re.compile(f"produkt_{weather_param.name.lower()}_stunde_.*_{station_code}.txt")
            file_name = next((s for s in thezip.namelist() if pattern.match(s)), None)
            if not file_name:
                raise FileNotFoundError(f"No file matching '{pattern.pattern}' found in the ZIP archive.")
            with thezip.open(file_name) as file:
                df = pd.read_csv(file, delimiter=';', encoding='latin1')
        
        self.logger.info(f"Extracted file: {file_name}")
        return df
