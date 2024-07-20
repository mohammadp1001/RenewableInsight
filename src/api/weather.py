from enum import Enum
import pandas as pd
import requests
import zipfile
from io import BytesIO
import re
import logging
from parameters import WeatherParameter
from config import Config
from setup_logging import SetupLogging

@SetupLogging(log_dir=Config.LOG_DIR)
class WeatherDataDownloader:
    def __init__(self, base_url: str = None):
        """
        Initializes the WeatherDataDownloader with a base URL for downloading weather data.
        
        Args:
            base_url (str, optional): The base URL for weather data. Defaults to the DWD open data URL.
        """
        self.base_url = base_url or "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/"
        
    def download_and_load_weather_data(self, station_code: str, weather_param: WeatherParameter) -> pd.DataFrame:
        """
        Downloads and loads weather data for a given station and weather parameter.
        
        Args:
            station_code (str): The station code.
            weather_param (WeatherParameter): The weather parameter enum.

        Returns:
            pd.DataFrame: The weather data as a pandas DataFrame.
        
        Raises:
            ValueError: If weather_param is not an instance of WeatherParameter Enum.
            FileNotFoundError: If no matching file is found in the ZIP archive.
        """
        self._validate_weather_param(weather_param)
        
        url = self._construct_url(station_code, weather_param)
        zip_content = self._download_zip_content(url)
        df = self._extract_data_from_zip(zip_content, station_code, weather_param)
        
        return df
    
    def _validate_weather_param(self, weather_param: WeatherParameter):
        """
        Validates that the provided weather parameter is an instance of WeatherParameter Enum.
        
        Args:
            weather_param (WeatherParameter): The weather parameter to validate.
        
        Raises:
            ValueError: If weather_param is not an instance of WeatherParameter Enum.
        """
        if not isinstance(weather_param, WeatherParameter):
            raise ValueError("weather_param must be an instance of WeatherParameter Enum.")
    
    def _construct_url(self, station_code: str, weather_param: WeatherParameter) -> str:
        """
        Constructs the URL for downloading the weather data based on the station code and weather parameter.
        
        Args:
            station_code (str): The station code.
            weather_param (WeatherParameter): The weather parameter enum.

        Returns:
            str: The constructed URL.
        """
        category = weather_param.category
        suffix = weather_param.url_suffix
        
        if weather_param == WeatherParameter.ST:
            url = f"{self.base_url}{category}/stundenwerte_{weather_param.name}_{station_code}{suffix}.zip"
        else:
            url = f"{self.base_url}{category}/recent/stundenwerte_{weather_param.name}_{station_code}{suffix}.zip"
        
        logger.info(f"Constructed URL: {url}")
        return url
    
    def _download_zip_content(self, url: str) -> bytes:
        """
        Downloads the content of the ZIP file from the provided URL.
        
        Args:
            url (str): The URL to download the ZIP file from.

        Returns:
            bytes: The content of the downloaded ZIP file.
        
        Raises:
            requests.RequestException: If the request to download the ZIP file fails.
        """
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.content
        except requests.RequestException as e:
            logger.error(f"Failed to download data: {e}")
            raise
    
    def _extract_data_from_zip(self, zip_content: bytes, station_code: str, weather_param: WeatherParameter) -> pd.DataFrame:
        """
        Extracts data from the ZIP file content and loads it into a pandas DataFrame.
        
        Args:
            zip_content (bytes): The content of the ZIP file.
            station_code (str): The station code.
            weather_param (WeatherParameter): The weather parameter enum.

        Returns:
            pd.DataFrame: The extracted weather data as a pandas DataFrame.
        
        Raises:
            FileNotFoundError: If no matching file is found in the ZIP archive.
        """
        with zipfile.ZipFile(BytesIO(zip_content)) as thezip:
            pattern = re.compile(f"produkt_{weather_param.name.lower()}_stunde_.*_{station_code}.txt")
            file_name = next((s for s in thezip.namelist() if pattern.match(s)), None)
            if not file_name:
                raise FileNotFoundError(f"No file matching '{pattern.pattern}' found in the ZIP archive.")
            with thezip.open(file_name) as file:
                df = pd.read_csv(file, delimiter=';', encoding='latin1')
        
        logger.info(f"Extracted file: {file_name}")
        return df
