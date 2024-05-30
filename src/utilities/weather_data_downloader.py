from enum import Enum
import pandas as pd
import requests
import zipfile
from io import BytesIO
from enum import Enum
import re  

class WeatherParameter(Enum):
    TU = ("air_temperature", "Temperature in °C", ['QN_9', 'eor', 'RF_TU'], '_akt')
    SD = ("sun", "Sunshine Duration", ['QN_7', 'eor'], '_akt')
    FF = ("wind", "Wind Speed in km/h", ['QN_3', 'eor'], '_akt')
    ST = ("solar", "Solar Radiation", ['QN_592', 'eor','MESS_DATUM_WOZ','ATMO_LBERG'], '_row')  
    F = ("wind_synop", "Synoptic Wind Data", ['QN_8', 'eor'], '_akt')
    VN = ("total_cloud_cover", "Total Cloud Cover", ['QN_8', 'V_N_I'], '_akt')
    R1 = ("precipitation", "Precipitation in mm", ['QN_8', 'WRTR', 'RS_IND'], '_akt')

    
    def __init__(self, category, description, columns_rm, url_suffix):
        self.category = category
        self.description = description
        self.columns_rm = columns_rm
        self.url_suffix = url_suffix  

    def __str__(self):
        return f"{self.name} ({self.description}) - Columns: {', '.join(self.columns)}"


class WeatherDataDownloader:
    def __init__(self, base_url=None):
        self.base_url = base_url or "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/"

    def download_and_load_weather_data(self, station_code, weather_param):
        if not isinstance(weather_param, WeatherParameter):
            raise ValueError("weather_param must be an instance of WeatherParameter Enum.")
        
        category = weather_param.category
        suffix = weather_param.url_suffix
        if weather_param == WeatherParameter.ST:
            url = f"{self.base_url}{category}/stundenwerte_{weather_param.name}_{station_code}{suffix}.zip"
        else:
            url = f"{self.base_url}{category}/recent/stundenwerte_{weather_param.name}_{station_code}{suffix}.zip"

        response = requests.get(url)
        response.raise_for_status()
        with zipfile.ZipFile(BytesIO(response.content)) as thezip:
            pattern = re.compile(f"produkt_{weather_param.name.lower()}_stunde_.*_{station_code}.txt")
            file_name = next((s for s in thezip.namelist() if pattern.match(s)), None)
            if not file_name:
                raise FileNotFoundError(f"No file matching '{pattern.pattern}' found in the ZIP archive.")
            with thezip.open(file_name) as file:
                df = pd.read_csv(file, delimiter=';', encoding='latin1')
        return df