if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
from RenewableInsight.src.WeatherDataDownloader import WeatherDataDownloader,WeatherParameter


@data_loader
def load_data(**kwargs):
    """
    Load weather data for a given station and weather parameter.
    
    :param station_code: The code of the weather station.
    :param weather_param: The WeatherParameter enum member specifying the type of weather data.
    :return: A pandas DataFrame with the loaded and cleaned weather data.
    """
    station_code = kwargs['station_code']
    weather_param = WeatherParameter[kwargs['weather_param']] 
    # Create an instance of the WeatherDataDownloader
    downloader = WeatherDataDownloader()

    # Download and load the data using the downloader
    df = downloader.download_and_load_weather_data(station_code,weather_param)

    
    return df
