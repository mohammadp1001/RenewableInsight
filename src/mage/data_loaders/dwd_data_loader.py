if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
from pathlib import Path
from RenewableInsight.src.DwdMosmixParser import DwdMosmixParser,kml_reader
from RenewableInsight.src.utils import download_kmz_file

@data_loader
def load_data(*args, **kwargs):
    """
    Extracts forecast data for a given station from a KMZ file and returns it as a pandas DataFrame.

    Parameters:
    - station_name: The name of the station to extract the forecast data for.
    - kmz_file: Path to the KMZ file containing the forecast data.

    Returns:
    - A pandas DataFrame containing the forecast data for the specified station, or None if the station is not found.
    """
                
    url = "https://opendata.dwd.de/weather/local_forecasts/mos/MOSMIX_S/all_stations/kml/MOSMIX_S_LATEST_240.kmz"  # Replace with the actual URL of the KMZ file
   
    save_dir = "/tmp/"
    filename = kwargs['filename_forecast']
    station_name = kwargs['station_name'] 
    
    kmz_file_path = download_kmz_file(url, save_dir, filename)

    parser = DwdMosmixParser()

    # kmz_file_path = "/home/mohammadp/RenewableInsight/src/MOSMIX_S_LATEST_240.kmz"
    # kmz_file_path = Path(kmz_file_path)

    with kml_reader(kmz_file_path) as fp:
        timestamps = list(parser.parse_timestamps(fp))
        fp.seek(0)  # Reset the file pointer to reuse it for reading forecasts
        forecasts = list(parser.parse_forecasts(fp, {station_name}))

    if not forecasts:
        logging.warning(f"No data found for station: {station_name}")
        
    df = parser.convert_to_dataframe(forecasts, station_name)
    df["forecast_time"] = timestamps
    
    return df


