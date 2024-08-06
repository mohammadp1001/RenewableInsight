
def load_data():
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

    kmz_file_path = "/home/mohammadp/RenewableInsight/src/MOSMIX_S_LATEST_240.kmz"
    kmz_file_path = Path(kmz_file_path)

    with kml_reader(kmz_file_path) as fp:
        timestamps = list(parser.parse_timestamps(fp))
        fp.seek(0)  
        forecasts = list(parser.parse_forecasts(fp, {station_name}))

    if not forecasts:
        logging.warning(f"No data found for station: {station_name}")
        
    data = parser.convert_to_dataframe(forecasts, station_name)
    data["forecast_time"] = timestamps
    
    return data



def transform(data):
    """
    """
    columns = kwargs['weather_param'].split(',')
    
    data = data[columns]

    data.loc[:,'day'] = data.forecast_time.dt.day
    data.loc[:,'month'] = data.forecast_time.dt.month
    data.loc[:,'year'] = data.forecast_time.dt.year
    data.loc[:,'hour'] = data.forecast_time.dt.hour

    data = data.rename(columns={"TTT": "temperature_2m", "TX": "temperature_max", "TN": "temperature_min", "DD": "wind_direction",
    "FF": "wind_speed", "Rad1h": "global_irradiance","ww": "significant_weather", "N": "total_cloud_cover","SunD1":"sunshine_dur"})

    # Iterate over each column in the DataFrame
    for col in data.columns:
        # If the column is of integer type
        if pd.api.types.is_integer_dtype(data[col]):
            # Convert the column to int16
            data[col] = data[col].astype('int16')
        if pd.api.types.is_float_dtype(data[col]):
            # Convert the column to int16
            data[col] = data[col].astype('float32')    


    data.temperature_2m = data.temperature_2m - 273.15
    data.temperature_max = data.temperature_max - 273.15
    data.temperature_min = data.temperature_min - 273.15
    

    return data
