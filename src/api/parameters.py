from enum import Enum

class WeatherParameter(Enum):
    """
    Enum representing different weather parameters, each associated with a category,
    description, columns to be removed, and a URL suffix.

    Attributes:
        TU (tuple): Air temperature data.
        SD (tuple): Sunshine duration data.
        FF (tuple): Wind speed data.
        ST (tuple): Solar radiation data.
        F (tuple): Synoptic wind data.
        VN (tuple): Total cloud cover data.
        R1 (tuple): Precipitation data.
    """
    TU = ("air_temperature", "Temperature in °C", ['QN_9', 'eor', 'RF_TU'], '_akt')
    SD = ("sun", "Sunshine Duration", ['QN_7', 'eor'], '_akt')
    FF = ("wind", "Wind Speed in km/h", ['QN_3', 'eor'], '_akt')
    ST = ("solar", "Solar Radiation", ['QN_592', 'eor','MESS_DATUM_WOZ','ATMO_LBERG'], '_row')  
    F = ("wind_synop", "Synoptic Wind Data", ['QN_8', 'eor'], '_akt')
    VN = ("total_cloud_cover", "Total Cloud Cover", ['QN_8', 'V_N_I'], '_akt')
    R1 = ("precipitation", "Precipitation in mm", ['QN_8', 'WRTR', 'RS_IND'], '_akt')
    
    def __init__(self, category: str, description: str, columns_rm: list, url_suffix: str):
        """
        Initializes a WeatherParameter instance.

        Args:
            category (str): The category of the weather parameter.
            description (str): A brief description of the weather parameter.
            columns_rm (list): List of columns to be removed from the dataset.
            url_suffix (str): The URL suffix for the weather parameter data.
        """
        self.category = category
        self.description = description
        self.columns_rm = columns_rm
        self.url_suffix = url_suffix  

    def __str__(self) -> str:
        """
        Returns a string representation of the WeatherParameter instance.

        Returns:
            str: A string describing the weather parameter, its description, and the columns to be removed.
        """
        return f"{self.name} ({self.description}) - Columns: {', '.join(self.columns_rm)}"
