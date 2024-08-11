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
    
    def __new__(cls, category: str, description: str, columns_rm: list, url_suffix: str):
        """
        Creates a new instance of the WeatherParameter enum.

        Args:
            category (str): The category of the weather parameter.
            description (str): A brief description of the weather parameter.
            columns_rm (list): List of columns to be removed from the dataset.
            url_suffix (str): The URL suffix for the weather parameter data.
        """
        obj = object.__new__(cls)
        obj._value_ = category
        obj.category = category
        obj.description = description
        obj.columns_rm = columns_rm
        obj.url_suffix = url_suffix
        return obj

    def __str__(self) -> str:
        """
        Returns a string representation of the WeatherParameter instance.

        Returns:
            str: A string describing the weather parameter, its description, and the columns to be removed.
        """
        return f"{self.name} ({self.description}) - Columns: {', '.join(self.columns_rm)}"

    @classmethod
    def from_name(cls, name: str):
        """
        Allows instantiation of a WeatherParameter instance by its name.

        Args:
            name (str): The name of the weather parameter.

        Returns:
            WeatherParameter: The corresponding WeatherParameter instance.
        """
        try:
            return cls[name]
        except KeyError:
            raise ValueError(f"{name} is not a valid WeatherParameter name")