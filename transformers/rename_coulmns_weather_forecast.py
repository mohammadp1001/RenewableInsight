if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    
    """
    
    # Specify your transformation logic here
    df = df.rename(columns={"TTT": "temperature_2m", "TX": "temperature_max", "TN": "temperature_min", "DD": "wind_direction",
    "FF": "wind_speed", "Rad1h": "global_irradiance","ww": "significant_weather", "N": "total_cloud_cover","SunD1":"sunshine_dur"})

    return df

