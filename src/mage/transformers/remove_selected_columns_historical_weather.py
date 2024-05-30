if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
from RenewableInsight.src.WeatherDataDownloader import WeatherParameter


@transformer
def transform(df,**kwargs):
    """
    """
    weather_param = WeatherParameter[kwargs['weather_param']] 

    # Remove the unnecessary columns specified in the WeatherParameter enum
    df = df.drop(columns=weather_param.columns_rm, errors='ignore')
    
    
    return df