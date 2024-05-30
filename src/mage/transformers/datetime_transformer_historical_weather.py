if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import pandas as pd

@transformer
def transform(df,**kwargs):
    """
    """
    df.MESS_DATUM = df.MESS_DATUM.astype('str')
    if kwargs['weather_param'] == "ST":
        df['MESS_DATUM'] = pd.to_datetime(df['MESS_DATUM'].str.slice(stop=10), format='%Y%m%d%H')
    else:    
        df.MESS_DATUM = pd.to_datetime(df.MESS_DATUM, format='%Y%m%d%H')
    df = df.rename(columns = {"MESS_DATUM":"measurment_time"})
    df = df.drop(columns="STATIONS_ID")

    return df