if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, **kwargs):
    
 
    df.loc[:,'day'] = df.forecast_time.dt.day
    df.loc[:,'month'] = df.forecast_time.dt.month
    df.loc[:,'year'] = df.forecast_time.dt.year
    df.loc[:,'hour'] = df.forecast_time.dt.hour
    

    return df