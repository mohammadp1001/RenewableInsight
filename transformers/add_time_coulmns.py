if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


import pandas as pd


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    df['hour'] = df.datetime.dt.hour.astype('int8')
    df['day'] = df.datetime.dt.day.astype('int8')
    df['month'] = df.datetime.dt.month.astype('int8')
    df['year'] = df.datetime.dt.year.astype('int16')
    

    return df
