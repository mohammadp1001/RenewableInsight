
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import pandas as pd

@transformer
def transform(df, **kwargs):
    """
    Converts all integer columns in the DataFrame to int16 to save memory space.

    Args:
    df (pd.DataFrame): The DataFrame whose integer columns are to be converted.

    Returns:
    pd.DataFrame: A DataFrame with integer columns converted to int16.
    """
    # Iterate over each column in the DataFrame
    for col in df.columns:
        # If the column is of integer type
        if pd.api.types.is_integer_dtype(df[col]):
            # Convert the column to int16
            df[col] = df[col].astype('int16')
        if pd.api.types.is_float_dtype(df[col]):
            # Convert the column to int16
            df[col] = df[col].astype('float32')    
    return df
