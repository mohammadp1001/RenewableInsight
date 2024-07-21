if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd
@transformer
def transform(df, *args, **kwargs):
    """
    """
    # Iterate over each column in the DataFrame
    for col in df.columns:
        # If the column is of integer type
        if pd.api.types.is_integer_dtype(df[col]):
            # Convert the column to int16
            df[col] = df[col].astype('int16')
        # If the column is of float type    
        if pd.api.types.is_float_dtype(df[col]):
            # Convert the column to float32
            df[col] = df[col].astype('float32')    
    return df

    return data
