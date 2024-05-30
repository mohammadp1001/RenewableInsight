if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, **kwargs):
    """
    Convert all column names in a pandas DataFrame to lowercase.

    Parameters:
    df (pd.DataFrame): The DataFrame whose columns need to be lowercased.

    Returns:
    pd.DataFrame: The DataFrame with lowercased column names.
    
    """
    df.columns = [col.lower() for col in df.columns]
    return df