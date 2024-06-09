if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
from utilities.streaming import read_from_kafka
from pyspark.sql import SparkSession
from utilities.settings import RECORDS_SCHEMA


@data_loader
def load_data(**kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your data loading logic here

    return None
