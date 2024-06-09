if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
import pyspark.sql.functions as F
from RenewableInsight.utilities.streaming import create_spark_session,fetch_new_data,stop_existing_query,read_from_kafka
from RenewableInsight.utilities.settings import RECORDS_SCHEMA
import os


@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    spark = create_spark_session()

    df_stream = read_from_kafka(spark,kwargs['CONSUME_TOPIC_ACTUALLOAD_CSV'])

    stop_existing_query(spark,kwargs['query'])
    
    pandas_df = fetch_new_data(df_stream,spark,kwargs['query'],RECORDS_SCHEMA)
    
    spark.stop()
    
    return pandas_df 