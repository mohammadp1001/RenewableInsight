if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
import pyspark.sql.functions as F
from RenewableInsight.utilities.streaming import read_from_kafka,fetch_new_data,stop_existing_query
from pyspark.sql import SparkSession
from RenewableInsight.utilities.settings import RECORDS_SCHEMA
from pyspark.sql.functions import from_json, col
import os
CONSUME_TOPIC_ACTUALLOAD_CSV  = 'actualload'

@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Spark Streaming DataFrame, connect to Kafka topic served at host in bootrap.servers option
    os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.apache.spark:spark-avro_2.12:3.3.1 pyspark-shell'
    
    spark = SparkSession \
        .builder \
        .appName("Spark-Notebook") \
        .master("spark://ac7384505186:7077") \
        .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    df_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092,broker:29092") \
        .option("subscribe", 'actualload') \
        .option("startingOffsets", "earliest") \
        .option("checkpointLocation", "checkpoint") \
        .load()
    
    

    stop_existing_query(spark,"qraw")

    query = df_stream \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as json_string") \
    .writeStream \
    .queryName("qraw") \
    .format("memory") \
    .start()

    # Execute Spark SQL to get the data from the in-memory table
    raw_spark_df = spark.sql("select * from qraw")
    
    # Parse the JSON string in the 'value' column
    parsed_df = raw_spark_df \
        .select(from_json(col("json_string"), RECORDS_SCHEMA).alias("data")) \
        .select("data.*")
    
    # Convert to Pandas DataFrame
    parsed_pandas_df = parsed_df.toPandas()

    # parsed_pandas_df = fetch_new_data(df_consume_stream,spark,RECORDS_SCHEMA)
    print(parsed_pandas_df.shape)

    spark.stop()
    
    return parsed_pandas_df