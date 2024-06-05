import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import os
import pandas as pd 
from pyspark.sql.functions import from_json, col
def read_from_kafka(consume_topic: str):

    # Spark Streaming DataFrame, connect to Kafka topic served at host in bootrap.servers option
    os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.apache.spark:spark-avro_2.12:3.3.1 pyspark-shell'
    
    spark = SparkSession \
    .builder \
    .appName("Spark-Streaming") \
    .master("spark://ac7384505186:7077") \
    .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    df_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092,broker:29092") \
        .option("subscribe", consume_topic) \
        .option("startingOffsets", "earliest") \
        .option("checkpointLocation", "checkpoint") \
        .load()

    return df_stream,spark

# Function to stop a streaming query by name
def stop_existing_query(spark,query_name):
    for query in spark.streams.active:
        if query.name == query_name:
            print(f"Stopping existing query: {query_name}")
            query.stop()

# Function to fetch and parse new data from the in-memory table
def fetch_new_data(df_stream,spark,schema):
    

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
        .select(from_json(col("json_string"), schema).alias("data")) \
        .select("data.*")
    
    # Convert to Pandas DataFrame
    parsed_pandas_df = parsed_df.toPandas()
    

    return parsed_pandas_df