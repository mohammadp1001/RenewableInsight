import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import os
import time
import pandas as pd 
from pyspark.sql.functions import from_json, col


def create_spark_session():

    # Spark Streaming DataFrame, connect to Kafka topic served at host in bootrap.servers option
    os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.apache.spark:spark-avro_2.12:3.3.1 pyspark-shell'
    
    spark = SparkSession \
    .builder \
    .appName("spark_streaming") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    return spark

# Function to stop a streaming query by name
def stop_existing_query(spark,query_name):
    for query in spark.streams.active:
        if query.name == query_name:
            print(f"Stopping existing query: {query_name}")
            query.stop()


def read_from_kafka(spark,consume_topic):
       # Read the Kafka stream
    df_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092,broker:29092") \
        .option("subscribe", consume_topic) \
        .option("startingOffsets", "earliest") \
        .load()
    return df_stream

# Function to fetch and parse new data from the in-memory table
def fetch_new_data(df_stream,spark,query_name,schema):

    parsed_df = df_stream \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*")

    # Write the parsed DataFrame to an in-memory table
    query = parsed_df.writeStream \
        .outputMode("append") \
        .format("memory") \
        .queryName(query_name) \
        .start()

    # Allow some time for data to be collected in the in-memory table
    # Adjust the sleep time as needed based on your data stream rate  
    time.sleep(10)

    # Read the data from the in-memory table into a Spark DataFrame
    spark_df = spark.sql(f"SELECT * FROM {query_name}")

    # Convert the Spark DataFrame to a Pandas DataFrame
    pandas_df = spark_df.toPandas()

    # Stop the streaming query
    query.stop()
    
    return pandas_df