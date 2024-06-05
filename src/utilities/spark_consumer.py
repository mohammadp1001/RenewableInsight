from streaming import read_from_kafka
from pyspark.sql import SparkSession
from settings import RECORDS_SCHEMA


CONSUME_TOPIC_ACTUALLOAD_CSV  = 'actualload'


# read_streaming data
df_consume_stream = read_from_kafka(consume_topic=CONSUME_TOPIC_ACTUALLOAD_CSV)
print(df_consume_stream.printSchema())


# parse streaming data
# df_records = parse_ride_from_kafka_message(df_consume_stream, RECORDS_SCHEMA)
# print(df_records.printSchema())

# sink_console(df_rides, output_mode='append')

# df_trip_count_by_vendor_id = op_groupby(df_rides, ['vendor_id'])
# df_trip_count_by_pickup_date_vendor_id = op_windowed_groupby(df_rides, window_duration="10 minutes",
#                                                                slide_duration='5 minutes')

# write the output out to the console for debugging / testing
# sink_console(df_trip_count_by_vendor_id)
# write the output to the kafka topic
# df_trip_count_messages = prepare_df_to_kafka_sink(df=df_trip_count_by_pickup_date_vendor_id,
#                                                    value_columns=['count'], key_column='vendor_id')
# kafka_sink_query = sink_kafka(df=df_trip_count_messages, topic=TOPIC_WINDOWED_VENDOR_ID_COUNT)

# spark.streams.awaitAnyTermination()