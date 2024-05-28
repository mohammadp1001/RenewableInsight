import pyspark.sql.types as T

INPUT_DATA_PATH = '/tmp/'
BOOTSTRAP_SERVERS = 'localhost:9092'

MONTH = '05'
YEAR = '2024'
PRODUCE_TOPIC_ACTUALLOAD_CSV = CONSUME_TOPIC_ACTUALLOAD_CSV = 'actualload'


RECORDS_SCHEMA = T.StructType(
    [T.StructField("DateTime", T.TimestampType()),
     T.StructField('ResolutionCode', T.StringType()),
     T.StructField('AreaCode', T.StringType()),
     T.StructField("AreaTypeCode", T.StringType()),
     T.StructField("AreaName", T.StringType()),
     T.StructField("MapCode", T.StringType()),
     T.StructField("TotalLoadValue", T.FloatType()),
     ])
