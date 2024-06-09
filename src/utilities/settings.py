import pyspark.sql.types as T


RECORDS_SCHEMA = T.StructType(
    [T.StructField("DateTime", T.StringType()),
     T.StructField("AreaName", T.StringType()),
     T.StructField("TotalLoadValue", T.StringType()),
     ])
