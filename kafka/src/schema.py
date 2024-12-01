import pyspark.sql.types as T

WEATHER_SCHEMA = T.StructType([
    T.StructField("time", T.TimestampType()),
    T.StructField("latitude", T.FloatType()),
    T.StructField("longitude", T.FloatType()),
    T.StructField("number", T.IntegerType()),
    T.StructField("step", T.StringType()),
    T.StructField("surface", T.FloatType()),
    T.StructField("valid_time", T.TimestampType()),
    T.StructField("u10", T.FloatType()),
    T.StructField("v10", T.FloatType()),
    T.StructField("d2m", T.FloatType()),
    T.StructField("t2m", T.FloatType()),
    T.StructField("msl", T.FloatType()),
    T.StructField("sst", T.FloatType(), nullable=True),
    T.StructField("sp", T.FloatType()),
    T.StructField("tcc", T.FloatType()),
    T.StructField("tciw", T.FloatType()),
    T.StructField("tclw", T.FloatType())
])
