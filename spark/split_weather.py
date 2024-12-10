import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.types import LongType, StructType, StructField, FloatType, IntegerType, DoubleType
from pyspark.sql.functions import col, from_unixtime, year, month, dayofmonth, hour, monotonically_increasing_id
from pyspark.sql import functions as F

credentials_location = '/home/phuc_0703/learnDE/data-engineering-zoomcamp/strong-ward-437213-j6-c3ae16d10e5f.json'

conf = SparkConf() \
    .setMaster('spark://dataengineer.europe-west1-b.c.strong-ward-437213-j6.internal:7077') \
    .setAppName('weather_split') \
    .set("spark.jars", "/home/phuc_0703/learnDE/data-engineering-zoomcamp/05-batch/libs/gcs-connector-hadoop3-2.2.5.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)

sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

schema_weather = StructType([
    StructField("time", LongType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("number", LongType(), True),
    StructField("step", LongType(), True),
    StructField("surface", DoubleType(), True),
    StructField("valid_time", LongType(), True),
    StructField("u10", FloatType(), True),
    StructField("v10", FloatType(), True),
    StructField("d2m", FloatType(), True),
    StructField("t2m", FloatType(), True),
    StructField("msl", FloatType(), True),
    StructField("sst", FloatType(), True),
    StructField("sp", FloatType(), True),
    StructField("tcc", FloatType(), True),
    StructField("tciw", FloatType(), True),
    StructField("tclw", FloatType(), True)
])

df_weather = spark.read.option("basePath", "gs://weather_bigdata_20241/other_data/") \
                       .schema(schema_weather) \
                       .parquet("gs://weather_bigdata_20241/other_data/*")

fact_weather = df_weather.select(
    monotonically_increasing_id().alias("fact_id"),
    col("time").alias("time_id"),
    F.concat_ws(",", col("latitude"), col("longitude")).alias("location_id"),
    F.lit(None).alias("weather_id"),
    col("u10").alias("u_wind"),
    col("v10").alias("v_wind"),
    col("d2m").alias("dewpoint_temp"),
    col("t2m").alias("air_temp"),
    col("msl").alias("mean_sea_level_pressure"),
    col("sst").alias("sea_surface_temp"),
    col("sp").alias("surface_pressure"),
    col("tcc").alias("total_cloud_cover"),
    col("tciw").alias("cloud_ice_water_content"),
    col("tclw").alias("cloud_liquid_water_content")
)

time_dimension = df_weather.select(
    col("time").alias("time_id"),
    from_unixtime(col("time")).alias("time"),
    year(from_unixtime(col("time"))).alias("year"),
    month(from_unixtime(col("time"))).alias("month"),
    dayofmonth(from_unixtime(col("time"))).alias("day"),
    hour(from_unixtime(col("time"))).alias("hour")
).distinct()

location_dimension = df_weather.select(
    F.concat_ws(",", col("latitude"), col("longitude")).alias("location_id"),
    col("latitude"),
    col("longitude"),
    F.lit("Unknown Region").alias("region")
).distinct()

weather_conditions_dimension = df_weather.select(
    monotonically_increasing_id().alias("weather_id"),
    F.lit("Unknown").alias("weather_type"),
    F.lit("Unknown").alias("wind_category"),
    F.lit("Unknown").alias("cloud_cover_category")
).distinct()

df_weather_repartitioned = weather_conditions_dimension.repartition(49)

output_fact = "gs://weather_bigdata_20241/split_dim/fact_weather_measurements"
output_time = "gs://weather_bigdata_20241/split_dim/dim_time"
output_location = "gs://weather_bigdata_20241/split_dim/dim_location"
output_weather_conditions = "gs://weather_bigdata_20241/split_dim/dim_weather_conditions"

fact_weather.write.mode("overwrite").parquet(output_fact)
time_dimension.write.mode("overwrite").parquet(output_time)
location_dimension.write.mode("overwrite").parquet(output_location)
weather_conditions_dimension.write.mode("overwrite").parquet(output_weather_conditions)

print("Job completed successfully.")
