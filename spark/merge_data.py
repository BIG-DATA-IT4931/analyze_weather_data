import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
import time

from pyspark import SparkConf
credentials_location = '/home/phuc_0703/learnDE/data-engineering-zoomcamp/strong-ward-437213-j6-c3ae16d10e5f.json'

conf = SparkConf() \
    .setMaster('spark://dataengineer.europe-west1-b.c.strong-ward-437213-j6.internal:7077') \
    .setAppName('merge-data') \
    .set("spark.jars", "/home/phuc_0703/learnDE/data-engineering-zoomcamp/05-batch/libs/gcs-connector.jar,/home/phuc_0703/learnDE/data-engineering-zoomcamp/05-batch/libs/bigquery-connector.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)


sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()
    
    
from pyspark.sql.types import LongType, StructType, StructField, FloatType, IntegerType, DoubleType
from pyspark.sql.functions import col, from_unixtime
from pyspark.sql import functions as F
schema = StructType([
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
                       .schema(schema) \
                       .parquet("gs://weather_bigdata_20241/other_data/*")
                       
schema = StructType([
    StructField("time", LongType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("number", LongType(), True),
    StructField("step", LongType(), True),
    StructField("surface", DoubleType(), True),
    StructField("valid_time", LongType(), True),
    StructField("tp", FloatType(), True)
])


df_rain = spark.read.option("basePath", "gs://weather_bigdata_20241/rain/") \
                       .schema(schema) \
                       .parquet("gs://weather_bigdata_20241/rain/*")
                       
df_rain_filtered = df_rain.filter(df_rain.tp.isNotNull())

from pyspark.sql.functions import col

# Thực hiện JOIN với điều kiện
df_joined = df_rain_filtered.join(
    df_weather,
    (df_rain_filtered.latitude == df_weather.latitude) &
    (df_rain_filtered.longitude == df_weather.longitude) &
    (df_rain_filtered.valid_time == df_weather.time),
    "inner"  # Chỉ lấy các bản ghi khớp nhau (INNER JOIN)
)

# Chọn các cột cần thiết và đổi tên nếu cần
df_joined = df_joined.select(
    df_rain_filtered.latitude.alias("latitude"),
    df_rain_filtered.longitude.alias("longitude"),
    df_rain_filtered.valid_time.alias("time"),
    df_weather.step,
    df_weather.surface,
    df_weather.t2m,
    df_weather.d2m,
    df_weather.u10,
    df_weather.v10,
    df_weather.msl,
    df_weather.sst,
    df_weather.sp,
    df_weather.tcc,
    df_weather.tciw,
    df_weather.tclw,
    df_rain_filtered.tp
)

df_joined = df_joined.withColumn("time", from_unixtime(col("time") / 1_000_000_000).cast("timestamp"))
df_joined = df_joined.withColumn("d2m", F.col("d2m") - 273.15) \
                        .withColumn("t2m", F.col("t2m") - 273.15)

df_joined_repartitioned = df_joined.repartition(49)

output_path = "gs://weather_bigdata_20241/weather_all_main"

df_joined_repartitioned.write \
    .mode("overwrite") \
    .parquet(output_path)