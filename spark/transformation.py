import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import col, from_unixtime

# Define the path to the credentials file
credentials_location = '/home/phuc_0703/learnDE/data-engineering-zoomcamp/strong-ward-437213-j6-c3ae16d10e5f.json'

# Set up the Spark configuration
conf = SparkConf() \
    .setMaster('spark://dataengineer.europe-west1-b.c.strong-ward-437213-j6.internal:7077') \
    .setAppName('transformation') \
    .set("spark.jars", "/home/phuc_0703/learnDE/data-engineering-zoomcamp/05-batch/libs/gcs-connector.jar,/home/phuc_0703/learnDE/data-engineering-zoomcamp/05-batch/libs/bigquery-connector.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)

# Initialize SparkContext and SparkSession
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=sc.getConf()).getOrCreate()

# Load the merged data from the "weather_all" directory (partitioned by year, month, and day)
df_weather_all = spark.read.parquet("gs://weather_bigdata_20241/weather_all")
df_weather_main = df_weather_all
df_weather_main = df_weather_main.withColumn("wind_speed", F.sqrt(F.col("u10")**2 + F.col("v10")**2))
df_weather_main = df_weather_main.withColumn("saturation_vapor_pressure", 
    6.11 * (10 ** (7.5 * F.col("t2m") / (F.col("t2m") + 237.3)))
)
df_weather_main = df_weather_main.withColumn("relative_humidity", 
    (F.col("vapor_pressure") / F.col("saturation_vapor_pressure")) * 100
)
df_weather_main = df_weather_main.withColumn("apparent_temperature", 
    (F.col("t2m")) +
    0.33 * F.col("vapor_pressure") - 0.70 * F.col("wind_speed") - 4.00
)
R = 287.05
df_weather_main = df_weather_main.withColumn("air_density", 
    F.col("sp") / (R * (F.col("t2m") + 273.15)) 
)
import math
df_weather_main = df_weather_main.withColumn(
    "wind_direction",
    F.when(
        (180 + (180 / math.pi) * F.atan2(F.col("u10"), F.col("v10"))) < 0,
        360 + (180 + (180 / math.pi) * F.atan2(F.col("u10"), F.col("v10")))
    ).otherwise(
        180 + (180 / math.pi) * F.atan2(F.col("u10"), F.col("v10"))
    )
)
# Define the latitude and longitude range filters
df_weather_main = df_weather_main.filter(
    (F.col("latitude") >= 8) & (F.col("latitude") <= 24) &
    (F.col("longitude") >= 102) & (F.col("longitude") <= 112)
)

# Select the date and calculate total precipitation
df_weather_main = df_weather_main.groupBy(F.to_date("valid_time").alias("date")) \
    .agg(F.sum("tp").alias("total_precipitation")) \
    .orderBy("date")
df_result = df_weather_main.select("time", "latitude", "longitude", "wind_speed", "vapor_pressure", 
                        "relative_humidity", "apparent_temperature", "air_density", "wind_direction")

from pyspark.sql.functions import year, month, dayofmonth
df_result = df_result \
    .withColumn("year", year(col("time"))) \
    .withColumn("month", month(col("time"))) \
    .withColumn("day", dayofmonth(col("time")))

output_path = "gs://weather_bigdata_20241/result"

df_result.write \
    .partitionBy("year", "month", "day") \
    .mode("overwrite") \
    .parquet(output_path)

# Stop the SparkContext
sc.stop()
