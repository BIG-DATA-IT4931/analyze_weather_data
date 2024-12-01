import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from dotenv import load_dotenv
import pyspark.sql.types as T
import logging
from src.schema import WEATHER_SCHEMA
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


load_dotenv()

PROJECT_ID = os.getenv('PROJECT_ID')
CONSUME_TOPIC_WEATHER_CSV = os.getenv('CONSUME_TOPIC_WEATHER_CSV')
KAFKA_ADDRESS = os.getenv('KAFKA_ADDRESS')
KAFKA_BOOTSTRAP_SERVERS = f"{KAFKA_ADDRESS}:{os.getenv('BROKER1')},{KAFKA_ADDRESS}:{os.getenv('BROKER2')}"  # Cấu hình Kafka
GCP_GCS_BUCKET = os.getenv('GCP_GCS_BUCKET')
GCS_STORAGE_PATH = 'gs://' + GCP_GCS_BUCKET + '/realtime2'
CHECKPOINT_PATH = 'gs://' + GCP_GCS_BUCKET + '/realtime2/checkpoint/'
CHECKPOINT_PATH_BQ = 'gs://' + GCP_GCS_BUCKET + '/realtime2/checkpoint_bq/'

def read_from_kafka(consume_topic: str):
    try:
        df_stream = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", consume_topic) \
            .option("startingOffsets", "latest") \
            .option("checkpointLocation", CHECKPOINT_PATH) \
            .option("failOnDataLoss", "false") \
            .load()

        logger.info("Đọc dữ liệu từ Kafka thành công!")
        return df_stream
    except Exception as e:
        logger.error(f"Error when reading from Kafka: {e}")
        raise


def parse_weather_from_kafka_message(df, schema):
    try:
        assert df.isStreaming is True, "DataFrame doesn't receive streaming data"
        
        df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        
        col = F.split(df['value'], ', ')
        for idx, field in enumerate(schema):
            df = df.withColumn(field.name, col.getItem(idx).cast(field.dataType))
        
        df = df.withColumn("wind_speed", F.sqrt(F.pow(df["u10"], 2) + F.pow(df["v10"], 2)))
        
        logger.info("Dữ liệu đã được chuyển đổi và tính toán thành công!")
        
        return df.select([field.name for field in schema] + ["wind_speed"])
    except Exception as e:
        logger.error(f"Error when parsing Kafka message: {e}")
        raise
def process_batch(df, batch_id):
    try:

        if not df.rdd.isEmpty():  
            num_rows = df.count() 
            logger.info(f"Batch {batch_id}: Đã đọc {num_rows} dòng từ Kafka.")
        else:
            logger.info(f"Batch {batch_id}: Không có dữ liệu mới.")
    except Exception as e:
        logger.error(f"Error processing batch {batch_id}: {e}")

def create_file_write_stream(stream, storage_path, checkpoint_path='/checkpoint', trigger="10 seconds", output_mode="append", file_format="parquet"):
    write_stream = (stream
                    .writeStream
                    .foreachBatch(process_batch) 
                    .format(file_format)
                    .option("path", storage_path)
                    .option("checkpointLocation", checkpoint_path)
                    .trigger(processingTime=trigger)
                    .outputMode(output_mode)
                    ) 
    return write_stream
def create_file_write_stream_bq(stream,  checkpoint_path='/checkpoint', trigger="10 seconds", output_mode="append"):
    write_stream = (stream
                    .writeStream
                    .format("bigquery")
                    .option("table", f"{PROJECT_ID}.realtime.weathers")
                    .option("checkpointLocation", checkpoint_path)
                    .trigger(processingTime=trigger)
                    .outputMode(output_mode))

    return write_stream
if __name__ == "__main__":
    try:
        os.environ['KAFKA_ADDRESS'] = KAFKA_ADDRESS
        os.environ['GCP_GCS_BUCKET'] = GCP_GCS_BUCKET
        
        spark = SparkSession.builder.appName('streaming-examples').getOrCreate()
        spark.conf.set('temporaryGcsBucket', 'dataproc-staging-asia-east2-58292358803-ffnad45o')
        spark.sparkContext.setLogLevel('WARN') 
        spark.streams.resetTerminated()  

        df_consume_stream = read_from_kafka(consume_topic=CONSUME_TOPIC_WEATHER_CSV)
        logger.info("Schema của dữ liệu từ Kafka: ")
        df_consume_stream.printSchema() 
        df_weather = parse_weather_from_kafka_message(df_consume_stream, WEATHER_SCHEMA)
        logger.info("Schema sau khi chuyển đổi: ")
        df_weather.printSchema() 
        write_stream = create_file_write_stream(df_weather, GCS_STORAGE_PATH, checkpoint_path=CHECKPOINT_PATH)
        write_bq = create_file_write_stream_bq(df_weather, checkpoint_path=CHECKPOINT_PATH_BQ)

        write_stream.start()  
        write_bq.start()

        spark.streams.awaitAnyTermination()
    except Exception as e:
        logger.error(f"Error in the streaming process: {e}")
