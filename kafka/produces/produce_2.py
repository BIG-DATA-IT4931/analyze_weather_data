
import sys
import argparse
import time
from kafka import KafkaAdminClient
from kafka.admin.new_partitions import NewPartitions
from produce import WeatherCSVProducer,delivery_report
PRODUCE_TOPIC_WEATHER_CSV = 'weathers'

INPUT_DATA_PATH = '../resources/other_data_31_12_2023.csv'
BOOTSTRAP_SERVERS = ['localhost:9093']



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--time', type=float, default=0.5, help='Time interval between each message')
    args = parser.parse_args(sys.argv[1:])

    try:
        client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
        client.create_partitions({PRODUCE_TOPIC_WEATHER_CSV: NewPartitions(4)})
    except Exception as e:
        print(f"Admin client exception: {e}")

    config = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'key_serializer': lambda x: x.encode('utf-8'),
        'value_serializer': lambda x: x.encode('utf-8'),
        'acks': 'all',
    }

    producer = WeatherCSVProducer(props=config)
    weather_records = producer.read_records(resource_path=INPUT_DATA_PATH)
    print(f"Producing records to topic: {PRODUCE_TOPIC_WEATHER_CSV}")
    start_time = time.time()
    producer.publish(records=weather_records, sleep_time=args.time,topic=PRODUCE_TOPIC_WEATHER_CSV)
    end_time = time.time()
    print(f"Producing records took: {end_time - start_time} seconds")
