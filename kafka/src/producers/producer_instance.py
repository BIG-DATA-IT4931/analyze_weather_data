import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
import sys
import argparse
import time
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin.new_partitions import NewPartitions
from kafka.errors import TopicAlreadyExistsError
from kafka.admin import NewTopic
from producer import WeatherProducer,delivery_report
import os
from dotenv import load_dotenv
import datetime

def producer_instance(topic, kafka_address,broker,producer_name,datetime_obj: datetime):
    load_dotenv()
    PRODUCE_TOPIC_WEATHER_CSV =  topic
    BOOTSTRAP_SERVERS = [kafka_address+":"+broker]
    parser = argparse.ArgumentParser()
    parser.add_argument('--time', type=float, default=0.5, help='Time interval between each message')
    args = parser.parse_args(sys.argv[1:])

    try:
        client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)

        existing_topics = client.list_topics()
        if PRODUCE_TOPIC_WEATHER_CSV not in existing_topics:
            try:
                client.create_topics([NewTopic(name=PRODUCE_TOPIC_WEATHER_CSV, num_partitions=4, replication_factor=1)])
                print(f"Topic '{PRODUCE_TOPIC_WEATHER_CSV}' created successfully.")
            except TopicAlreadyExistsError:
                print(f"Topic '{PRODUCE_TOPIC_WEATHER_CSV}' already exists.")
        else:
            print(f"Topic '{PRODUCE_TOPIC_WEATHER_CSV}' already exists.")
    except Exception as e:
        print(e)
        pass
            
    except Exception as e:
        print(f"Admin client exception: {e}")
    config = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'key_serializer': None,  
        'value_serializer': None,  
        'acks': 'all',
        'client_id':producer_name
    }
    producer = WeatherProducer(props=config)
    
    weather_records = producer.read_records(datetime_obj)
    print(f"Producing records to topic: {PRODUCE_TOPIC_WEATHER_CSV}")
    start_time = time.time()
    print(start_time)
    producer.publish(records=weather_records, sleep_time=args.time,topic=PRODUCE_TOPIC_WEATHER_CSV,producer_name=producer_name)
    end_time = time.time()
    print(f"Producing records took: {end_time - start_time} seconds by {producer_name}")
