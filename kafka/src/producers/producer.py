import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from time import sleep
from typing import Dict, Tuple, List
from kafka import KafkaProducer
from datetime import datetime
import pandas as pd
from src.resources.get_data import fetch_weather_data


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f'Record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

class WeatherProducer:
    def __init__(self, props: Dict):
        self.producer = KafkaProducer(**props)

    @staticmethod
    def read_records(datetime_obj: datetime) -> List[Tuple[str, str]]:
        records = []
        try:
           
            records=fetch_weather_data(datetime_obj)
        except Exception as e:
            print(f"Error reading records from CSV: {e}")
        return records

    def publish(self, topic, records: List[Tuple[str, str]], producer_name,sleep_time: float = 0.5,):
        for key, value in records:
            try:
                future = self.producer.send(topic=topic, key=key.encode('utf-8'), value=value.encode('utf-8'))
                result = future.get(timeout=10)
                print(f"Produced record to topic {result.topic} partition [{result.partition}] @ offset {result.offset} from {producer_name}")
            except Exception as e:
                print(f"Error producing record: {e}")
            sleep(sleep_time)
        self.producer.flush()