import csv
from time import sleep
from typing import Dict, Tuple, List
from kafka import KafkaProducer

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f'Record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

class WeatherCSVProducer:
    def __init__(self, props: Dict):
        self.producer = KafkaProducer(**props)

    @staticmethod
    def read_records(resource_path: str) -> List[Tuple[str, str]]:
        records = []
        try:
            with open(resource_path, 'r') as f:
                reader = csv.reader(f)
                header = next(reader)  # Bỏ qua header
                for row in reader:
                    records.append((f"{row[0]}_{row[1]}_{row[2]}", ', '.join(row)))
        except Exception as e:
            print(f"Error reading records from CSV: {e}")
        return records

    def publish(self, topic, records: List[Tuple[str, str]], sleep_time: float = 0.5):
        for key, value in records:
            try:
                future = self.producer.send(topic=topic, key=key.encode('utf-8'), value=value.encode('utf-8'))
                result = future.get(timeout=10)
                print(f"Produced record to topic {result.topic} partition [{result.partition}] @ offset {result.offset}")
            except Exception as e:
                print(f"Error producing record: {e}")
            sleep(sleep_time)
        self.producer.flush()
