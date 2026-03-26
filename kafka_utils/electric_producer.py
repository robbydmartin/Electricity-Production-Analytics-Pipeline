import json
import atexit
from kafka import KafkaProducer
from data.electricity_api import poll_api

class ElectricityProductionProducer:

    def __init__(self, bootstrap_servers):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks = 'all',
            retries = 5,
            linger_ms = 20,
            compression_type = 'lz4',
            key_serializer = lambda k: k.encode('utf-8'),
            value_serializer = lambda v: json.dumps(v).encode('utf-8')
        )
        atexit.register(self.close)

    def send_data_to_kafka(self, topic, data):

        try:
            self.producer.send(
                topic=topic,
                key=data.get("respondent", "unknown"),
                value=data
            )
        except Exception as e:
            print(f"[ERROR] Kafka send error: {e}")

    def close(self):
        self.producer.flush()
        self.producer.close()
