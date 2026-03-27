from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
import json
from typing import Optional
from eia_api_connection import poll_api

def create_producer(bootstrap_servers: str = "localhost:9092") -> Optional[KafkaProducer]:

    try:
        producer = KafkaProducer(
            bootstrap_servers= bootstrap_servers,
            acks= 'all',
            enable_idempotence= True,
            retries= 5,
            retry_backoff_ms= 100,
            linger_ms= 20,
            compression_type= 'lz4',
            key_serializer= lambda k: k.encode('utf-8'),
            value_serializer= lambda v: json.dumps(v).encode('utf-8')
        )

        if producer:
            print(f"    [SUCESS] Producer created successfully")
    
        return producer
    except NoBrokersAvailable:
        print(f"    [ERROR] No Kafka brokers available")
        return None
    except Exception as e:
        print(f"    [ERROR] {e}")
        return None
    
def run_producer(producer, topic: str, records):

    count = 0

    try:
        for record in records:
            producer.send(
                topic=topic,
                key=record.get("period", "unknown"),
                value=record
            )
            count += 1

    except Exception as e:
        print(f"    [ERROR] Kafka send error: {e}")
    finally:
        print(f"Total records produced: {count}")

    producer.flush()


def main():
    
    topic = "electricity-production"
    records = poll_api(previous_hours_back=12)

    if not records:
        print(" [WARNING] No data to send")

    producer = create_producer()

    try:
        run_producer(producer, topic, records)
    except Exception as e:
        print(f"    [ERROR] An unexpected error occured: {e}")
    finally:
        producer.close()

    
if __name__ == '__main__':
    main()