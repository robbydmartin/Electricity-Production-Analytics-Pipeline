from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import time
from typing import Optional
from eia_api_connection import poll_api
from electricity_production_faker import create_single_record

def create_producer(bootstrap_servers: str = "localhost:9092") -> Optional[KafkaProducer]:

    try:
        # Create a producer
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
    
def run_producer(producer, topic: str, interval: float = 1.0, mini_batch_size: int = 5) -> None:

    print("Starting producer...")

    # Variable to keep track of records produced
    record_count = 0
    running = True

    try:
        while running:
            for _ in range(mini_batch_size):

                # Create a record
                record = create_single_record()

                # Send the record to the Kafka topic
                producer.send(
                    topic=topic,
                    key=record.get("period", "unknown"),
                    value=record
                )

                # Increase record count
                record_count += 1

            # Wait before producing again
            time.sleep(interval)

    except KeyboardInterrupt:
        print("Stopping producer...")
        running = False
    except Exception as e:
        print(f"    [ERROR] Kafka send error: {e}")
    finally:
        producer.flush()
        print(f"Total records produced: {record_count}")

    
def main():
    
    # Kafka topic
    topic = "electricity-production"

    # Create the producer
    producer = create_producer()

    # Run the producer
    try:
        run_producer(producer, topic, interval=1)
    except Exception as e:
        print(f"    [ERROR] An unexpected error occured: {e}")
    finally:
        producer.close()

if __name__ == '__main__':
    main()