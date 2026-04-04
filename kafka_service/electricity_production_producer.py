from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import time
import logging
from typing import Optional
from kafka_service.create_topic import main
from kafka_service.electricity_production_simulator import create_single_record

logger = logging.getLogger(__name__)

def create_producer(bootstrap_servers: str = "localhost:9094") -> Optional[KafkaProducer]:

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
            logger.info("Producer created successfully")
    
        return producer
    
    except NoBrokersAvailable:
        logger.error("No Kafka brokers available")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return None
    
def run_producer(producer, topic: str, interval: float = 1.0, mini_batch_size: int = 5) -> None:

    logger.info("Starting producer...")

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
        logger.info("Stopping producer...")
        running = False
    except Exception as e:
        logger.error(f"Kafka send error: {e}")
    finally:
        producer.flush()
        logger.info(f"Total records produced: {record_count}")

    
def main():
    
    # Kafka topic
    topic = "electricity-production"

    # Create the producer
    producer = create_producer()

    # Run the producer
    try:
        run_producer(producer, topic, interval=1)
    except Exception as e:
        logger.error(f"An unexpected error occured: {e}")
    finally:
        producer.close()

if __name__ == '__main__':
    main()