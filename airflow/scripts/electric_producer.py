import json
from kafka import KafkaProducer
from eia_api_connection import poll_api


def create_producer(bootstrap_servers):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks = 'all',
        retries = 5,
        linger_ms = 20,
        compression_type = 'lz4',
        key_serializer = lambda k: k.encode('utf-8'),
        value_serializer = lambda v: json.dumps(v).encode('utf-8')
    )
    return producer

def send_data_to_kafka(topic, data):

    producer = create_producer(bootstrap_servers='kafka:9092')

    try:
        producer.send(
            topic=topic,
            key=data.get("respondent", "unknown"),
            value=data
        )
    except Exception as e:
        print(f"[ERROR] Kafka send error: {e}")

    producer.flush()
    producer.close()

def main():


    records = poll_api()

    print("Sending records to topic..")


    send_data_to_kafka("electricity-production", records)

        
    # print(f"Producer: {count} records sent.")

if __name__ == '__main__':
    main()