import json
from types import Optional
from kafka import KafkaConsumer

def create_consumer(topic: str, bootstrap_servers: str = "localhost:9092") -> Optional[KafkaConsumer]:
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers= bootstrap_servers,
        group_id= "electricity-production-consumer",
        auto_offset_reset= "earliest",
        enable_auto_commit= True,
        value_deserializer= lambda v : json.loads(v.decode('utf-8'))
    )

    return consumer

def consume_records(consumer):

    record_count = 0

    for record in consumer:
        product = record.value
        record_count += 1

    print(f"Consumed {record_count} total records")



def main():

    topic = "electricity-production"
    consumer = create_consumer(topic=topic)

    consumer.close()
if __name__ == '__main__':
    main()