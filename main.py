from data.electricity_api import poll_api
from kafka_utils.create_topic import create_single_topic
from kafka_utils.electric_producer import ElectricityProductionProducer

def run():

    producer = ElectricityProductionProducer(bootstrap_servers='localhost:9094')
    count = 0

    records = poll_api()
    print("Sending records to topic..")

    for record in records:
        producer.send_data_to_kafka("electricity-production", record)
        count += 1
        
    print(f"Producer: {count} records sent.")

def main():

    create_single_topic('electricity-production')

    run()

if __name__ == '__main__':
    main()