import os
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

def create_single_topic(name, num_partitions=3, replication_factor=1):

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
    
    # Connect to kafka
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id='topic-creator'
    )

    # Define a new topic
    new_topic = NewTopic(
        name=name,
        num_partitions=num_partitions,
        replication_factor=replication_factor
    )

    # Create the topic
    try:
        admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        print(f"Topic '{name}' created with {num_partitions} partitions")
    except TopicAlreadyExistsError:
        print(f"Topic '{name}' already exists")
    except Exception as e:
        print(f"Error creating topic: {e}")
    finally:
        admin_client.close()

    # Close the connection
    admin_client.close()