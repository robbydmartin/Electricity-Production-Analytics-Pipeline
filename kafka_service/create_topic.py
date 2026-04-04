import logging
import os
from typing import Optional
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

logger = logging.getLogger(__name__)

def create_admin_client(bootstrap_servers: str = "localhost:9094") -> Optional[KafkaAdminClient]:

    admin_client = KafkaAdminClient(
        bootstrap_servers = bootstrap_servers,
        client_id = "topic_creator"
    )

    return admin_client

def create_single_topic(admin_client, topic_name: str, num_partitions: int = 3, replication_factor: int = 1) -> bool:
    
    logger.info(f"Creating topic '{topic_name}'...")

    topic = NewTopic(
        name= topic_name,
        num_partitions= num_partitions,
        replication_factor= replication_factor
    )

    try:
        admin_client.create_topics([topic])
        logger.info(f"Topic '{topic_name}' created with {num_partitions} partitions")
    except TopicAlreadyExistsError:
        logger.error(f"Topic '{topic_name}' already exists")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return False

    return True;

def create_multiple_topics(admin_client, topics_config: list) -> bool:
    
    topics = []

    for config in topics_config:
        topic = NewTopic(
            name= config["name"],
            num_partitions= config["num_partitions"],
            replication_factor= config["replication_factor"]
        )
        topics.append(topic)

    try:
        admin_client.create_topics([topics])
        logger.info(f"Created {len(topics)} topics")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return False
    
def main():

    # Create admin client
    admin_client = create_admin_client()

    # Create topic
    create_single_topic(admin_client, "electricity-production")

    # Cleanup
    admin_client.close()

if __name__ == '__main__':
    main()