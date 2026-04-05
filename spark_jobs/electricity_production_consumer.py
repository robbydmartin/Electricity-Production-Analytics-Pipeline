import json
import logging
from typing import Optional
from kafka import KafkaConsumer
from utils.logger import setup_logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

logger = setup_logging(__name__)

json_schema = StructType() \
    .add("period", StringType()) \
    .add("respondent", StringType()) \
    .add("respondent-name", StringType()) \
    .add("fueltype", StringType()) \
    .add("type-name", StringType()) \
    .add("value", IntegerType()) \
    .add("value-units", StringType())

def create_consumer(topic: str, bootstrap_servers: str = "kafka:9092") -> Optional[KafkaConsumer]:
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers= bootstrap_servers,
        group_id= "electricity-production-group",
        auto_offset_reset= "earliest",
        enable_auto_commit= True,
        key_deserializer = lambda k : k.decode('utf-8') if k else None,
        value_deserializer= lambda v : json.loads(v.decode('utf-8'))
    )

    return consumer

def process_record(record: dict):

    print(f"Period: {record.get('period')}")
    print(f"Respondent: {record.get('respondent')}")
    print(f"Fuel type: {record.get('fueltype')}")

def consume_records(consumer):

    logger.info("Starting consumer...")

    record_count = 0
    running = True

    spark = SparkSession.builder \
        .appName("Electricity-Production-Analytics-Pipeline") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

    try:
        raw_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "electricity-production") \
            .option("startingOffsets", "earliest") \
            .load()
    
        parsed_df = raw_df \
            .selectExpr("CAST(value AS STRING) as json_str") \
            .select(from_json(col("json_str"), json_schema).alias("data")) \
            .select("data.*")

        query = parsed_df.writeStream \
            .outputMode("append") \
            .format("json") \
            .option("path", "/data/checkpoints") \
            .option("checkpointLocation", "/data/checkpoints") \
            .start()
        
        query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        spark.stop()


def main():

    # Kafka topic
    topic = "electricity-production"

    # Create a consumer
    consumer = create_consumer(topic=topic)

    try:
        consume_records(consumer)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        consumer.close()

if __name__ == '__main__':
    main()