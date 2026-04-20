import json
import logging
from typing import Optional
from kafka import KafkaConsumer
from utils.logger import setup_logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

json_schema = StructType() \
    .add("period", StringType()) \
    .add("respondent", StringType()) \
    .add("respondent-name", StringType()) \
    .add("fueltype", StringType()) \
    .add("type-name", StringType()) \
    .add("value", IntegerType()) \
    .add("value-units", StringType())

def consume_records():

    logger = setup_logging(__name__)

    logger.info("Starting consumer...")

    record_count = 0
    running = True

    spark = SparkSession.builder \
        .appName("Electricity-Production-Analytics-Pipeline") \
        .getOrCreate()

    try:
        raw_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
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
            .option("path", "/opt/airflow/data/raw/") \
            .option("checkpointLocation", "/opt/airflow/data/checkpoints") \
            .trigger(once=True) \
            .start()
        
        query.awaitTermination()
        logger.info("Finished batch sucessfully.")

    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        spark.stop()


def main():

    # Kafka topic
    topic = "electricity-production"

    try:
        consume_records()
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        pass

if __name__ == '__main__':
    main()