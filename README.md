# Electricity-Production-Analytics-Pipeline

## Description:
This in an end-to-end data engineering pipeline that ingests simulated real-time electricity production events via Kafka, processes them with Pyspark, and orchestrates the entire workflow using AirFlow. Originally a pair project, this is an updated version with additional error handling and logging. 

## Instructions:
- To run this program, you will need Docker installed on your system: https://www.docker.com/get-started/
- Run this command while in the root folder of the project: docker-compose up -d
- Start the producer using the terminal: python -m kafka_service.electricity_production_producer
- Run the consumer and RDD/DF transformations via Airflow UI.

## Status:
Work in progress.
