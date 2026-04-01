import logging
from utils.logger import setup_logging
import create_topic
import electricity_production_producer
import electricity_production_consumer

def main():

    logger = setup_logging()

    logger.info("Logger setup complete.")

if __name__ == '__main__':
    main()