import logging
from utils.logger import setup_logging

def main():

    logger = setup_logging()

    logger.info("Logger setup complete.")


if __name__ == '__main__':
    main()