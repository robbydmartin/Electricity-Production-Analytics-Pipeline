import os
import logging
from logging import handlers
from logging.handlers import RotatingFileHandler
from typing import Optional

LOG_DIRECTORY = os.getenv("LOG_DIR", "./data/logs")
os.makedirs(LOG_DIRECTORY, exist_ok=True)

FILENAME = os.path.join(LOG_DIRECTORY, "electricity_production.log")
MAX_BYTES = 5 * 1024 * 1024

def setup_logging(name: str) -> logging.Logger:

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    # Create handler for console output
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter("%(asctime)s | %(levelname)s | %(module)s | %(message)s")
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)

    # Create handler for file logging
    file_handler = RotatingFileHandler(FILENAME, mode="a", maxBytes=MAX_BYTES, backupCount=1)
    file_handler.setLevel(logging.INFO)
    file_format = logging.Formatter("%(asctime)s | %(levelname)s | %(module)s | %(lineno)s | %(message)s")
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)

    logger.propagate = True

    # Silence any Kafka logs
    logging.getLogger("kafka").propagate = False

    return logger