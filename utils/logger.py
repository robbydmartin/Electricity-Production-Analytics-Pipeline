import os
import logging
from logging import handlers
from logging.handlers import RotatingFileHandler
from typing import Optional

FILENAME = "./data/logs/electricity_production_logs.log"
MAX_BYTES = 50000

def setup_logging(name: str) -> logging.Logger:

    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

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

    # Silence any Kafka logs
    logging.getLogger("kafka").propagate = False

    return logger