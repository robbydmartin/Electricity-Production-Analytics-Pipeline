import logging
import os
import requests
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

def poll_api(previous_hours_back: int = 12) -> list:

    load_dotenv()

    api_base_url = os.getenv("BASE_URL")
    api_key = os.getenv("API_KEY")

    start_datetime = (datetime.now(timezone.utc) - timedelta(hours=previous_hours_back)).strftime("%Y-%m-%dT%H")
    end_datetime = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H")

    url = f"{api_base_url}?api_key={api_key}&frequency=hourly&data[0]=value&start={start_datetime}&end={end_datetime}&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

    try:

        response = requests.get(url, timeout=10)

        if response.status_code == 200:
            logger.info("API polled successfully")
            data = response.json()
            return data.get("response", {}).get("data", [])
        else:
            logger.error(f"Request failed with status code: {response.status_code}")
            return []

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        raise
