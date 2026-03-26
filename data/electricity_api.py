import os
import requests
from dotenv import load_dotenv

def poll_api():

    load_dotenv()

    api_base_url = os.getenv("BASE_URL")
    api_key = os.getenv("API_KEY")
    url = f"{api_base_url}?api_key={api_key}&frequency=hourly&data[0]=value&start=2026-02-22T00&end=2026-02-28T00&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=500"

    try:

        response = requests.get(url, timeout=10)

        if response.status_code == 200:
            data = response.json()
            return data.get("response", {}).get("data", [])
        else:
            print(f"Request failed with status code: {response.status_code}")
            return []

    except Exception as e:
        print(f"[ERROR] {e}")
        raise