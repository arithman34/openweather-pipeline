import sys
import os
import json
import importlib.resources as pkg_resources
from datetime import datetime, timezone

import httpx
import backoff

from rain_pipeline.scripts.utils import require_api_key, http_client, get_logger
from rain_pipeline import config


BASE_URL = "https://api.openweathermap.org/data/2.5/weather"


# --- Logging ---
logger = get_logger("fetch_current")


# --- Helper Functions ---
def sanitize_name(name: str) -> str:
    """Sanitize place name for filesystem-safe filename."""
    return name.replace(" ", "_").lower()

def load_places():
    with pkg_resources.open_text(config, "populated_places.json") as f:
        return json.load(f)


# --- API Call ---
@backoff.on_exception(
    backoff.expo,
    (httpx.HTTPStatusError, httpx.RequestError),
    max_tries=5,
    jitter=backoff.full_jitter,
    logger=None
)
def fetch_current_weather(client: httpx.Client, lat: float, lon: float) -> dict:
    """Fetch current weather data for a given latitude and longitude."""
    key = require_api_key()
    r = client.get(BASE_URL, params={"lat": lat, "lon": lon, "appid": key})
    r.raise_for_status()
    return r.json()


# --- File Writing ---
def write_weather(place: dict, payload: dict, output_dir: str) -> None:
    """Write the weather data to a JSON file."""
    dt = datetime.now(timezone.utc)
    date = dt.strftime("%Y-%m-%d")
    hour = dt.strftime("%H")

    dir_path = os.path.join(output_dir, f"date={date}", f"hour={hour}")
    os.makedirs(dir_path, exist_ok=True)

    fname = f"{sanitize_name(place['name'])}.json"
    fpath = os.path.join(dir_path, fname)

    with open(fpath, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)


# --- Pipeline Execution ---
def run_pipeline(output_dir: str) -> int:
    """Fetch current weather for all places in places_path."""
    places = load_places()

    if not places:
        logger.error("No places found in the provided file.")
        return 0
    
    count = 0
    with http_client() as client:
        for place in places:
            try:
                payload = fetch_current_weather(client, place["lat"], place["lon"])
                write_weather(place, payload, output_dir)
                count += 1
            except Exception as e:
                logger.error(f"Failed for {place['name']}: {e}")
    
    return count, len(places)


# --- Main Function ---
def main():
    try:
        output_dir = "/usr/local/airflow/data/bronze/openweather/current"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        
        start_time = datetime.now()
        logger.info("Starting job...")
        successful, total = run_pipeline(output_dir)
        seconds = (datetime.now() - start_time).total_seconds()
        logger.info(f"{successful}/{total} processed successfully. Job took {seconds:.3f} seconds.")
    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
