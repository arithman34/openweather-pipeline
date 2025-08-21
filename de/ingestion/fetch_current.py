import sys
import os
import json
import datetime

import httpx
import backoff

from utils.openweather import require_api_key, http_client, get_logger


BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
CITIES_PATH = "config/uk_cities.json"

# --- Logging ---
logger = get_logger("fetch_current")


# --- Helper Functions ---
def sanitize_name(name: str) -> str:
    """Sanitize city name for filesystem-safe filename."""
    return name.replace(" ", "_").lower()


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
def write_weather(city: dict, payload: dict, output_dir: str) -> None:
    """Write the weather data to a JSON file."""
    dt = datetime.datetime.now(datetime.timezone.utc)
    date = dt.strftime("%Y-%m-%d")
    hour = dt.strftime("%H")

    dir_path = os.path.join(output_dir, f"date={date}", f"hour={hour}")
    os.makedirs(dir_path, exist_ok=True)

    fname = f"{sanitize_name(city['name'])}.json"
    fpath = os.path.join(dir_path, fname)

    with open(fpath, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)


# --- Pipeline Execution ---
def run_pipeline(output_dir: str) -> int:
    """Fetch current weather for all cities in cities_path."""
    with open(CITIES_PATH, "r", encoding="utf-8") as f:
        cities = json.load(f)

    if not cities:
        logger.error("No cities found in the provided file.")
        return 0
    
    count = 0
    with http_client() as client:
        for city in cities:
            try:
                payload = fetch_current_weather(client, city["lat"], city["lon"])
                write_weather(city, payload, output_dir)
                count += 1
            except Exception as e:
                logger.error(f"Failed for {city['name']}: {e}")
    
    return count


# --- Main Function ---
def main():
    try:
        output_dir = "data/bronze/openweather/current"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        
        logger.info("Starting job...")
        count = run_pipeline(output_dir)
        logger.info(f"Job complete! {count} cities processed.")
    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
