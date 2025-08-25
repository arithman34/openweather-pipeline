import logging
import time
import os
import httpx

from airflow.models import Variable


API_KEY = Variable.get("OPENWEATHER_API_KEY")
USER_AGENT = Variable.get("USER_AGENT")


def require_api_key() -> str:
    """Ensure the OpenWeather API key is set and return it."""
    if not API_KEY:
        raise RuntimeError("OPENWEATHER_API_KEY not set (add to environment or .env).")
    return API_KEY


def http_client() -> httpx.Client:
    """Reusable HTTP client with User-Agent and sane timeouts."""
    headers = {"User-Agent": USER_AGENT}
    return httpx.Client(timeout=httpx.Timeout(20.0, connect=10.0), headers=headers)


def get_logger(name: str) -> logging.Logger:
    log_file = os.path.join("logs", f"{name}.log")

    os.makedirs("logs", exist_ok=True)

    # UTC timestamp with ISO 8601 format
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)sZ | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        filename=log_file,
        filemode="a"
    )

    logging.Formatter.converter = time.gmtime

    logger = logging.getLogger(name)

    # Suppress noisy libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)

    return logger
