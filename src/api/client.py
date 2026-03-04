"""
API Client — Road Logistics Weather Risk Pipeline
---------------------------------------------------
Fetches live weather data from Open-Meteo (free, no API key)
and country metadata from REST Countries API.

In a real K+N environment this would also pull from:
  - Internal TMS (Transport Management System) for active routes
  - Carrier APIs for live vehicle positions
  - Internal hub/depot database for facility coordinates
"""

import time
import logging
import requests
from typing import Optional

logger = logging.getLogger(__name__)

WEATHER_BASE_URL = "https://api.open-meteo.com/v1/forecast"
COUNTRIES_BASE_URL = "https://restcountries.com/v3.1"
MAX_RETRIES = 3
RETRY_BACKOFF = 2


# ── Major K+N Road Logistics Hubs across Europe ───────────────────────────────
# In production this would come from an internal database or TMS API
LOGISTICS_HUBS = [
    {"id": "HAM", "name": "Hamburg",    "country": "DE", "lat": 53.5511, "lng": 9.9937},
    {"id": "RTD", "name": "Rotterdam",  "country": "NL", "lat": 51.9244, "lng": 4.4777},
    {"id": "WAW", "name": "Warsaw",     "country": "PL", "lat": 52.2297, "lng": 21.0122},
    {"id": "BUD", "name": "Budapest",   "country": "HU", "lat": 47.4979, "lng": 19.0402},
    {"id": "MIL", "name": "Milan",      "country": "IT", "lat": 45.4654, "lng": 9.1859},
    {"id": "MAD", "name": "Madrid",     "country": "ES", "lat": 40.4168, "lng": -3.7038},
    {"id": "PAR", "name": "Paris",      "country": "FR", "lat": 48.8566, "lng": 2.3522},
    {"id": "VIE", "name": "Vienna",     "country": "AT", "lat": 48.2082, "lng": 16.3738},
    {"id": "BUC", "name": "Bucharest",  "country": "RO", "lat": 44.4268, "lng": 26.1025},
    {"id": "PRG", "name": "Prague",     "country": "CZ", "lat": 50.0755, "lng": 14.4378},
    {"id": "LYO", "name": "Lyon",       "country": "FR", "lat": 45.7640, "lng": 4.8357},
    {"id": "MUC", "name": "Munich",     "country": "DE", "lat": 48.1351, "lng": 11.5820},
]

# ── Key Road Corridors (origin → destination) ─────────────────────────────────
# Represents major truck routes K+N operates across Europe
ROAD_CORRIDORS = [
    {"id": "HAM-RTD", "origin": "HAM", "destination": "RTD", "distance_km": 385},
    {"id": "HAM-MUC", "origin": "HAM", "destination": "MUC", "distance_km": 778},
    {"id": "WAW-BUD", "origin": "WAW", "destination": "BUD", "distance_km": 544},
    {"id": "WAW-PRG", "origin": "WAW", "destination": "PRG", "distance_km": 517},
    {"id": "MUC-MIL", "origin": "MUC", "destination": "MIL", "distance_km": 415},
    {"id": "MUC-VIE", "origin": "MUC", "destination": "VIE", "distance_km": 456},
    {"id": "PAR-MAD", "origin": "PAR", "destination": "MAD", "distance_km": 1272},
    {"id": "PAR-LYO", "origin": "PAR", "destination": "LYO", "distance_km": 465},
    {"id": "VIE-BUC", "origin": "VIE", "destination": "BUC", "distance_km": 1284},
    {"id": "RTD-PAR", "origin": "RTD", "destination": "PAR", "distance_km": 502},
    {"id": "MIL-MAD", "origin": "MIL", "destination": "MAD", "distance_km": 1858},
    {"id": "BUD-BUC", "origin": "BUD", "destination": "BUC", "distance_km": 812},
]


def _request_with_retry(url: str, params: Optional[dict] = None) -> dict:
    """GET request with exponential backoff retry logic."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"[Attempt {attempt}] Fetching: {url}")
            response = requests.get(url, params=params, timeout=10)

            if response.status_code == 429:
                wait = RETRY_BACKOFF ** attempt
                logger.warning(f"Rate limited. Waiting {wait}s...")
                time.sleep(wait)
                continue

            response.raise_for_status()
            return response.json()

        except requests.exceptions.Timeout:
            logger.error(f"Timeout on attempt {attempt}")
        except requests.exceptions.ConnectionError:
            logger.error(f"Connection error on attempt {attempt}")
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error: {e}")
            raise

        time.sleep(RETRY_BACKOFF ** attempt)

    raise RuntimeError(f"All {MAX_RETRIES} retries failed for {url}")


def fetch_weather_for_hub(hub: dict) -> dict:
    """
    Fetches current weather conditions for a single logistics hub.
    Uses Open-Meteo API — free, no API key required.

    Returns current temperature, wind speed, precipitation,
    visibility, and weather condition code.
    """
    params = {
        "latitude":           hub["lat"],
        "longitude":          hub["lng"],
        "current":            [
            "temperature_2m",
            "wind_speed_10m",
            "precipitation",
            "visibility",
            "weather_code",
            "wind_gusts_10m",
            "snowfall",
        ],
        "wind_speed_unit":    "kmh",
        "timezone":           "Europe/Berlin",
    }

    data = _request_with_retry(WEATHER_BASE_URL, params=params)
    current = data.get("current", {})

    return {
        "hub_id":        hub["id"],
        "temp_c":        current.get("temperature_2m"),
        "wind_kmh":      current.get("wind_speed_10m"),
        "wind_gusts_kmh": current.get("wind_gusts_10m"),
        "precipitation": current.get("precipitation"),
        "snowfall":      current.get("snowfall"),
        "visibility_m":  current.get("visibility"),
        "weather_code":  current.get("weather_code"),
    }


def fetch_all_hub_weather() -> dict:
    """
    Fetches weather for all logistics hubs.
    Returns a dict keyed by hub_id for fast lookup.
    """
    weather_map = {}
    for hub in LOGISTICS_HUBS:
        try:
            weather = fetch_weather_for_hub(hub)
            weather_map[hub["id"]] = weather
            logger.info(f"  Weather fetched for {hub['name']} ({hub['id']})")
        except Exception as e:
            logger.error(f"Failed to fetch weather for {hub['id']}: {e}")
            weather_map[hub["id"]] = None

    return weather_map


def fetch_country_data(country_codes: list[str]) -> dict:
    """
    Fetches country metadata for all countries involved in corridors.
    Returns a dict keyed by 2-letter country code.
    """
    url = f"{COUNTRIES_BASE_URL}/alpha"
    params = {"codes": ",".join(country_codes)}

    try:
        countries = _request_with_retry(url, params=params)
        return {
            c["cca2"]: {
                "name":    c["name"]["common"],
                "region":  c.get("subregion", "Europe"),
                "flag":    c.get("flag", ""),
            }
            for c in countries
        }
    except Exception as e:
        logger.warning(f"Could not fetch country data: {e}")
        return {}
