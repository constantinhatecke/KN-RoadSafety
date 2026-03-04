"""
API Client — Road Logistics Weather Risk Pipeline
---------------------------------------------------
Fetches live weather data from Open-Meteo (free, no API key)
and country metadata from REST Countries API.

v2 — adds waypoint sampling along corridors:
  - Regular interval waypoints based on corridor distance
  - Hardcoded high-risk geographic areas (mountain passes etc.)
  - Batch weather fetching for all waypoints per corridor
"""

import time
import math
import logging
import requests
from typing import Optional

logger = logging.getLogger(__name__)

WEATHER_BASE_URL   = "https://api.open-meteo.com/v1/forecast"
COUNTRIES_BASE_URL = "https://restcountries.com/v3.1"
MAX_RETRIES    = 3
RETRY_BACKOFF  = 2


# ── Major K+N Road Logistics Hubs ────────────────────────────────────────────
LOGISTICS_HUBS = [
    {"id": "HAM", "name": "Hamburg",   "country": "DE", "lat": 53.5511, "lng":  9.9937},
    {"id": "RTD", "name": "Rotterdam", "country": "NL", "lat": 51.9244, "lng":  4.4777},
    {"id": "WAW", "name": "Warsaw",    "country": "PL", "lat": 52.2297, "lng": 21.0122},
    {"id": "BUD", "name": "Budapest",  "country": "HU", "lat": 47.4979, "lng": 19.0402},
    {"id": "MIL", "name": "Milan",     "country": "IT", "lat": 45.4654, "lng":  9.1859},
    {"id": "MAD", "name": "Madrid",    "country": "ES", "lat": 40.4168, "lng": -3.7038},
    {"id": "PAR", "name": "Paris",     "country": "FR", "lat": 48.8566, "lng":  2.3522},
    {"id": "VIE", "name": "Vienna",    "country": "AT", "lat": 48.2082, "lng": 16.3738},
    {"id": "BUC", "name": "Bucharest", "country": "RO", "lat": 44.4268, "lng": 26.1025},
    {"id": "PRG", "name": "Prague",    "country": "CZ", "lat": 50.0755, "lng": 14.4378},
    {"id": "LYO", "name": "Lyon",      "country": "FR", "lat": 45.7640, "lng":  4.8357},
    {"id": "MUC", "name": "Munich",    "country": "DE", "lat": 48.1351, "lng": 11.5820},
]

# ── Key Road Corridors ────────────────────────────────────────────────────────
ROAD_CORRIDORS = [
    {"id": "HAM-RTD", "origin": "HAM", "destination": "RTD", "distance_km":  385},
    {"id": "HAM-MUC", "origin": "HAM", "destination": "MUC", "distance_km":  778},
    {"id": "WAW-BUD", "origin": "WAW", "destination": "BUD", "distance_km":  544},
    {"id": "WAW-PRG", "origin": "WAW", "destination": "PRG", "distance_km":  517},
    {"id": "MUC-MIL", "origin": "MUC", "destination": "MIL", "distance_km":  415},
    {"id": "MUC-VIE", "origin": "MUC", "destination": "VIE", "distance_km":  456},
    {"id": "PAR-MAD", "origin": "PAR", "destination": "MAD", "distance_km": 1272},
    {"id": "PAR-LYO", "origin": "PAR", "destination": "LYO", "distance_km":  465},
    {"id": "VIE-BUC", "origin": "VIE", "destination": "BUC", "distance_km": 1284},
    {"id": "RTD-PAR", "origin": "RTD", "destination": "PAR", "distance_km":  502},
    {"id": "MIL-MAD", "origin": "MIL", "destination": "MAD", "distance_km": 1858},
    {"id": "BUD-BUC", "origin": "BUD", "destination": "BUC", "distance_km":  812},
]

# ── High-Risk Geographic Areas ────────────────────────────────────────────────
# Known weather hotspots for European road freight.
# Each entry defines which corridors it applies to based on proximity.
# In production these would come from a geospatial database.
HIGH_RISK_AREAS = [
    {
        "id":          "BRENNER",
        "name":        "Brenner Pass",
        "lat":         47.0167,
        "lng":         11.5000,
        "elevation_m": 1374,
        "risk_note":   "Critical Alpine crossing — closes in heavy snow",
        "applies_to":  ["MUC-MIL"],          # lies on this corridor
    },
    {
        "id":          "PYRENEES",
        "name":        "Pyrenees (Col du Somport)",
        "lat":         42.7897,
        "lng":         -0.5314,
        "elevation_m": 1632,
        "risk_note":   "Only land route FR→ES — prone to closure Oct–Apr",
        "applies_to":  ["PAR-MAD", "MIL-MAD"],
    },
    {
        "id":          "TAUERN",
        "name":        "Tauern Pass",
        "lat":         47.1833,
        "lng":         13.2000,
        "elevation_m": 1739,
        "risk_note":   "Major AT transit route — tunnel often congested in storms",
        "applies_to":  ["MUC-VIE", "VIE-BUC"],
    },
    {
        "id":          "CARPATHIANS",
        "name":        "Carpathian Pass (Dukla)",
        "lat":         49.4167,
        "lng":         21.6833,
        "elevation_m": 502,
        "risk_note":   "Key Eastern Europe freight crossing — ice risk Oct–Mar",
        "applies_to":  ["WAW-BUD"],
    },
    {
        "id":          "TRANSFAGARASAN",
        "name":        "Transylvanian Alps",
        "lat":         45.6000,
        "lng":         24.5500,
        "elevation_m": 900,
        "risk_note":   "Extreme winter conditions — RO mountain section",
        "applies_to":  ["VIE-BUC", "BUD-BUC"],
    },
    {
        "id":          "RHINE_VALLEY",
        "name":        "Rhine Valley (Loreley)",
        "lat":         50.1333,
        "lng":         7.7167,
        "elevation_m": 100,
        "risk_note":   "Heavy fog corridor — major DE freight artery",
        "applies_to":  ["HAM-MUC", "RTD-PAR"],
    },
]


# ── Waypoint Generation ───────────────────────────────────────────────────────

def _interpolate_waypoints(
    origin_lat: float, origin_lng: float,
    dest_lat: float,   dest_lng: float,
    n_points: int
) -> list[dict]:
    """
    Generates evenly spaced waypoints along a straight line between
    two coordinates using linear interpolation.

    Note: For short European corridors (<2000km) linear interpolation
    is accurate enough. For longer routes you'd use great-circle
    interpolation to account for Earth's curvature.

    Returns list of {lat, lng} dicts NOT including the endpoints
    (endpoints are already checked as hub weather).
    """
    waypoints = []
    for i in range(1, n_points + 1):
        fraction = i / (n_points + 1)
        lat = origin_lat + (dest_lat - origin_lat) * fraction
        lng = origin_lng + (dest_lng - origin_lng) * fraction
        waypoints.append({"lat": round(lat, 4), "lng": round(lng, 4)})
    return waypoints


def _n_interval_waypoints(distance_km: int) -> int:
    """
    Determines how many interval waypoints to sample based on
    corridor distance. Longer corridors need more samples to
    avoid missing localised weather events.
    """
    if distance_km < 400:
        return 2   # short:  check midpoint + 1 extra
    elif distance_km < 700:
        return 3   # medium: 3 interval points
    elif distance_km < 1200:
        return 5   # long:   5 interval points
    else:
        return 7   # very long (e.g. MIL-MAD at 1858km): 7 points


def get_corridor_waypoints(corridor: dict) -> list[dict]:
    """
    Returns the full list of waypoints to check for a corridor,
    combining regular interval points with known high-risk areas.

    Each waypoint has:
      lat, lng, type ("interval" | "high_risk"), name, risk_note
    """
    hub_lookup  = {h["id"]: h for h in LOGISTICS_HUBS}
    origin      = hub_lookup[corridor["origin"]]
    destination = hub_lookup[corridor["destination"]]
    distance    = corridor["distance_km"]
    n_points    = _n_interval_waypoints(distance)

    # Regular interval waypoints
    interval_coords = _interpolate_waypoints(
        origin["lat"], origin["lng"],
        destination["lat"], destination["lng"],
        n_points
    )
    waypoints = [
        {**pt, "type": "interval", "name": f"Waypoint {i+1}", "risk_note": None}
        for i, pt in enumerate(interval_coords)
    ]

    # High-risk area waypoints — only add if they apply to this corridor
    for area in HIGH_RISK_AREAS:
        if corridor["id"] in area["applies_to"]:
            waypoints.append({
                "lat":       area["lat"],
                "lng":       area["lng"],
                "type":      "high_risk",
                "name":      area["name"],
                "risk_note": area["risk_note"],
            })

    logger.info(
        f"  {corridor['id']}: {len(interval_coords)} interval + "
        f"{sum(1 for w in waypoints if w['type']=='high_risk')} high-risk waypoints"
    )
    return waypoints


# ── Weather Fetching ──────────────────────────────────────────────────────────

def _request_with_retry(url: str, params: Optional[dict] = None) -> dict:
    """GET request with exponential backoff retry logic."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"[Attempt {attempt}] GET {url}")
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 429:
                wait = RETRY_BACKOFF ** attempt
                logger.warning(f"Rate limited — waiting {wait}s")
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


def _fetch_weather_at_coord(lat: float, lng: float) -> dict:
    """Fetches current weather at any coordinate."""
    params = {
        "latitude":        lat,
        "longitude":       lng,
        "current":         [
            "temperature_2m", "wind_speed_10m", "precipitation",
            "visibility", "weather_code", "wind_gusts_10m", "snowfall",
        ],
        "wind_speed_unit": "kmh",
        "timezone":        "Europe/Berlin",
    }
    data    = _request_with_retry(WEATHER_BASE_URL, params=params)
    current = data.get("current", {})
    return {
        "hub_id":         f"{lat},{lng}",
        "temp_c":         current.get("temperature_2m"),
        "wind_kmh":       current.get("wind_speed_10m"),
        "wind_gusts_kmh": current.get("wind_gusts_10m"),
        "precipitation":  current.get("precipitation"),
        "snowfall":       current.get("snowfall"),
        "visibility_m":   current.get("visibility"),
        "weather_code":   current.get("weather_code"),
    }


def fetch_weather_for_hub(hub: dict) -> dict:
    """Fetches current weather for a named logistics hub."""
    raw = _fetch_weather_at_coord(hub["lat"], hub["lng"])
    raw["hub_id"] = hub["id"]
    return raw


def fetch_all_hub_weather() -> dict:
    """Fetches weather for all logistics hubs. Returns dict keyed by hub_id."""
    weather_map = {}
    for hub in LOGISTICS_HUBS:
        try:
            weather_map[hub["id"]] = fetch_weather_for_hub(hub)
            logger.info(f"  Hub weather fetched: {hub['name']}")
        except Exception as e:
            logger.error(f"Failed for hub {hub['id']}: {e}")
            weather_map[hub["id"]] = None
    return weather_map


def fetch_waypoint_weather(corridor: dict) -> list[dict]:
    """
    Fetches weather for all waypoints along a corridor.
    Returns list of waypoint dicts enriched with weather data.
    """
    waypoints = get_corridor_waypoints(corridor)
    results   = []

    for wp in waypoints:
        try:
            weather = _fetch_weather_at_coord(wp["lat"], wp["lng"])
            results.append({**wp, "weather": weather})
            logger.info(f"    ✓ {wp['name']} ({wp['lat']:.2f}, {wp['lng']:.2f})")
        except Exception as e:
            logger.warning(f"    ✗ Failed waypoint {wp['name']}: {e}")
            results.append({**wp, "weather": None})

    return results


def fetch_country_data(country_codes: list[str]) -> dict:
    """Fetches country metadata. Returns dict keyed by 2-letter code."""
    url    = f"{COUNTRIES_BASE_URL}/alpha"
    params = {"codes": ",".join(country_codes)}
    try:
        countries = _request_with_retry(url, params=params)
        return {
            c["cca2"]: {
                "name":   c["name"]["common"],
                "region": c.get("subregion", "Europe"),
                "flag":   c.get("flag", ""),
            }
            for c in countries
        }
    except Exception as e:
        logger.warning(f"Could not fetch country data: {e}")
        return {}
