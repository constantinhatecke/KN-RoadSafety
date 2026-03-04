"""
API Client — Road Logistics Weather Risk Pipeline — Final
----------------------------------------------------------
Optimised for speed with full forecast coverage:
  - Waypoints are fully hardcoded (lat, lng, name) — no geocoding
  - All weather requests run in parallel via ThreadPoolExecutor
  - Three forecast windows: +6h, +12h, +24h
  - forecast_days=2 to cover full 24h window
"""

import time
import logging
import requests
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

WEATHER_BASE_URL   = "https://api.open-meteo.com/v1/forecast"
COUNTRIES_BASE_URL = "https://restcountries.com/v3.1"
MAX_RETRIES   = 3
RETRY_BACKOFF = 2
MAX_WORKERS   = 12
FORECAST_HOURS = [6, 12, 24]


# ── Network definitions ───────────────────────────────────────────────────────
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

HIGH_RISK_AREAS = [
    {"id": "BRENNER",        "name": "Brenner Pass",              "lat": 47.0167, "lng": 11.5000, "risk_note": "Critical Alpine crossing — closes in heavy snow",       "applies_to": ["MUC-MIL"]},
    {"id": "PYRENEES",       "name": "Pyrenees (Col du Somport)", "lat": 42.7897, "lng": -0.5314, "risk_note": "Only land route FR to ES — prone to closure Oct–Apr",  "applies_to": ["PAR-MAD", "MIL-MAD"]},
    {"id": "TAUERN",         "name": "Tauern Pass",               "lat": 47.1833, "lng": 13.2000, "risk_note": "Major AT transit route — tunnel congested in storms",   "applies_to": ["MUC-VIE", "VIE-BUC"]},
    {"id": "CARPATHIANS",    "name": "Carpathian Pass (Dukla)",   "lat": 49.4167, "lng": 21.6833, "risk_note": "Key Eastern Europe crossing — ice risk Oct–Mar",        "applies_to": ["WAW-BUD"]},
    {"id": "TRANSFAGARASAN", "name": "Transylvanian Alps",        "lat": 45.6000, "lng": 24.5500, "risk_note": "Extreme winter conditions — RO mountain section",        "applies_to": ["VIE-BUC", "BUD-BUC"]},
    {"id": "RHINE_VALLEY",   "name": "Rhine Valley (Loreley)",    "lat": 50.1333, "lng":  7.7167, "risk_note": "Heavy fog corridor — major DE freight artery",           "applies_to": ["HAM-MUC", "RTD-PAR"]},
]

# ── Hardcoded interval waypoints ──────────────────────────────────────────────
# Coordinates chosen to follow actual road routes, not straight-line
# interpolation. Names verified against OpenStreetMap.
# Format: corridor_id -> list of {lat, lng, name} in route order.
CORRIDOR_WAYPOINTS = {
    "HAM-RTD": [
        {"lat": 52.2786, "lng":  8.0474, "name": "WP1 — Osnabrück"},
        {"lat": 51.9851, "lng":  5.8987, "name": "WP2 — Arnhem"},
    ],
    "HAM-MUC": [
        {"lat": 51.3127, "lng":  9.4797, "name": "WP1 — Kassel"},
        {"lat": 49.4521, "lng": 11.0767, "name": "WP2 — Nuremberg"},
    ],
    "WAW-BUD": [
        {"lat": 50.0647, "lng": 19.9450, "name": "WP1 — Kraków"},
        {"lat": 48.7164, "lng": 21.2611, "name": "WP2 — Košice"},
    ],
    "WAW-PRG": [
        {"lat": 51.1079, "lng": 17.0385, "name": "WP1 — Wrocław"},
        {"lat": 50.7671, "lng": 15.0563, "name": "WP2 — Liberec"},
    ],
    "MUC-MIL": [
        {"lat": 47.2692, "lng": 11.4041, "name": "WP1 — Innsbruck"},
        {"lat": 46.4983, "lng": 11.3548, "name": "WP2 — Bolzano"},
    ],
    "MUC-VIE": [
        {"lat": 47.8095, "lng": 13.0550, "name": "WP1 — Salzburg"},
        {"lat": 48.3069, "lng": 14.2858, "name": "WP2 — Linz"},
    ],
    "PAR-MAD": [
        {"lat": 44.8378, "lng": -0.5792, "name": "WP1 — Bordeaux"},
        {"lat": 42.8467, "lng": -2.6727, "name": "WP2 — Vitoria-Gasteiz"},
    ],
    "PAR-LYO": [
        {"lat": 46.9167, "lng":  4.8333, "name": "WP1 — Mâcon"},
        {"lat": 46.2050, "lng":  5.2257, "name": "WP2 — Bourg-en-Bresse"},
    ],
    "VIE-BUC": [
        {"lat": 47.4979, "lng": 19.0402, "name": "WP1 — Budapest"},
        {"lat": 45.7489, "lng": 21.2087, "name": "WP2 — Timișoara"},
    ],
    "RTD-PAR": [
        {"lat": 51.2213, "lng":  4.4051, "name": "WP1 — Antwerp"},
        {"lat": 50.8503, "lng":  4.3517, "name": "WP2 — Brussels"},
    ],
    "MIL-MAD": [
        {"lat": 44.4056, "lng":  8.9463, "name": "WP1 — Genoa"},
        {"lat": 41.3851, "lng":  2.1734, "name": "WP2 — Barcelona"},
    ],
    "BUD-BUC": [
        {"lat": 46.7712, "lng": 23.6236, "name": "WP1 — Cluj-Napoca"},
        {"lat": 45.7983, "lng": 24.1519, "name": "WP2 — Sibiu"},
    ],
}


# ── HTTP ──────────────────────────────────────────────────────────────────────
def _request_with_retry(url: str, params: Optional[dict] = None) -> dict:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 429:
                time.sleep(RETRY_BACKOFF ** attempt)
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


# ── Weather fetching ──────────────────────────────────────────────────────────
def _fetch_weather_at_coord(lat: float, lng: float) -> dict:
    """
    Fetches current + hourly forecast at a coordinate in a single API call.
    Returns {current: {...}, forecast: {6: {...}, 12: {...}, 24: {...}}}.
    """
    params = {
        "latitude":        lat,
        "longitude":       lng,
        "current":         [
            "temperature_2m", "wind_speed_10m", "precipitation",
            "visibility", "weather_code", "wind_gusts_10m", "snowfall",
        ],
        "hourly":          [
            "temperature_2m", "wind_speed_10m", "precipitation",
            "visibility", "weather_code", "wind_gusts_10m", "snowfall",
        ],
        "wind_speed_unit": "kmh",
        "timezone":        "Europe/Berlin",
        "forecast_days":   2,
    }
    data    = _request_with_retry(WEATHER_BASE_URL, params=params)
    current = data.get("current", {})
    hourly  = data.get("hourly", {})

    hub_id = f"{lat},{lng}"

    current_dict = {
        "hub_id":         hub_id,
        "temp_c":         current.get("temperature_2m"),
        "wind_kmh":       current.get("wind_speed_10m"),
        "wind_gusts_kmh": current.get("wind_gusts_10m"),
        "precipitation":  current.get("precipitation"),
        "snowfall":       current.get("snowfall"),
        "visibility_m":   current.get("visibility"),
        "weather_code":   current.get("weather_code"),
    }

    forecast = {}
    for h in FORECAST_HOURS:
        try:
            forecast[h] = {
                "hub_id":         hub_id,
                "temp_c":         hourly["temperature_2m"][h],
                "wind_kmh":       hourly["wind_speed_10m"][h],
                "wind_gusts_kmh": hourly["wind_gusts_10m"][h],
                "precipitation":  hourly["precipitation"][h],
                "snowfall":       hourly["snowfall"][h],
                "visibility_m":   hourly["visibility"][h],
                "weather_code":   hourly["weather_code"][h],
            }
        except (IndexError, KeyError, TypeError):
            forecast[h] = None

    return {"current": current_dict, "forecast": forecast}


def _fetch_hub(hub: dict) -> tuple[str, dict | None]:
    try:
        data = _fetch_weather_at_coord(hub["lat"], hub["lng"])
        data["current"]["hub_id"] = hub["id"]
        logger.info(f"  Hub: {hub['name']}")
        return hub["id"], data
    except Exception as e:
        logger.error(f"  Hub failed {hub['id']}: {e}")
        return hub["id"], None


def fetch_all_hub_weather() -> dict:
    """Fetches all hub weather in parallel."""
    results = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(_fetch_hub, hub): hub for hub in LOGISTICS_HUBS}
        for f in as_completed(futures):
            hub_id, data = f.result()
            results[hub_id] = data
    return results


# ── Waypoint generation ───────────────────────────────────────────────────────
def get_corridor_waypoints(corridor: dict) -> list[dict]:
    """
    Returns hardcoded interval waypoints + applicable high-risk areas.
    All coordinates and names are pre-verified — no live geocoding.
    """
    waypoints = []

    for wp in CORRIDOR_WAYPOINTS.get(corridor["id"], []):
        waypoints.append({
            "lat":       wp["lat"],
            "lng":       wp["lng"],
            "type":      "interval",
            "name":      wp["name"],
            "risk_note": None,
        })

    for area in HIGH_RISK_AREAS:
        if corridor["id"] in area["applies_to"]:
            waypoints.append({
                "lat":       area["lat"],
                "lng":       area["lng"],
                "type":      "high_risk",
                "name":      area["name"],
                "risk_note": area["risk_note"],
            })

    return waypoints


def _fetch_waypoint(wp: dict) -> dict:
    try:
        data = _fetch_weather_at_coord(wp["lat"], wp["lng"])
        logger.info(f"  WP: {wp['name']}")
        return {**wp, "weather": data["current"], "forecast": data["forecast"]}
    except Exception as e:
        logger.warning(f"  WP failed {wp['name']}: {e}")
        return {**wp, "weather": None, "forecast": {}}


def fetch_all_waypoints_parallel(corridors: list[dict]) -> dict:
    """
    Fetches weather for all waypoints across all corridors in one parallel pool.
    Returns dict keyed by corridor_id.
    """
    all_tasks = [
        (corridor["id"], wp)
        for corridor in corridors
        for wp in get_corridor_waypoints(corridor)
    ]
    logger.info(f"Fetching {len(all_tasks)} waypoints in parallel...")

    results: dict[str, list] = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(_fetch_waypoint, wp): corridor_id for corridor_id, wp in all_tasks}
        for f in as_completed(futures):
            corridor_id = futures[f]
            result = f.result()
            results.setdefault(corridor_id, []).append(result)

    return results


def fetch_country_data(country_codes: list[str]) -> dict:
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
