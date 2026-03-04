"""
API Client — Road Logistics Weather Risk Pipeline v5
-----------------------------------------------------
Fetches live + forecast weather from Open-Meteo and
reverse-geocodes waypoint coordinates to town names.

New in v5:
  - fetch_waypoint_weather() now returns current + hourly forecast
  - reverse_geocode() resolves (lat, lng) -> nearest town name
  - Waypoints labeled "WP2 — Cologne" format
"""

import time
import logging
import requests
from typing import Optional

logger = logging.getLogger(__name__)

WEATHER_BASE_URL   = "https://api.open-meteo.com/v1/forecast"
GEOCODE_URL        = "https://nominatim.openstreetmap.org/reverse"
COUNTRIES_BASE_URL = "https://restcountries.com/v3.1"
MAX_RETRIES   = 3
RETRY_BACKOFF = 2

# Forecast hours we care about — what matters operationally
FORECAST_HOURS = [6, 12, 24]


# ── Logistics network definitions ─────────────────────────────────────────────
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
    {"id": "BRENNER",       "name": "Brenner Pass",              "lat": 47.0167, "lng": 11.5000, "elevation_m": 1374, "risk_note": "Critical Alpine crossing — closes in heavy snow",         "applies_to": ["MUC-MIL"]},
    {"id": "PYRENEES",      "name": "Pyrenees (Col du Somport)", "lat": 42.7897, "lng": -0.5314, "elevation_m": 1632, "risk_note": "Only land route FR to ES — prone to closure Oct–Apr",    "applies_to": ["PAR-MAD", "MIL-MAD"]},
    {"id": "TAUERN",        "name": "Tauern Pass",               "lat": 47.1833, "lng": 13.2000, "elevation_m": 1739, "risk_note": "Major AT transit route — tunnel congested in storms",     "applies_to": ["MUC-VIE", "VIE-BUC"]},
    {"id": "CARPATHIANS",   "name": "Carpathian Pass (Dukla)",   "lat": 49.4167, "lng": 21.6833, "elevation_m":  502, "risk_note": "Key Eastern Europe freight crossing — ice risk Oct–Mar",  "applies_to": ["WAW-BUD"]},
    {"id": "TRANSFAGARASAN","name": "Transylvanian Alps",        "lat": 45.6000, "lng": 24.5500, "elevation_m":  900, "risk_note": "Extreme winter conditions — RO mountain section",          "applies_to": ["VIE-BUC", "BUD-BUC"]},
    {"id": "RHINE_VALLEY",  "name": "Rhine Valley (Loreley)",    "lat": 50.1333, "lng":  7.7167, "elevation_m":  100, "risk_note": "Heavy fog corridor — major DE freight artery",             "applies_to": ["HAM-MUC", "RTD-PAR"]},
]


# ── HTTP ──────────────────────────────────────────────────────────────────────
def _request_with_retry(url: str, params: Optional[dict] = None, headers: Optional[dict] = None) -> dict:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
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


# ── Reverse geocoding ─────────────────────────────────────────────────────────
def reverse_geocode(lat: float, lng: float) -> str:
    """
    Resolves a coordinate to a human-readable town/city name.
    Uses Nominatim (OpenStreetMap) — free, no API key required.
    Returns the town/village/city name, falling back to county or country.

    Rate limit: 1 request/second per Nominatim policy.
    We sleep 1.1s between calls in the waypoint loop.
    """
    try:
        headers = {"User-Agent": "KN-RoadLogistics-Dashboard/1.0"}
        data    = _request_with_retry(
            GEOCODE_URL,
            params={"lat": lat, "lon": lng, "format": "json", "zoom": 10},
            headers=headers,
        )
        addr = data.get("address", {})
        # Priority: town > village > city > municipality > county
        name = (
            addr.get("town")
            or addr.get("village")
            or addr.get("city")
            or addr.get("municipality")
            or addr.get("county")
            or addr.get("state")
            or "Unknown"
        )
        time.sleep(1.1)   # Nominatim rate limit
        return name
    except Exception as e:
        logger.warning(f"Reverse geocode failed for ({lat}, {lng}): {e}")
        return "Unknown"


# ── Weather fetching ──────────────────────────────────────────────────────────
def _fetch_weather_at_coord(lat: float, lng: float) -> dict:
    """
    Fetches current weather AND hourly forecast at a coordinate.
    Returns a dict with 'current' and 'forecast' keys.

    Forecast structure: {6: {...weather fields...}, 12: {...}, 24: {...}}
    Each weather dict is compatible with HubWeather schema.
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

    # Current weather
    current_weather = {
        "hub_id":         f"{lat},{lng}",
        "temp_c":         current.get("temperature_2m"),
        "wind_kmh":       current.get("wind_speed_10m"),
        "wind_gusts_kmh": current.get("wind_gusts_10m"),
        "precipitation":  current.get("precipitation"),
        "snowfall":       current.get("snowfall"),
        "visibility_m":   current.get("visibility"),
        "weather_code":   current.get("weather_code"),
    }

    # Hourly forecast slices — index 6 = +6h from now, etc.
    # Open-Meteo returns hourly arrays starting from current hour
    forecast = {}
    for h in FORECAST_HOURS:
        idx = h  # index h = h hours from now
        try:
            forecast[h] = {
                "hub_id":         f"{lat},{lng}",
                "temp_c":         hourly.get("temperature_2m", [])[idx],
                "wind_kmh":       hourly.get("wind_speed_10m", [])[idx],
                "wind_gusts_kmh": hourly.get("wind_gusts_10m", [])[idx],
                "precipitation":  hourly.get("precipitation", [])[idx],
                "snowfall":       hourly.get("snowfall", [])[idx],
                "visibility_m":   hourly.get("visibility", [])[idx],
                "weather_code":   hourly.get("weather_code", [])[idx],
            }
        except (IndexError, TypeError):
            forecast[h] = None

    return {"current": current_weather, "forecast": forecast}


def fetch_weather_for_hub(hub: dict) -> dict:
    raw = _fetch_weather_at_coord(hub["lat"], hub["lng"])
    raw["current"]["hub_id"] = hub["id"]
    return raw


def fetch_all_hub_weather() -> dict:
    """Returns dict keyed by hub_id, each value has 'current' and 'forecast'."""
    weather_map = {}
    for hub in LOGISTICS_HUBS:
        try:
            weather_map[hub["id"]] = fetch_weather_for_hub(hub)
            logger.info(f"  Hub weather + forecast fetched: {hub['name']}")
        except Exception as e:
            logger.error(f"Failed for hub {hub['id']}: {e}")
            weather_map[hub["id"]] = None
    return weather_map


# ── Waypoint generation ───────────────────────────────────────────────────────
def _interpolate_waypoints(o_lat, o_lng, d_lat, d_lng, n):
    return [
        {
            "lat": round(o_lat + (d_lat - o_lat) * i / (n + 1), 4),
            "lng": round(o_lng + (d_lng - o_lng) * i / (n + 1), 4),
        }
        for i in range(1, n + 1)
    ]


def _n_interval_waypoints(distance_km: int) -> int:
    if distance_km < 400:  return 2
    if distance_km < 700:  return 3
    if distance_km < 1200: return 5
    return 7


def get_corridor_waypoints(corridor: dict) -> list[dict]:
    hub_lookup  = {h["id"]: h for h in LOGISTICS_HUBS}
    origin      = hub_lookup[corridor["origin"]]
    destination = hub_lookup[corridor["destination"]]
    n_points    = _n_interval_waypoints(corridor["distance_km"])

    interval_coords = _interpolate_waypoints(
        origin["lat"], origin["lng"],
        destination["lat"], destination["lng"],
        n_points,
    )

    waypoints = []
    for i, pt in enumerate(interval_coords):
        # Reverse geocode to get town name
        town = reverse_geocode(pt["lat"], pt["lng"])
        waypoints.append({
            **pt,
            "type":      "interval",
            "wp_number": i + 1,
            "name":      f"WP{i+1} — {town}",
            "town":      town,
            "risk_note": None,
        })

    for area in HIGH_RISK_AREAS:
        if corridor["id"] in area["applies_to"]:
            waypoints.append({
                "lat":       area["lat"],
                "lng":       area["lng"],
                "type":      "high_risk",
                "wp_number": None,
                "name":      area["name"],
                "town":      area["name"],
                "risk_note": area["risk_note"],
            })

    return waypoints


def fetch_waypoint_weather(corridor: dict) -> list[dict]:
    """
    Fetches current + forecast weather for all waypoints along a corridor.
    Each result has 'weather' (current) and 'forecast' ({6: ..., 12: ..., 24: ...}).
    """
    waypoints = get_corridor_waypoints(corridor)
    results   = []
    for wp in waypoints:
        try:
            data = _fetch_weather_at_coord(wp["lat"], wp["lng"])
            results.append({**wp, "weather": data["current"], "forecast": data["forecast"]})
            logger.info(f"    {wp['name']} ({wp['lat']:.2f}, {wp['lng']:.2f})")
        except Exception as e:
            logger.warning(f"    Failed: {wp['name']}: {e}")
            results.append({**wp, "weather": None, "forecast": {}})
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
