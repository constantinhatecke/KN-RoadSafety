"""
Corridor Risk Transformer
--------------------------
Combines validated hub weather data with corridor definitions
to produce a risk assessment for every road logistics route.

This is the core business logic layer — it answers the question:
"Which routes should operations managers be worried about right now?"

Output is structured dashboard-ready JSON optimized for:
  - Operations morning briefings
  - Automated alerting systems
  - Route planning tools
  - KPI dashboards
"""

import json
import logging
from datetime import datetime, timezone
from collections import defaultdict
from src.api.client import LOGISTICS_HUBS, ROAD_CORRIDORS
from src.parsers.validator import HubWeather

logger = logging.getLogger(__name__)

# Build hub lookup for fast access
HUB_LOOKUP = {h["id"]: h for h in LOGISTICS_HUBS}


def assess_corridor_risk(
    corridor: dict,
    weather_map: dict[str, HubWeather],
    country_data: dict
) -> dict:
    """
    Assesses the disruption risk for a single road corridor.

    Risk is determined by the WORSE of the two endpoint conditions —
    a corridor is only as safe as its most dangerous end.
    In reality you'd also factor in waypoint weather along the route,
    but this models the core logic correctly.
    """
    origin_id = corridor["origin"]
    dest_id = corridor["destination"]

    origin_hub = HUB_LOOKUP.get(origin_id, {})
    dest_hub = HUB_LOOKUP.get(dest_id, {})
    origin_weather = weather_map.get(origin_id)
    dest_weather = weather_map.get(dest_id)

    # Combine disruptions from both ends
    all_disruptions = []
    if origin_weather:
        all_disruptions.extend(origin_weather.disruptions)
    if dest_weather:
        all_disruptions.extend(dest_weather.disruptions)
    unique_disruptions = list(set(all_disruptions))

    # Overall corridor risk = worst of both endpoints
    risk_levels = ["low"]
    if origin_weather:
        risk_levels.append(origin_weather.risk_level)
    if dest_weather:
        risk_levels.append(dest_weather.risk_level)

    priority = {"high": 3, "medium": 2, "low": 1}
    overall_risk = max(risk_levels, key=lambda r: priority.get(r, 0))

    # Build recommendation based on risk
    recommendation = _build_recommendation(overall_risk, unique_disruptions)

    # Get country info
    origin_country = country_data.get(origin_hub.get("country", ""), {})
    dest_country = country_data.get(dest_hub.get("country", ""), {})

    return {
        "corridor_id":    corridor["id"],
        "origin": {
            "hub_id":      origin_id,
            "city":        origin_hub.get("name", "Unknown"),
            "country_code": origin_hub.get("country", ""),
            "country_name": origin_country.get("name", ""),
            "flag":        origin_country.get("flag", ""),
        },
        "destination": {
            "hub_id":      dest_id,
            "city":        dest_hub.get("name", "Unknown"),
            "country_code": dest_hub.get("country", ""),
            "country_name": dest_country.get("name", ""),
            "flag":        dest_country.get("flag", ""),
        },
        "distance_km":    corridor["distance_km"],
        "risk_level":     overall_risk,
        "disruptions":    unique_disruptions,
        "recommendation": recommendation,
        "weather": {
            "origin":      _format_weather(origin_weather),
            "destination": _format_weather(dest_weather),
        },
    }


def _format_weather(hw: HubWeather | None) -> dict:
    """Formats a HubWeather object into a clean dashboard-ready dict."""
    if hw is None:
        return {"available": False}
    return {
        "available":     True,
        "condition":     hw.condition,
        "temp_c":        hw.temp_c,
        "wind_kmh":      hw.wind_kmh,
        "wind_gusts_kmh": hw.wind_gusts_kmh,
        "precipitation": hw.precipitation,
        "snowfall":      hw.snowfall,
        "visibility_m":  hw.visibility_m,
        "risk_level":    hw.risk_level,
        "disruptions":   hw.disruptions,
    }


def _build_recommendation(risk: str, disruptions: list[str]) -> str:
    """Generates a human-readable operations recommendation."""
    if risk == "high":
        if "heavy_snowfall" in disruptions or "severe_freeze" in disruptions:
            return "Route closure recommended. Snow chains required. Consider 24h delay."
        if "severe_wind" in disruptions:
            return "High-sided vehicles should avoid this corridor. Reroute via sheltered roads."
        if "severe_low_visibility" in disruptions:
            return "Visibility critically low. Mandatory speed reduction. Consider delay."
        return "High disruption risk. Contact driver and consider alternate route."
    if risk == "medium":
        if "black_ice_risk" in disruptions:
            return "Black ice risk. Reduce speed, increase following distance."
        if "moderate_snowfall" in disruptions:
            return "Snow possible. Ensure vehicle winter-ready. Monitor conditions."
        return "Adverse conditions expected. Advise driver to monitor weather updates."
    return "Conditions normal. No action required."


def build_summary(corridors: list[dict], weather_map: dict[str, HubWeather]) -> dict:
    """Builds aggregated KPIs for the dashboard summary panel."""
    by_risk = defaultdict(int)
    by_country = defaultdict(int)
    all_disruptions = []

    for c in corridors:
        by_risk[c["risk_level"]] += 1
        by_country[c["origin"]["country_code"]] += 1
        all_disruptions.extend(c["disruptions"])

    # Most common disruption type
    disruption_counts = defaultdict(int)
    for d in all_disruptions:
        disruption_counts[d] += 1
    most_common = max(disruption_counts, key=disruption_counts.get) if disruption_counts else None

    # Most affected country (most high-risk corridors originating there)
    high_risk = [c for c in corridors if c["risk_level"] == "high"]
    affected_countries = defaultdict(int)
    for c in high_risk:
        affected_countries[c["origin"]["country_code"]] += 1
    most_affected = max(affected_countries, key=affected_countries.get) if affected_countries else None

    # Average temperature across all hubs
    temps = [hw.temp_c for hw in weather_map.values() if hw and hw.temp_c is not None]
    avg_temp = round(sum(temps) / len(temps), 1) if temps else None

    return {
        "total_corridors":        len(corridors),
        "corridors_at_high_risk": by_risk.get("high", 0),
        "corridors_at_medium_risk": by_risk.get("medium", 0),
        "corridors_clear":        by_risk.get("low", 0),
        "risk_breakdown":         dict(by_risk),
        "most_common_disruption": most_common,
        "most_affected_country":  most_affected,
        "avg_temp_across_network_c": avg_temp,
        "hubs_monitored":         len(weather_map),
    }


def to_dashboard_json(
    corridors: list[dict],
    weather_map: dict[str, HubWeather],
    output_path: str = "data/processed/road_risk_dashboard.json"
) -> dict:
    """
    Assembles and writes the final dashboard-ready JSON output.
    Sorted by risk level (high first) for immediate ops visibility.
    """
    risk_order = {"high": 0, "medium": 1, "low": 2}
    sorted_corridors = sorted(corridors, key=lambda c: risk_order.get(c["risk_level"], 3))

    summary = build_summary(corridors, weather_map)

    output = {
        "meta": {
            "pipeline_version":   "2.0.0",
            "generated_at":       datetime.now(timezone.utc).isoformat(),
            "data_sources":       ["Open-Meteo (live weather)", "REST Countries"],
            "update_frequency":   "hourly",
            "corridors_assessed": len(corridors),
            "hubs_monitored":     len(weather_map),
            "coverage":           "European Road Network",
        },
        "summary":   summary,
        "corridors": sorted_corridors,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    logger.info(f"Dashboard JSON written to: {output_path}")
    return output
