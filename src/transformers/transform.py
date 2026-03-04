"""
Corridor Risk Transformer — v2 with Waypoint Sampling
-------------------------------------------------------
Assesses disruption risk across the FULL length of each corridor
by combining endpoint weather with intermediate waypoint weather.

Key improvement over v1:
  - v1: risk = worst of 2 endpoint cities
  - v2: risk = worst of ALL points (endpoints + interval waypoints + high-risk areas)

This catches scenarios like clear conditions at both city endpoints
but severe weather at a mountain pass in between.
"""

import json
import logging
from datetime import datetime, timezone
from collections import defaultdict
from src.api.client import LOGISTICS_HUBS, ROAD_CORRIDORS
from src.parsers.validator import HubWeather

logger   = logging.getLogger(__name__)
HUB_LOOKUP = {h["id"]: h for h in LOGISTICS_HUBS}
PRIORITY   = {"high": 3, "medium": 2, "low": 1}


def assess_corridor_risk(
    corridor:        dict,
    hub_weather_map: dict[str, HubWeather],
    waypoint_weather: list[dict],
    country_data:    dict,
) -> dict:
    """
    Full corridor risk assessment using endpoints + all waypoints.

    Assessment logic:
      1. Collect risk from origin hub
      2. Collect risk from each waypoint (interval + high-risk areas)
      3. Collect risk from destination hub
      4. Overall corridor risk = worst single point across all of the above
      5. Track WHICH point triggered the worst risk (for dashboard display)
    """
    origin_id = corridor["origin"]
    dest_id   = corridor["destination"]
    origin_hub = HUB_LOOKUP.get(origin_id, {})
    dest_hub   = HUB_LOOKUP.get(dest_id, {})

    origin_weather = hub_weather_map.get(origin_id)
    dest_weather   = hub_weather_map.get(dest_id)

    # ── Collect all assessment points ─────────────────────────────────────────
    # Each point: {name, risk_level, disruptions, weather_dict, type}
    all_points = []

    # Endpoint: origin
    if origin_weather:
        all_points.append({
            "name":        origin_hub.get("name", origin_id),
            "type":        "endpoint",
            "risk_level":  origin_weather.risk_level,
            "disruptions": origin_weather.disruptions,
            "weather":     _format_weather(origin_weather),
            "lat":         origin_hub.get("lat"),
            "lng":         origin_hub.get("lng"),
        })

    # Waypoints (interval + high-risk areas)
    for wp in waypoint_weather:
        w_raw = wp.get("weather")
        if w_raw is None:
            continue
        try:
            hw = HubWeather(**w_raw)
            all_points.append({
                "name":        wp["name"],
                "type":        wp["type"],          # "interval" or "high_risk"
                "risk_note":   wp.get("risk_note"),
                "risk_level":  hw.risk_level,
                "disruptions": hw.disruptions,
                "weather":     _format_weather(hw),
                "lat":         wp["lat"],
                "lng":         wp["lng"],
            })
        except Exception as e:
            logger.warning(f"Could not validate waypoint {wp['name']}: {e}")

    # Endpoint: destination
    if dest_weather:
        all_points.append({
            "name":        dest_hub.get("name", dest_id),
            "type":        "endpoint",
            "risk_level":  dest_weather.risk_level,
            "disruptions": dest_weather.disruptions,
            "weather":     _format_weather(dest_weather),
            "lat":         dest_hub.get("lat"),
            "lng":         dest_hub.get("lng"),
        })

    # ── Determine overall corridor risk ───────────────────────────────────────
    if not all_points:
        overall_risk     = "low"
        worst_point      = None
        all_disruptions  = []
    else:
        worst_point      = max(all_points, key=lambda p: PRIORITY.get(p["risk_level"], 0))
        overall_risk     = worst_point["risk_level"]
        all_disruptions  = list(set(
            d for p in all_points for d in p["disruptions"]
        ))

    # ── Country info ──────────────────────────────────────────────────────────
    origin_country = country_data.get(origin_hub.get("country", ""), {})
    dest_country   = country_data.get(dest_hub.get("country", ""), {})

    return {
        "corridor_id":   corridor["id"],
        "origin": {
            "hub_id":       origin_id,
            "city":         origin_hub.get("name", "Unknown"),
            "country_code": origin_hub.get("country", ""),
            "country_name": origin_country.get("name", ""),
            "flag":         origin_country.get("flag", ""),
        },
        "destination": {
            "hub_id":       dest_id,
            "city":         dest_hub.get("name", "Unknown"),
            "country_code": dest_hub.get("country", ""),
            "country_name": dest_country.get("name", ""),
            "flag":         dest_country.get("flag", ""),
        },
        "distance_km":       corridor["distance_km"],
        "risk_level":        overall_risk,
        "disruptions":       all_disruptions,
        "worst_point":       worst_point,        # NEW — which point drove the risk
        "all_points":        all_points,         # NEW — full point-by-point breakdown
        "waypoint_count":    len(all_points),    # NEW — total points assessed
        "recommendation":    _build_recommendation(overall_risk, all_disruptions, worst_point),
        # Keep endpoint weather for backwards compatibility
        "weather": {
            "origin":      _format_weather(origin_weather) if origin_weather else {"available": False},
            "destination": _format_weather(dest_weather)   if dest_weather   else {"available": False},
        },
    }


def _format_weather(hw: HubWeather | None) -> dict:
    if hw is None:
        return {"available": False}
    return {
        "available":      True,
        "condition":      hw.condition,
        "temp_c":         hw.temp_c,
        "wind_kmh":       hw.wind_kmh,
        "wind_gusts_kmh": hw.wind_gusts_kmh,
        "precipitation":  hw.precipitation,
        "snowfall":       hw.snowfall,
        "visibility_m":   hw.visibility_m,
        "risk_level":     hw.risk_level,
        "disruptions":    hw.disruptions,
    }


def _build_recommendation(risk: str, disruptions: list[str], worst_point: dict | None) -> str:
    """Generates recommendation, now referencing the specific problem location."""
    location = f" at {worst_point['name']}" if worst_point and worst_point["risk_level"] != "low" else ""

    if risk == "high":
        if "heavy_snowfall" in disruptions or "severe_freeze" in disruptions:
            return f"Route closure recommended{location}. Snow chains required. Consider 24h delay."
        if "severe_wind" in disruptions:
            return f"High-sided vehicles should avoid this corridor{location}. Reroute via sheltered roads."
        if "severe_low_visibility" in disruptions:
            return f"Visibility critically low{location}. Mandatory speed reduction. Consider delay."
        return f"High disruption risk{location}. Contact driver and consider alternate route."
    if risk == "medium":
        if "black_ice_risk" in disruptions:
            return f"Black ice risk{location}. Reduce speed, increase following distance."
        if "moderate_snowfall" in disruptions:
            return f"Snow possible{location}. Ensure vehicle winter-ready. Monitor conditions."
        return f"Adverse conditions{location}. Advise driver to monitor weather updates."
    return "Conditions normal across full corridor. No action required."


def build_summary(corridors: list[dict], weather_map: dict[str, HubWeather]) -> dict:
    by_risk     = defaultdict(int)
    all_disrupt = []
    affected    = defaultdict(int)

    for c in corridors:
        by_risk[c["risk_level"]] += 1
        all_disrupt.extend(c["disruptions"])
        if c["risk_level"] == "high":
            affected[c["origin"]["country_code"]] += 1

    disrupt_counts = defaultdict(int)
    for d in all_disrupt:
        disrupt_counts[d] += 1

    most_common  = max(disrupt_counts, key=disrupt_counts.get) if disrupt_counts else None
    most_affected = max(affected, key=affected.get) if affected else None
    temps = [hw.temp_c for hw in weather_map.values() if hw and hw.temp_c is not None]
    avg_temp = round(sum(temps) / len(temps), 1) if temps else None

    # How many high-risk waypoints were mountain passes / high-risk areas?
    high_risk_area_triggers = sum(
        1 for c in corridors
        if c.get("worst_point") and c["worst_point"].get("type") == "high_risk"
    )

    return {
        "total_corridors":              len(corridors),
        "corridors_at_high_risk":       by_risk.get("high", 0),
        "corridors_at_medium_risk":     by_risk.get("medium", 0),
        "corridors_clear":              by_risk.get("low", 0),
        "risk_breakdown":               dict(by_risk),
        "most_common_disruption":       most_common,
        "most_affected_country":        most_affected,
        "avg_temp_across_network_c":    avg_temp,
        "hubs_monitored":               len(weather_map),
        "high_risk_area_triggers":      high_risk_area_triggers,  # NEW
    }


def to_dashboard_json(
    corridors:    list[dict],
    weather_map:  dict[str, HubWeather],
    output_path:  str = "data/processed/road_risk_dashboard.json"
) -> dict:
    risk_order = {"high": 0, "medium": 1, "low": 2}
    sorted_corridors = sorted(corridors, key=lambda c: risk_order.get(c["risk_level"], 3))
    summary = build_summary(corridors, weather_map)

    output = {
        "meta": {
            "pipeline_version":   "3.0.0",
            "generated_at":       datetime.now(timezone.utc).isoformat(),
            "data_sources":       ["Open-Meteo (live weather)", "REST Countries"],
            "update_frequency":   "on-demand",
            "corridors_assessed": len(corridors),
            "hubs_monitored":     len(weather_map),
            "coverage":           "European Road Network",
            "assessment_method":  "endpoint + interval waypoints + high-risk areas",
        },
        "summary":   summary,
        "corridors": sorted_corridors,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    logger.info(f"Dashboard JSON written to: {output_path}")
    return output
