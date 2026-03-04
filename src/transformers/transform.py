"""
Corridor Risk Transformer — v3
--------------------------------
Full corridor assessment with smarter high-risk area logic.

Key change from v2:
  - High-risk geographic areas (mountain passes etc.) now require
    SEVERE conditions to escalate a corridor to high risk.
    Rationale: a mountain pass at 2°C with light rain is normal —
    it only becomes operationally relevant when conditions are
    genuinely dangerous (heavy snow, severe wind, ice).
  - Interval waypoints still use standard thresholds.
  - Endpoints (city hubs) still use standard thresholds.
"""

import json
import logging
from datetime import datetime, timezone
from collections import defaultdict
from src.api.client import LOGISTICS_HUBS, ROAD_CORRIDORS
from src.parsers.validator import HubWeather

logger     = logging.getLogger(__name__)
HUB_LOOKUP = {h["id"]: h for h in LOGISTICS_HUBS}
PRIORITY   = {"high": 3, "medium": 2, "low": 1}

# Disruptions that are severe enough to escalate a high-risk AREA to corridor high risk.
# Medium conditions at a mountain pass don't close roads — these do.
HIGH_RISK_AREA_SEVERE_FLAGS = {
    "severe_freeze",
    "heavy_snowfall",
    "severe_wind",
    "severe_low_visibility",
    "extreme_weather_event",
}


def _effective_risk_for_point(point_type: str, disruptions: list[str], raw_risk: str) -> str:
    """
    Applies type-aware risk capping.

    For high_risk area waypoints: only escalate to 'high' if a genuinely
    severe disruption flag is present. Otherwise cap at 'medium' max.
    For all other point types: use the raw risk level as-is.
    """
    if point_type != "high_risk":
        return raw_risk

    # High-risk area: only allow 'high' if a severe flag is present
    if raw_risk == "high":
        if any(f in HIGH_RISK_AREA_SEVERE_FLAGS for f in disruptions):
            return "high"
        else:
            return "medium"   # downgrade — conditions notable but not route-closing

    return raw_risk


def assess_corridor_risk(
    corridor:         dict,
    hub_weather_map:  dict[str, HubWeather],
    waypoint_weather: list[dict],
    country_data:     dict,
) -> dict:
    """
    Full corridor risk assessment using endpoints + all waypoints.

    Assessment logic:
      1. Collect risk from origin hub (standard thresholds)
      2. Collect risk from each waypoint:
           - interval: standard thresholds
           - high_risk area: only HIGH if SEVERE conditions present
      3. Collect risk from destination hub (standard thresholds)
      4. Overall = worst single point
      5. Track which point triggered it
    """
    origin_id  = corridor["origin"]
    dest_id    = corridor["destination"]
    origin_hub = HUB_LOOKUP.get(origin_id, {})
    dest_hub   = HUB_LOOKUP.get(dest_id, {})

    origin_weather = hub_weather_map.get(origin_id)
    dest_weather   = hub_weather_map.get(dest_id)

    all_points = []

    # Origin endpoint
    if origin_weather:
        all_points.append({
            "name":        origin_hub.get("name", origin_id),
            "type":        "endpoint",
            "risk_note":   None,
            "risk_level":  origin_weather.risk_level,
            "disruptions": origin_weather.disruptions,
            "weather":     _format_weather(origin_weather),
            "lat":         origin_hub.get("lat"),
            "lng":         origin_hub.get("lng"),
        })

    # Waypoints
    for wp in waypoint_weather:
        w_raw = wp.get("weather")
        if w_raw is None:
            continue
        try:
            hw          = HubWeather(**w_raw)
            raw_risk    = hw.risk_level
            effective   = _effective_risk_for_point(wp["type"], hw.disruptions, raw_risk)
            all_points.append({
                "name":         wp["name"],
                "type":         wp["type"],
                "risk_note":    wp.get("risk_note"),
                "risk_level":   effective,
                "raw_risk":     raw_risk,        # keep original for transparency
                "disruptions":  hw.disruptions,
                "weather":      _format_weather(hw),
                "lat":          wp["lat"],
                "lng":          wp["lng"],
            })
        except Exception as e:
            logger.warning(f"Could not validate waypoint {wp['name']}: {e}")

    # Destination endpoint
    if dest_weather:
        all_points.append({
            "name":        dest_hub.get("name", dest_id),
            "type":        "endpoint",
            "risk_note":   None,
            "risk_level":  dest_weather.risk_level,
            "disruptions": dest_weather.disruptions,
            "weather":     _format_weather(dest_weather),
            "lat":         dest_hub.get("lat"),
            "lng":         dest_hub.get("lng"),
        })

    # Overall risk
    if not all_points:
        overall_risk    = "low"
        worst_point     = None
        all_disruptions = []
    else:
        worst_point     = max(all_points, key=lambda p: PRIORITY.get(p["risk_level"], 0))
        overall_risk    = worst_point["risk_level"]
        all_disruptions = list(set(d for p in all_points for d in p["disruptions"]))

    origin_country = country_data.get(origin_hub.get("country", ""), {})
    dest_country   = country_data.get(dest_hub.get("country", ""), {})

    return {
        "corridor_id":    corridor["id"],
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
        "distance_km":    corridor["distance_km"],
        "risk_level":     overall_risk,
        "disruptions":    all_disruptions,
        "worst_point":    worst_point,
        "all_points":     all_points,
        "waypoint_count": len(all_points),
        "recommendation": _build_recommendation(overall_risk, all_disruptions, worst_point),
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
    by_risk, all_disrupt, affected = defaultdict(int), [], defaultdict(int)
    for c in corridors:
        by_risk[c["risk_level"]] += 1
        all_disrupt.extend(c["disruptions"])
        if c["risk_level"] == "high":
            affected[c["origin"]["country_code"]] += 1

    disrupt_counts = defaultdict(int)
    for d in all_disrupt:
        disrupt_counts[d] += 1

    temps = [hw.temp_c for hw in weather_map.values() if hw and hw.temp_c is not None]
    hr_triggers = sum(
        1 for c in corridors
        if c.get("worst_point") and c["worst_point"].get("type") == "high_risk"
    )

    return {
        "total_corridors":           len(corridors),
        "corridors_at_high_risk":    by_risk.get("high", 0),
        "corridors_at_medium_risk":  by_risk.get("medium", 0),
        "corridors_clear":           by_risk.get("low", 0),
        "risk_breakdown":            dict(by_risk),
        "most_common_disruption":    max(disrupt_counts, key=disrupt_counts.get) if disrupt_counts else None,
        "most_affected_country":     max(affected, key=affected.get) if affected else None,
        "avg_temp_across_network_c": round(sum(temps) / len(temps), 1) if temps else None,
        "hubs_monitored":            len(weather_map),
        "high_risk_area_triggers":   hr_triggers,
    }


def to_dashboard_json(corridors, weather_map, output_path="data/processed/road_risk_dashboard.json"):
    risk_order = {"high": 0, "medium": 1, "low": 2}
    output = {
        "meta": {
            "pipeline_version":  "3.1.0",
            "generated_at":      datetime.now(timezone.utc).isoformat(),
            "data_sources":      ["Open-Meteo (live weather)", "REST Countries"],
            "corridors_assessed": len(corridors),
            "hubs_monitored":    len(weather_map),
            "coverage":          "European Road Network",
            "assessment_method": "endpoints + interval waypoints + high-risk areas (severity-gated)",
        },
        "summary":   build_summary(corridors, weather_map),
        "corridors": sorted(corridors, key=lambda c: risk_order.get(c["risk_level"], 3)),
    }
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    logger.info(f"Output: {output_path}")
    return output
