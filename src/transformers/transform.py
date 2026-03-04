"""
Corridor Risk Transformer — v5
--------------------------------
Full corridor assessment across current + forecast time windows.

New in v5:
  - assess_corridor_risk() now returns a 'timeline' with risk at
    NOW, +6h, +12h, +24h for every corridor
  - Deterioration detection: flag corridors that are currently
    clear or medium but will worsen within 24h
  - High-risk area threshold remains severity-gated
"""

import json
import logging
from datetime import datetime, timezone
from collections import defaultdict
from src.api.client import LOGISTICS_HUBS, ROAD_CORRIDORS, FORECAST_HOURS
from src.parsers.validator import HubWeather

logger     = logging.getLogger(__name__)
HUB_LOOKUP = {h["id"]: h for h in LOGISTICS_HUBS}
PRIORITY   = {"high": 3, "medium": 2, "low": 1}

HIGH_RISK_AREA_SEVERE_FLAGS = {
    "severe_freeze", "heavy_snowfall", "severe_wind",
    "severe_low_visibility", "extreme_weather_event",
}


def _effective_risk(point_type: str, disruptions: list[str], raw_risk: str) -> str:
    """Severity-gate high-risk area waypoints."""
    if point_type != "high_risk" or raw_risk != "high":
        return raw_risk
    return "high" if any(f in HIGH_RISK_AREA_SEVERE_FLAGS for f in disruptions) else "medium"


def _weather_to_hub(raw: dict) -> HubWeather | None:
    """Safely convert a raw weather dict to a HubWeather object."""
    if raw is None:
        return None
    try:
        return HubWeather(**raw)
    except Exception:
        return None


def _assess_points_at_time(
    origin_hw:   HubWeather | None,
    dest_hw:     HubWeather | None,
    waypoints:   list[dict],
    weather_key: str,                  # "weather" for current, or forecast hour int
) -> tuple[str, dict | None, list[str]]:
    """
    Compute overall risk, worst point, and disruptions for a
    single time window across all corridor points.

    weather_key: "weather" for current conditions,
                 or an int (6, 12, 24) for forecast hours.
    """
    points = []

    def add_endpoint(hw, name, lat, lng):
        if hw:
            points.append({
                "name":        name,
                "type":        "endpoint",
                "risk_level":  hw.risk_level,
                "disruptions": hw.disruptions,
                "weather":     _format_weather(hw),
                "lat":         lat,
                "lng":         lng,
                "risk_note":   None,
            })

    add_endpoint(origin_hw,
                 HUB_LOOKUP.get(list(HUB_LOOKUP.keys())[0], {}).get("name", "Origin"),
                 None, None)
    add_endpoint(dest_hw,
                 HUB_LOOKUP.get(list(HUB_LOOKUP.keys())[0], {}).get("name", "Destination"),
                 None, None)

    for wp in waypoints:
        if weather_key == "weather":
            raw = wp.get("weather")
        else:
            raw = wp.get("forecast", {}).get(weather_key)

        hw = _weather_to_hub(raw)
        if hw is None:
            continue

        raw_risk  = hw.risk_level
        effective = _effective_risk(wp["type"], hw.disruptions, raw_risk)

        points.append({
            "name":        wp["name"],
            "type":        wp["type"],
            "risk_note":   wp.get("risk_note"),
            "risk_level":  effective,
            "disruptions": hw.disruptions,
            "weather":     _format_weather(hw),
            "lat":         wp["lat"],
            "lng":         wp["lng"],
        })

    if not points:
        return "low", None, []

    worst           = max(points, key=lambda p: PRIORITY.get(p["risk_level"], 0))
    overall         = worst["risk_level"]
    all_disruptions = list(set(d for p in points for d in p["disruptions"]))
    return overall, worst, all_disruptions


def assess_corridor_risk(
    corridor:         dict,
    hub_weather_map:  dict,
    waypoint_weather: list[dict],
    country_data:     dict,
) -> dict:
    """
    Full corridor risk assessment with forecast timeline.

    hub_weather_map values are now dicts with 'current' and 'forecast' keys.
    """
    origin_id  = corridor["origin"]
    dest_id    = corridor["destination"]
    origin_hub = HUB_LOOKUP.get(origin_id, {})
    dest_hub   = HUB_LOOKUP.get(dest_id, {})

    # Pull current weather objects
    o_data = hub_weather_map.get(origin_id) or {}
    d_data = hub_weather_map.get(dest_id) or {}

    origin_current = _weather_to_hub(o_data.get("current") if isinstance(o_data, dict) else None)
    dest_current   = _weather_to_hub(d_data.get("current") if isinstance(d_data, dict) else None)

    # ── Current assessment ────────────────────────────────────────────────────
    # Build full all_points for display
    all_points = []

    if origin_current:
        all_points.append({
            "name":        origin_hub.get("name", origin_id),
            "type":        "endpoint",
            "risk_note":   None,
            "risk_level":  origin_current.risk_level,
            "disruptions": origin_current.disruptions,
            "weather":     _format_weather(origin_current),
            "lat":         origin_hub.get("lat"),
            "lng":         origin_hub.get("lng"),
        })

    for wp in waypoint_weather:
        hw = _weather_to_hub(wp.get("weather"))
        if hw is None:
            continue
        effective = _effective_risk(wp["type"], hw.disruptions, hw.risk_level)
        all_points.append({
            "name":        wp["name"],
            "type":        wp["type"],
            "risk_note":   wp.get("risk_note"),
            "risk_level":  effective,
            "raw_risk":    hw.risk_level,
            "disruptions": hw.disruptions,
            "weather":     _format_weather(hw),
            "lat":         wp["lat"],
            "lng":         wp["lng"],
        })

    if dest_current:
        all_points.append({
            "name":        dest_hub.get("name", dest_id),
            "type":        "endpoint",
            "risk_note":   None,
            "risk_level":  dest_current.risk_level,
            "disruptions": dest_current.disruptions,
            "weather":     _format_weather(dest_current),
            "lat":         dest_hub.get("lat"),
            "lng":         dest_hub.get("lng"),
        })

    worst_now   = max(all_points, key=lambda p: PRIORITY.get(p["risk_level"], 0)) if all_points else None
    current_risk = worst_now["risk_level"] if worst_now else "low"
    all_disruptions = list(set(d for p in all_points for d in p["disruptions"]))

    # ── Forecast timeline ─────────────────────────────────────────────────────
    timeline = {"now": current_risk}
    for h in FORECAST_HOURS:
        o_forecast = _weather_to_hub(
            (o_data.get("forecast") or {}).get(h) if isinstance(o_data, dict) else None
        )
        d_forecast = _weather_to_hub(
            (d_data.get("forecast") or {}).get(h) if isinstance(d_data, dict) else None
        )
        risk_h, _, _ = _assess_points_at_time(
            o_forecast, d_forecast, waypoint_weather, h
        )
        timeline[f"+{h}h"] = risk_h

    # ── Deterioration detection ───────────────────────────────────────────────
    # Deteriorating = corridor is currently clear or medium but gets WORSE
    # within 24h. This is the actionable early warning.
    future_risks    = [PRIORITY.get(timeline[f"+{h}h"], 0) for h in FORECAST_HOURS]
    current_priority = PRIORITY.get(current_risk, 0)
    max_future       = max(future_risks) if future_risks else 0

    deteriorating    = max_future > current_priority
    # Find the first hour where it gets worse
    deterioration_at = None
    if deteriorating:
        for h in FORECAST_HOURS:
            if PRIORITY.get(timeline[f"+{h}h"], 0) > current_priority:
                deterioration_at = h
                break

    # Country info
    origin_country = country_data.get(origin_hub.get("country", ""), {})
    dest_country   = country_data.get(dest_hub.get("country", ""), {})

    return {
        "corridor_id":        corridor["id"],
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
        "distance_km":        corridor["distance_km"],
        "risk_level":         current_risk,
        "disruptions":        all_disruptions,
        "worst_point":        worst_now,
        "all_points":         all_points,
        "waypoint_count":     len(all_points),
        "timeline":           timeline,
        "deteriorating":      deteriorating,
        "deterioration_at":   deterioration_at,
        "recommendation":     _build_recommendation(current_risk, all_disruptions, worst_now, deteriorating, deterioration_at),
        "weather": {
            "origin":      _format_weather(origin_current) if origin_current else {"available": False},
            "destination": _format_weather(dest_current)   if dest_current   else {"available": False},
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


def _build_recommendation(
    risk: str, disruptions: list[str],
    worst: dict | None, deteriorating: bool, deterioration_at: int | None
) -> str:
    location = f" at {worst['name']}" if worst and worst["risk_level"] != "low" else ""

    det_suffix = ""
    if deteriorating and deterioration_at:
        det_suffix = f" Conditions expected to worsen within {deterioration_at}h — act now."

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
            return f"Black ice risk{location}. Reduce speed, increase following distance.{det_suffix}"
        if "moderate_snowfall" in disruptions:
            return f"Snow possible{location}. Ensure vehicle is winter-ready.{det_suffix}"
        return f"Adverse conditions{location}. Advise driver to monitor weather updates.{det_suffix}"
    if deteriorating:
        return f"Currently clear but conditions will deteriorate within {deterioration_at}h. Brief drivers before departure.{det_suffix}"
    return "Conditions normal across full corridor. No action required."


def build_summary(corridors: list[dict], weather_map: dict) -> dict:
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

    hub_currents = []
    for v in weather_map.values():
        if isinstance(v, dict) and v.get("current"):
            hw = _weather_to_hub(v["current"])
            if hw and hw.temp_c is not None:
                hub_currents.append(hw.temp_c)

    deteriorating_count = sum(1 for c in corridors if c.get("deteriorating"))
    hr_triggers         = sum(
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
        "avg_temp_across_network_c": round(sum(hub_currents) / len(hub_currents), 1) if hub_currents else None,
        "hubs_monitored":            len(weather_map),
        "high_risk_area_triggers":   hr_triggers,
        "deteriorating_corridors":   deteriorating_count,
    }


def to_dashboard_json(corridors, weather_map, output_path="data/processed/road_risk_dashboard.json"):
    risk_order = {"high": 0, "medium": 1, "low": 2}
    output = {
        "meta": {
            "pipeline_version":  "5.0.0",
            "generated_at":      datetime.now(timezone.utc).isoformat(),
            "data_sources":      ["Open-Meteo (live + 24h forecast)", "REST Countries", "Nominatim (geocoding)"],
            "corridors_assessed": len(corridors),
            "hubs_monitored":    len(weather_map),
            "coverage":          "European Road Network",
            "forecast_windows":  ["now", "+6h", "+12h", "+24h"],
        },
        "summary":   build_summary(corridors, weather_map),
        "corridors": sorted(corridors, key=lambda c: risk_order.get(c["risk_level"], 3)),
    }
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    logger.info(f"Output: {output_path}")
    return output
