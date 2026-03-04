"""
Corridor Risk Transformer — Final
----------------------------------
Full corridor assessment with:
  - Recommendations that state the actual weather cause (temp, wind etc.)
  - Three forecast windows: NOW / +6h / +12h / +24h
  - Deterioration detection across all three windows
  - Severity-gated high-risk area logic
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

# Human-readable disruption labels for recommendations
DISRUPTION_LABELS = {
    "severe_freeze":         "severe freeze",
    "black_ice_risk":        "black ice risk",
    "severe_wind":           "severe wind",
    "high_wind":             "high wind",
    "heavy_snowfall":        "heavy snowfall",
    "moderate_snowfall":     "snow",
    "heavy_precipitation":   "heavy rain",
    "moderate_precipitation":"moderate rain",
    "severe_low_visibility": "severely reduced visibility",
    "low_visibility":        "reduced visibility",
    "extreme_weather_event": "extreme weather",
    "adverse_conditions":    "adverse conditions",
}


def _effective_risk(point_type: str, disruptions: list[str], raw_risk: str) -> str:
    if point_type != "high_risk" or raw_risk != "high":
        return raw_risk
    return "high" if any(f in HIGH_RISK_AREA_SEVERE_FLAGS for f in disruptions) else "medium"


def _weather_to_hub(raw: dict | None) -> HubWeather | None:
    if not raw:
        return None
    try:
        return HubWeather(**raw)
    except Exception:
        return None


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


def _assess_points_at_time(
    origin_name: str,
    dest_name:   str,
    origin_hw:   HubWeather | None,
    dest_hw:     HubWeather | None,
    waypoints:   list[dict],
    weather_key,   # "weather" (current) or int (forecast hour)
) -> tuple[str, dict | None, list[str]]:
    """Returns (overall_risk, worst_point_dict, all_disruptions) for one time window."""
    points = []

    for hw, name, lat, lng in [(origin_hw, origin_name, None, None), (dest_hw, dest_name, None, None)]:
        if hw:
            points.append({
                "name":        name,
                "type":        "endpoint",
                "risk_level":  hw.risk_level,
                "disruptions": hw.disruptions,
                "weather":     _format_weather(hw),
                "lat": lat, "lng": lng, "risk_note": None,
            })

    for wp in waypoints:
        raw = wp.get("weather") if weather_key == "weather" else (wp.get("forecast") or {}).get(weather_key)
        hw  = _weather_to_hub(raw)
        if not hw:
            continue
        effective = _effective_risk(wp["type"], hw.disruptions, hw.risk_level)
        points.append({
            "name":        wp["name"],
            "type":        wp["type"],
            "risk_note":   wp.get("risk_note"),
            "risk_level":  effective,
            "disruptions": hw.disruptions,
            "weather":     _format_weather(hw),
            "lat": wp["lat"], "lng": wp["lng"],
        })

    if not points:
        return "low", None, []

    worst           = max(points, key=lambda p: PRIORITY.get(p["risk_level"], 0))
    all_disruptions = list(set(d for p in points for d in p["disruptions"]))
    return worst["risk_level"], worst, all_disruptions


def _build_recommendation(
    risk:            str,
    disruptions:     list[str],
    worst:           dict | None,
    deteriorating:   bool,
    deterioration_at: int | None,
    future_worst:    dict | None,   # worst point at the first deterioration window
) -> str:
    """
    Generates an actionable recommendation with the actual weather cause
    and values included, so operations teams know exactly why the risk
    is flagged and can brief drivers specifically.
    """
    # Build location string
    location = f" at {worst['name']}" if worst and worst["risk_level"] != "low" else ""

    # Extract actual weather values from worst point for specificity
    w = (worst or {}).get("weather", {}) if worst else {}
    temp     = w.get("temp_c")
    wind     = w.get("wind_kmh")
    snow     = w.get("snowfall")
    precip   = w.get("precipitation")
    vis      = w.get("visibility_m")

    def temp_str():
        return f" ({temp}°C)" if temp is not None else ""

    def wind_str():
        return f" ({wind} km/h)" if wind is not None else ""

    def vis_str():
        v_km = round(vis / 1000, 1) if vis is not None else None
        return f" ({v_km} km visibility)" if v_km is not None else ""

    def snow_str():
        return f" ({snow} cm/h)" if snow is not None else ""

    # Deterioration suffix
    det_suffix = ""
    if deteriorating and deterioration_at:
        fw     = (future_worst or {})
        fw_loc = f" at {fw['name']}" if fw and fw.get("name") else ""
        det_suffix = f" Forecast deterioration{fw_loc} within {deterioration_at}h — brief drivers before departure."

    if risk == "high":
        if "severe_freeze" in disruptions:
            return f"Severe freeze{location}{temp_str()}. Road surface icing expected. Snow chains mandatory. Delay departure by 24h or reroute."
        if "heavy_snowfall" in disruptions:
            return f"Heavy snowfall{location}{snow_str()}. Route closure likely. Snow chains required. Consider 24h delay."
        if "severe_wind" in disruptions:
            return f"Severe wind{location}{wind_str()}. High-sided vehicles must avoid this corridor. Reroute via sheltered roads."
        if "severe_low_visibility" in disruptions:
            return f"Critically low visibility{location}{vis_str()}. Mandatory speed reduction. Consider delaying departure."
        if "extreme_weather_event" in disruptions:
            return f"Extreme weather event{location}. Do not dispatch. Contact driver immediately if en route."
        if "heavy_precipitation" in disruptions:
            return f"Heavy precipitation{location} ({precip} mm). Reduced traction and visibility. Reduce speed, increase following distance."
        return f"High disruption risk{location}. Contact driver and consider alternate route.{det_suffix}"

    if risk == "medium":
        if "black_ice_risk" in disruptions:
            return f"Black ice risk{location}{temp_str()}. Reduce speed, increase following distance. Avoid braking on descents.{det_suffix}"
        if "moderate_snowfall" in disruptions:
            return f"Snow conditions{location}{snow_str()}. Ensure vehicle is winter-ready. Monitor conditions en route.{det_suffix}"
        if "high_wind" in disruptions:
            return f"High wind{location}{wind_str()}. Caution for high-sided vehicles. Reduce speed on exposed sections.{det_suffix}"
        if "low_visibility" in disruptions:
            return f"Reduced visibility{location}{vis_str()}. Headlights mandatory. Reduce speed on affected sections.{det_suffix}"
        if "moderate_precipitation" in disruptions:
            return f"Moderate precipitation{location} ({precip} mm). Reduced traction expected. Allow extra journey time.{det_suffix}"
        return f"Adverse conditions{location}. Advise driver to monitor weather updates.{det_suffix}"

    if deteriorating and deterioration_at:
        fw     = (future_worst or {})
        fw_loc = f" at {fw['name']}" if fw and fw.get("name") else ""
        fw_w   = fw.get("weather", {}) if fw else {}
        fw_disruptions = fw.get("disruptions", [])
        cause  = DISRUPTION_LABELS.get(fw_disruptions[0], "adverse conditions") if fw_disruptions else "adverse conditions"
        fw_temp = fw_w.get("temp_c")
        detail = f" ({fw_temp}°C)" if fw_temp is not None else ""
        return (
            f"Currently clear. {cause.capitalize()} forecast{fw_loc}{detail} within {deterioration_at}h. "
            f"Brief drivers before departure and monitor conditions."
        )

    return "All clear. Conditions normal across full corridor. No action required."


def assess_corridor_risk(
    corridor:         dict,
    hub_weather_map:  dict,
    waypoint_weather: list[dict],
    country_data:     dict,
) -> dict:
    origin_id  = corridor["origin"]
    dest_id    = corridor["destination"]
    origin_hub = HUB_LOOKUP.get(origin_id, {})
    dest_hub   = HUB_LOOKUP.get(dest_id, {})

    o_data = hub_weather_map.get(origin_id) or {}
    d_data = hub_weather_map.get(dest_id) or {}

    origin_current = _weather_to_hub(o_data.get("current") if isinstance(o_data, dict) else None)
    dest_current   = _weather_to_hub(d_data.get("current") if isinstance(d_data, dict) else None)

    # ── Current full point list ───────────────────────────────────────────────
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

    worst_now       = max(all_points, key=lambda p: PRIORITY.get(p["risk_level"], 0)) if all_points else None
    current_risk    = worst_now["risk_level"] if worst_now else "low"
    all_disruptions = list(set(d for p in all_points for d in p["disruptions"]))

    # ── Forecast timeline: NOW / +6h / +12h / +24h ───────────────────────────
    timeline      = {"now": current_risk}
    timeline_worst = {"now": worst_now}   # worst point at each time window

    for h in FORECAST_HOURS:
        o_fc = _weather_to_hub((o_data.get("forecast") or {}).get(h) if isinstance(o_data, dict) else None)
        d_fc = _weather_to_hub((d_data.get("forecast") or {}).get(h) if isinstance(d_data, dict) else None)
        risk_h, worst_h, _ = _assess_points_at_time(
            origin_hub.get("name", origin_id),
            dest_hub.get("name", dest_id),
            o_fc, d_fc, waypoint_weather, h,
        )
        timeline[f"+{h}h"]       = risk_h
        timeline_worst[f"+{h}h"] = worst_h

    # ── Deterioration detection ───────────────────────────────────────────────
    current_priority = PRIORITY.get(current_risk, 0)
    deteriorating    = False
    deterioration_at = None
    future_worst_pt  = None

    for h in FORECAST_HOURS:
        if PRIORITY.get(timeline.get(f"+{h}h", "low"), 0) > current_priority:
            deteriorating    = True
            deterioration_at = h
            future_worst_pt  = timeline_worst.get(f"+{h}h")
            break

    origin_country = country_data.get(origin_hub.get("country", ""), {})
    dest_country   = country_data.get(dest_hub.get("country", ""), {})

    return {
        "corridor_id":      corridor["id"],
        "origin": {
            "hub_id":       origin_id,
            "city":         origin_hub.get("name", "Unknown"),
            "country_code": origin_hub.get("country", ""),
            "country_name": origin_country.get("name", ""),
        },
        "destination": {
            "hub_id":       dest_id,
            "city":         dest_hub.get("name", "Unknown"),
            "country_code": dest_hub.get("country", ""),
            "country_name": dest_country.get("name", ""),
        },
        "distance_km":      corridor["distance_km"],
        "risk_level":       current_risk,
        "disruptions":      all_disruptions,
        "worst_point":      worst_now,
        "all_points":       all_points,
        "waypoint_count":   len(all_points),
        "timeline":         timeline,
        "timeline_worst":   {k: v for k, v in timeline_worst.items() if v},
        "deteriorating":    deteriorating,
        "deterioration_at": deterioration_at,
        "recommendation":   _build_recommendation(
            current_risk, all_disruptions, worst_now,
            deteriorating, deterioration_at, future_worst_pt,
        ),
        "weather": {
            "origin":      _format_weather(origin_current) if origin_current else {"available": False},
            "destination": _format_weather(dest_current)   if dest_current   else {"available": False},
        },
    }


def build_summary(corridors: list[dict], weather_map: dict) -> dict:
    by_risk, all_disrupt, affected = defaultdict(int), [], defaultdict(int)
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
        "high_risk_area_triggers":   sum(1 for c in corridors if c.get("worst_point") and c["worst_point"].get("type") == "high_risk"),
        "deteriorating_corridors":   sum(1 for c in corridors if c.get("deteriorating")),
    }
