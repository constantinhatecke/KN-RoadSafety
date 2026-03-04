"""
Road Logistics Weather Risk Pipeline — v3 with Waypoint Sampling
-----------------------------------------------------------------
Fetches live weather for 12 hubs + intermediate waypoints along
each corridor, then assesses full-corridor disruption risk.

Usage:
  python3 -m src.pipeline
  python3 -m src.pipeline --output custom_output.json
"""

import logging
import argparse
import os

from src.api.client import (
    fetch_all_hub_weather,
    fetch_waypoint_weather,
    fetch_country_data,
    LOGISTICS_HUBS,
    ROAD_CORRIDORS,
)
from src.parsers.validator import validate_hub_weather
from src.transformers.transform import assess_corridor_risk, to_dashboard_json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("pipeline")


def parse_args():
    parser = argparse.ArgumentParser(description="K+N Road Logistics Weather Risk Pipeline v3")
    parser.add_argument("--output", type=str, default="data/processed/road_risk_dashboard.json")
    return parser.parse_args()


def run(output_path: str = "data/processed/road_risk_dashboard.json"):
    logger.info("=" * 65)
    logger.info("  K+N ROAD LOGISTICS — WEATHER RISK PIPELINE v3")
    logger.info("  Full corridor assessment with waypoint sampling")
    logger.info("=" * 65)

    # ── Step 1: Hub weather ────────────────────────────────────────
    logger.info(f"STEP 1/4 | Fetching weather for {len(LOGISTICS_HUBS)} logistics hubs...")
    raw_weather = fetch_all_hub_weather()
    weather_map = validate_hub_weather(raw_weather)
    logger.info(f"          {len(weather_map)} hubs validated")

    # ── Step 2: Waypoint weather ───────────────────────────────────
    logger.info(f"STEP 2/4 | Sampling waypoints for {len(ROAD_CORRIDORS)} corridors...")
    corridor_waypoints = {}
    total_waypoints = 0
    for corridor in ROAD_CORRIDORS:
        logger.info(f"  Corridor {corridor['id']} ({corridor['distance_km']} km):")
        wp_weather = fetch_waypoint_weather(corridor)
        corridor_waypoints[corridor["id"]] = wp_weather
        total_waypoints += len(wp_weather)
    logger.info(f"          {total_waypoints} total waypoints assessed")

    # ── Step 3: Country metadata ───────────────────────────────────
    logger.info("STEP 3/4 | Fetching country metadata...")
    country_codes = list(set(h["country"] for h in LOGISTICS_HUBS))
    country_data  = fetch_country_data(country_codes)

    # ── Step 4: Assess corridors ───────────────────────────────────
    logger.info(f"STEP 4/4 | Assessing full corridor risk...")
    corridors = [
        assess_corridor_risk(
            corridor    = c,
            hub_weather_map  = weather_map,
            waypoint_weather = corridor_waypoints.get(c["id"], []),
            country_data     = country_data,
        )
        for c in ROAD_CORRIDORS
    ]

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    result = to_dashboard_json(corridors, weather_map, output_path)

    # ── Summary ────────────────────────────────────────────────────
    s = result["summary"]
    logger.info("=" * 65)
    logger.info("  PIPELINE COMPLETE — NETWORK STATUS")
    logger.info(f"  Hubs monitored        : {s['hubs_monitored']}")
    logger.info(f"  Corridors assessed    : {s['total_corridors']}")
    logger.info(f"  Total waypoints       : {total_waypoints}")
    logger.info(f"  🔴 High risk          : {s['corridors_at_high_risk']}")
    logger.info(f"  🟡 Medium risk        : {s['corridors_at_medium_risk']}")
    logger.info(f"  🟢 Clear              : {s['corridors_clear']}")
    logger.info(f"  High-risk area alerts : {s['high_risk_area_triggers']}")
    logger.info(f"  Avg network temp      : {s['avg_temp_across_network_c']}°C")
    logger.info(f"  Output                : {output_path}")
    logger.info("=" * 65)

    return result


if __name__ == "__main__":
    args = parse_args()
    run(output_path=args.output)
