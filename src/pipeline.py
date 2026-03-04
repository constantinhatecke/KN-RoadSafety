"""
Road Logistics Weather Risk Pipeline — Orchestrator
-----------------------------------------------------
Fetches live weather for 12 major European logistics hubs,
assesses disruption risk for 12 key road corridors,
and outputs a dashboard-ready JSON for operations teams.

Usage:
  python3 -m src.pipeline
  python3 -m src.pipeline --output custom_output.json
"""

import logging
import argparse
import os

from src.api.client import (
    fetch_all_hub_weather,
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
    parser = argparse.ArgumentParser(description="K+N Road Logistics Weather Risk Pipeline")
    parser.add_argument(
        "--output", type=str,
        default="data/processed/road_risk_dashboard.json",
        help="Output path for dashboard JSON"
    )
    return parser.parse_args()


def run(output_path: str = "data/processed/road_risk_dashboard.json"):
    logger.info("=" * 65)
    logger.info("  K+N ROAD LOGISTICS — WEATHER RISK PIPELINE")
    logger.info("=" * 65)

    # ── Step 1: Fetch live weather for all hubs ────────────────────
    logger.info(f"STEP 1/4 | Fetching live weather for {len(LOGISTICS_HUBS)} logistics hubs...")
    raw_weather = fetch_all_hub_weather()
    logger.info(f"          Weather data received for {len(raw_weather)} hubs")

    # ── Step 2: Validate and classify hub weather ──────────────────
    logger.info("STEP 2/4 | Validating and classifying weather conditions...")
    weather_map = validate_hub_weather(raw_weather)
    high_risk_hubs = [h for h, w in weather_map.items() if w and w.risk_level == "high"]
    logger.info(f"          {len(weather_map)} hubs validated | {len(high_risk_hubs)} at high risk")

    # ── Step 3: Fetch country metadata ────────────────────────────
    logger.info("STEP 3/4 | Fetching country metadata...")
    country_codes = list(set(h["country"] for h in LOGISTICS_HUBS))
    country_data = fetch_country_data(country_codes)
    logger.info(f"          Metadata fetched for {len(country_data)} countries")

    # ── Step 4: Assess corridor risk & write output ────────────────
    logger.info(f"STEP 4/4 | Assessing risk for {len(ROAD_CORRIDORS)} road corridors...")
    corridors = [
        assess_corridor_risk(c, weather_map, country_data)
        for c in ROAD_CORRIDORS
    ]

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    result = to_dashboard_json(corridors, weather_map, output_path)

    # ── Summary ────────────────────────────────────────────────────
    s = result["summary"]
    logger.info("=" * 65)
    logger.info("  PIPELINE COMPLETE — NETWORK STATUS")
    logger.info(f"  Hubs monitored   : {s['hubs_monitored']}")
    logger.info(f"  Corridors assessed: {s['total_corridors']}")
    logger.info(f"  🔴 High risk      : {s['corridors_at_high_risk']}")
    logger.info(f"  🟡 Medium risk    : {s['corridors_at_medium_risk']}")
    logger.info(f"  🟢 Clear          : {s['corridors_clear']}")
    logger.info(f"  Avg network temp  : {s['avg_temp_across_network_c']}°C")
    if s['most_common_disruption']:
        logger.info(f"  Top disruption    : {s['most_common_disruption']}")
    logger.info(f"  Output            : {output_path}")
    logger.info("=" * 65)

    return result


if __name__ == "__main__":
    args = parse_args()
    run(output_path=args.output)
