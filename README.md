# Road Logistics Weather Risk Pipeline

A real-time data pipeline that monitors weather disruption risk across 12 major European road logistics corridors. Built to demonstrate operational excellence data engineering relevant to road freight operations.

![Python](https://img.shields.io/badge/python-3.11%2B-blue)

---

## What It Does

1. **Fetches** live weather data for 12 major European logistics hubs (Hamburg, Rotterdam, Warsaw, Milan, Madrid, and more) from the Open-Meteo API — updated hourly, completely free
2. **Validates** all weather readings against a Pydantic schema with WMO weather code interpretation
3. **Classifies** each hub's risk level using real road logistics thresholds (wind speed, temperature, visibility, snowfall)
4. **Assesses** 12 key road corridors by combining origin + destination risk — a corridor is only as safe as its most dangerous endpoint
5. **Outputs** a structured dashboard JSON and an HTML operations dashboard

---

## Business Relevance

This pipeline models a real problem K+N operations teams face daily:

- Weather is the #1 cause of unplanned road freight delays in Europe
- Operations managers need early warning to proactively reroute drivers
- The pipeline output feeds directly into a morning briefing dashboard

---

## Quickstart

```bash
git clone https://github.com/YOUR_USERNAME/logistics-pipeline.git
cd logistics-pipeline
pip install -r requirements.txt
python3 -m src.pipeline
```

Then open `dashboard.html` in your browser to see the live results.

---

## Corridors Monitored

| Corridor | Route | Distance |
|---|---|---|
| HAM-RTD | Hamburg → Rotterdam | 385 km |
| HAM-MUC | Hamburg → Munich | 778 km |
| WAW-BUD | Warsaw → Budapest | 544 km |
| MUC-MIL | Munich → Milan | 415 km |
| PAR-MAD | Paris → Madrid | 1,272 km |
| VIE-BUC | Vienna → Bucharest | 1,284 km |
| + 6 more | | |

---

## Risk Thresholds

| Condition | Medium Risk | High Risk |
|---|---|---|
| Temperature | 0°C to -5°C | Below -10°C |
| Wind Speed | 60+ km/h | 80+ km/h |
| Snowfall | 2+ cm | 5+ cm |
| Precipitation | 5+ mm | 10+ mm |
| Visibility | Under 1,000m | Under 200m |

---

## Tech Stack

- **Python 3.11+**
- **requests** — HTTP client with retry logic
- **Pydantic v2** — weather data validation and schema enforcement
- **Open-Meteo API** — free real-time weather (no API key needed)
- **REST Countries API** — country metadata for corridor enrichment
