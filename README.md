# K+N Road Logistics — Weather Risk Dashboard

Live dashboard: [constantinhatecke-kn-roadsafety.streamlit.app](https://constantinhatecke-kn-roadsafety.streamlit.app)

---

## What This Is

A live operations dashboard that monitors weather-driven disruption risk across Kuehne+Nagel's European road logistics network. It is built as a prototype to demonstrate what a data-driven approach to proactive route risk management could look like at an operational level.

The core question it answers is: **should a heavy goods vehicle be dispatched on a given corridor right now, and will conditions along that route deteriorate before the truck arrives?**

Without tooling like this, operations teams typically rely on driver reports, general weather apps, or static regional forecasts — none of which are calibrated to the specific thresholds that matter for heavy transport, and none of which assess intermediate points along a route rather than just the city endpoints.

---

## Use Case

The intended user is a K+N road operations manager running a morning briefing or making dispatch decisions throughout the day. The dashboard surfaces:

- Which corridors are currently at high risk and should not be operated with heavy transport
- Which corridors are currently acceptable but will deteriorate within 6, 12, or 24 hours — enabling proactive driver briefings before departure
- The exact location along each corridor that is driving the risk, rather than a route-level average
- Specific weather values (temperature, wind speed, visibility) so drivers can be briefed with concrete numbers

The 12 monitored corridors cover K+N's major Central and Western European road freight lanes, including Alpine crossings (Munich–Milan via Brenner), Pyrenees crossings (Paris–Madrid), and Eastern European routes (Warsaw–Budapest, Vienna–Bucharest).

---

## How It Works

### Data Sources

| Source | Usage | Cost |
|---|---|---|
| [Open-Meteo](https://open-meteo.com) | Live weather + 24h hourly forecast | Free, no API key |
| [REST Countries](https://restcountries.com) | Country metadata | Free |

### Assessment Architecture

The pipeline runs in four stages on each refresh:

**1. Hub weather** — fetches current conditions and a 24h hourly forecast for all 12 logistics hubs in parallel.

**2. Waypoint sampling** — fetches weather at 4–5 intermediate waypoints per corridor, chosen to follow actual road routes rather than straight-line interpolation. High-risk geographic areas (mountain passes, fog corridors) are hardcoded as additional check points regardless of interval spacing.

**3. Risk classification** — each point is assessed independently using the thresholds below. The corridor's overall risk is the worst single point across all assessed locations, not an average.

**4. Forecast timeline** — steps 1–3 are repeated for the +6h, +12h, and +24h forecast slices. A corridor is flagged as *deteriorating* if its current risk is lower than any future window, triggering a forecast warning banner.

### Risk Thresholds

Thresholds are calibrated to heavy goods vehicle (HGV/TIR) operational standards, not general driving conditions. A temperature of 0°C on a clear road does not close a freight route; -15°C with ice accumulation does.

| Parameter | Medium Risk | High Risk |
|---|---|---|
| Temperature | below -3°C (road surface icing likely) | below -15°C (chains mandatory) |
| Wind speed | above 70 km/h (HGV stability warnings) | above 90 km/h (route closure) |
| Precipitation | above 8 mm/h | above 20 mm/h (aquaplaning risk) |
| Snowfall | above 3 cm/h (chains advisory) | above 8 cm/h (route closure likely) |
| Visibility | below 500m (speed reduction) | below 100m (do not operate) |
| Weather code | WMO moderate risk codes (fog, rain, snow) | WMO severe codes (heavy snow, thunderstorm, hail) |

High-risk geographic areas (Brenner Pass, Pyrenees, Tauern Pass, Carpathian crossings) apply an additional severity gate: they only escalate a corridor to high risk if a genuinely severe flag is present. Normal autumn conditions at altitude — light rain, mild wind — do not close roads and should not trigger operational alerts.

### Monitored Corridors and Waypoints

| Corridor | Distance | Key Waypoints |
|---|---|---|
| Hamburg → Rotterdam | 385 km | Bremen, Osnabrück, Bocholt, Arnhem |
| Hamburg → Munich | 778 km | Hanover, Kassel, Frankfurt, Nuremberg, Ingolstadt |
| Warsaw → Budapest | 544 km | Kraków, Nowy Sącz, Poprad, Košice, Miskolc |
| Warsaw → Prague | 517 km | Wrocław, Wałbrzych, Liberec, Hradec Králové |
| Munich → Milan | 415 km | Rosenheim, Innsbruck, Brenner Pass, Bolzano, Trento |
| Munich → Vienna | 456 km | Salzburg, Bad Ischl, Linz, St. Pölten |
| Paris → Madrid | 1,272 km | Tours, Bordeaux, Toulouse, Vitoria-Gasteiz, Valladolid |
| Paris → Lyon | 465 km | Bourges, Moulins, Mâcon |
| Vienna → Bucharest | 1,284 km | Budapest, Timișoara, Sibiu area, Ploiești |
| Rotterdam → Paris | 502 km | Antwerp, Brussels, Mons, Amiens |
| Milan → Madrid | 1,858 km | Genoa, Marseille, Toulouse, Pyrenees, Barcelona |
| Budapest → Bucharest | 812 km | Cluj-Napoca, Târgu Mureș, Sibiu, Brașov area |

---

## Performance

All weather requests run in parallel using `ThreadPoolExecutor`. A full refresh — 12 hubs, ~50 waypoints, country metadata — completes in approximately 5–8 seconds. The previous sequential implementation with live geocoding took over 10 minutes.

---

## Shortcomings and Honest Limitations

This is a prototype. These are the gaps between what it does and what a production system would require:

**Straight-line waypoint placement.** Waypoints follow actual road cities but the routing between them assumes a roughly linear path. A real system would use a routing API (Google Roads, HERE, TomTom) to fetch actual road geometry and sample weather at points along the true route, including road-level elevation data.

**Weather at a point, not on a road.** Open-Meteo returns weather for a coordinate, not for a road surface. A mountain pass may have significantly different conditions 200m above the coordinates used. Production systems would integrate with road weather station networks where available.

**No live traffic or closure data.** The dashboard assesses weather risk but has no visibility into actual road closures, accidents, or traffic delays. In practice, a high-wind warning might already have closed a route before the dashboard flags it.

**Static corridor definitions.** The 12 corridors are hardcoded. A real TMS integration would pull active routes dynamically, reflecting which corridors actually have vehicles on them at a given time.

**No driver or vehicle context.** Risk thresholds are the same for all vehicle types. A curtainsider in high wind is a different risk profile from a box trailer. A production system would adjust thresholds per vehicle type, load, and driver experience.

**Forecast accuracy degrades past 12 hours.** The +24h forecast window is included for planning purposes but should be treated as directional. Open-Meteo's hourly forecast is reliable for 6–12 hours; beyond that, confidence intervals widen significantly.

**No alerting.** The dashboard requires someone to look at it. A production deployment would push notifications to operations managers and drivers via email, Slack, or a TMS integration when a corridor crosses a risk threshold or a deterioration is detected.

---

## What a Production Version Would Look Like

1. **TMS integration** — pull active routes and vehicle positions from K+N's internal Transport Management System rather than using static corridor definitions
2. **Road weather station data** — supplement forecast data with real-time readings from road surface temperature sensors on motorways, particularly for Alpine routes
3. **Routing API** — use HERE or TomTom to sample weather along the actual driven path with elevation awareness
4. **Push alerting** — Slack/email notifications when corridors change risk level or when deterioration is detected for a corridor with active vehicles
5. **Historical analysis** — track risk events over time to identify which corridors are seasonally problematic and adjust dispatch planning accordingly
6. **Driver app integration** — surface the same risk data directly to drivers via mobile, with turn-by-turn awareness of which upcoming segments are flagged

---

## Tech Stack

- **Python 3.13** — pipeline and data processing
- **Streamlit** — dashboard framework
- **Plotly** — interactive map and charts
- **Pydantic** — weather data validation and schema enforcement
- **Open-Meteo API** — weather data (free tier)
- **ThreadPoolExecutor** — parallel API fetching
- **Streamlit Cloud** — deployment

## Repository Structure

```
kn-roadsafety/
├── app.py                          # Streamlit dashboard
├── src/
│   ├── api/
│   │   └── client.py               # Weather fetching, waypoint definitions
│   ├── parsers/
│   │   └── validator.py            # HubWeather model, risk thresholds
│   └── transformers/
│       └── transform.py            # Corridor risk assessment, forecast logic
├── data/
│   └── processed/                  # Pipeline output JSON
└── requirements.txt
```
