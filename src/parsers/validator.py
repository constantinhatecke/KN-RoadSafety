"""
Weather Validator & Risk Classifier
-------------------------------------
Interprets raw weather data and assigns disruption risk levels
to each logistics hub and corridor.

Risk thresholds are based on real road logistics operational standards:
  - Heavy snow/ice → drivers must stop or take alternate routes
  - High winds → trucks (especially curtainsiders) face overturn risk
  - Low visibility → mandatory speed reduction or route closure
  - Sub-zero temps → black ice risk on road surfaces

In a real K+N system these thresholds would come from an
internal risk management database and vary by country/season.
"""

import logging
from typing import Optional
from pydantic import BaseModel, field_validator

logger = logging.getLogger(__name__)


# ── WMO Weather Code Interpretation ──────────────────────────────────────────
# Open-Meteo uses WMO standard weather codes
WMO_CONDITIONS = {
    0:  "clear_sky",
    1:  "mainly_clear", 2: "partly_cloudy", 3: "overcast",
    45: "fog", 48: "rime_fog",
    51: "light_drizzle", 53: "moderate_drizzle", 55: "heavy_drizzle",
    61: "light_rain", 63: "moderate_rain", 65: "heavy_rain",
    71: "light_snow", 73: "moderate_snow", 75: "heavy_snow",
    77: "snow_grains",
    80: "light_showers", 81: "moderate_showers", 82: "violent_showers",
    85: "light_snow_showers", 86: "heavy_snow_showers",
    95: "thunderstorm", 96: "thunderstorm_hail", 99: "thunderstorm_heavy_hail",
}

HIGH_RISK_CONDITIONS = {75, 77, 85, 86, 95, 96, 99, 48}
MEDIUM_RISK_CONDITIONS = {45, 63, 65, 71, 73, 80, 81, 82}


# ── Risk Thresholds ───────────────────────────────────────────────────────────
THRESHOLDS = {
    "temp_freeze":        0.0,    # °C — black ice risk below this
    "temp_severe_freeze": -10.0,  # °C — severe icing, chains required
    "wind_high":          60.0,   # km/h — trucks face stability issues
    "wind_severe":        80.0,   # km/h — route closure recommended
    "precip_medium":      5.0,    # mm — reduced visibility/traction
    "precip_high":        10.0,   # mm — significant disruption
    "snowfall_medium":    2.0,    # cm — snow chains may be required
    "snowfall_high":      5.0,    # cm — route closure likely
    "visibility_low":     1000,   # m — reduced speed mandatory
    "visibility_severe":  200,    # m — route closure recommended
}


class HubWeather(BaseModel):
    """Validated and interpreted weather data for a single logistics hub."""
    hub_id:          str
    temp_c:          Optional[float] = None
    wind_kmh:        Optional[float] = None
    wind_gusts_kmh:  Optional[float] = None
    precipitation:   Optional[float] = None
    snowfall:        Optional[float] = None
    visibility_m:    Optional[float] = None
    weather_code:    Optional[int] = None

    @field_validator("temp_c", "wind_kmh", "precipitation", "snowfall", "visibility_m", mode="before")
    @classmethod
    def coerce_to_float(cls, v):
        if v is None:
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    @property
    def condition(self) -> str:
        return WMO_CONDITIONS.get(self.weather_code, "unknown")

    @property
    def disruptions(self) -> list[str]:
        """Returns list of specific disruption flags for this hub."""
        flags = []
        t = THRESHOLDS

        if self.temp_c is not None:
            if self.temp_c <= t["temp_severe_freeze"]:
                flags.append("severe_freeze")
            elif self.temp_c <= t["temp_freeze"]:
                flags.append("black_ice_risk")

        if self.wind_kmh is not None:
            if self.wind_kmh >= t["wind_severe"]:
                flags.append("severe_wind")
            elif self.wind_kmh >= t["wind_high"]:
                flags.append("high_wind")

        if self.snowfall is not None:
            if self.snowfall >= t["snowfall_high"]:
                flags.append("heavy_snowfall")
            elif self.snowfall >= t["snowfall_medium"]:
                flags.append("moderate_snowfall")

        if self.precipitation is not None:
            if self.precipitation >= t["precip_high"]:
                flags.append("heavy_precipitation")
            elif self.precipitation >= t["precip_medium"]:
                flags.append("moderate_precipitation")

        if self.visibility_m is not None:
            if self.visibility_m <= t["visibility_severe"]:
                flags.append("severe_low_visibility")
            elif self.visibility_m <= t["visibility_low"]:
                flags.append("low_visibility")

        if self.weather_code in HIGH_RISK_CONDITIONS:
            flags.append("extreme_weather_event")
        elif self.weather_code in MEDIUM_RISK_CONDITIONS:
            if "moderate_precipitation" not in flags and "heavy_precipitation" not in flags:
                flags.append("adverse_conditions")

        return flags

    @property
    def risk_level(self) -> str:
        """Derives overall risk level from disruption flags."""
        high_flags = {
            "severe_freeze", "severe_wind", "heavy_snowfall",
            "heavy_precipitation", "severe_low_visibility", "extreme_weather_event"
        }
        medium_flags = {
            "black_ice_risk", "high_wind", "moderate_snowfall",
            "moderate_precipitation", "low_visibility", "adverse_conditions"
        }

        d = set(self.disruptions)
        if d & high_flags:
            return "high"
        if d & medium_flags:
            return "medium"
        return "low"


def validate_hub_weather(raw_weather_map: dict) -> dict[str, HubWeather]:
    """
    Validates raw weather data for all hubs.
    Returns a dict of HubWeather objects keyed by hub_id.
    """
    validated = {}
    for hub_id, raw in raw_weather_map.items():
        if raw is None:
            logger.warning(f"No weather data for hub {hub_id} — skipping")
            continue
        try:
            validated[hub_id] = HubWeather(**raw)
            logger.info(f"  Validated weather for {hub_id}: {validated[hub_id].risk_level} risk")
        except Exception as e:
            logger.error(f"Validation failed for {hub_id}: {e}")

    return validated
