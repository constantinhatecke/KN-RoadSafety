"""
K+N Road Logistics — Live Operations Dashboard
------------------------------------------------
Streamlit app that runs the weather risk pipeline on demand
and visualises results as an interactive operations dashboard.

To run locally:
  streamlit run app.py

To deploy:
  Push to GitHub → connect to streamlit.io → done.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import sys
import os

# Make sure src/ is importable
sys.path.insert(0, os.path.dirname(__file__))

from src.api.client import (
    fetch_all_hub_weather,
    fetch_country_data,
    LOGISTICS_HUBS,
    ROAD_CORRIDORS,
)
from src.parsers.validator import validate_hub_weather
from src.transformers.transform import assess_corridor_risk, build_summary

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="K+N Road Risk Dashboard",
    page_icon="🚛",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap');

  html, body, [class*="css"] {
    font-family: 'IBM Plex Sans', sans-serif;
  }

  .stApp { background-color: #0a0c10; }

  /* Header */
  .dash-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0 0 24px 0;
    border-bottom: 1px solid #1e2330;
    margin-bottom: 28px;
  }

  .dash-title {
    font-size: 22px;
    font-weight: 600;
    color: #ffffff;
    letter-spacing: 0.3px;
  }

  .dash-subtitle {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 11px;
    color: #5a6478;
    letter-spacing: 1.5px;
    text-transform: uppercase;
    margin-top: 3px;
  }

  .live-badge {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: rgba(0,204,136,0.08);
    border: 1px solid rgba(0,204,136,0.25);
    color: #00cc88;
    padding: 5px 12px;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 11px;
    letter-spacing: 1px;
  }

  /* KPI cards */
  .kpi-box {
    background: #111318;
    border: 1px solid #1e2330;
    padding: 20px;
    position: relative;
  }

  .kpi-label {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 10px;
    color: #5a6478;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    margin-bottom: 8px;
  }

  .kpi-value {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 36px;
    font-weight: 600;
    line-height: 1;
  }

  .kpi-sub { font-size: 11px; color: #5a6478; margin-top: 6px; }

  .kpi-high   { color: #ff4455; border-top: 2px solid #ff4455; }
  .kpi-medium { color: #ffaa00; border-top: 2px solid #ffaa00; }
  .kpi-clear  { color: #00cc88; border-top: 2px solid #00cc88; }
  .kpi-total  { color: #ffffff; border-top: 2px solid #4a9eff; }
  .kpi-temp   { color: #aa88ff; border-top: 2px solid #aa88ff; }

  /* Alert */
  .alert-high {
    background: rgba(255,68,85,0.08);
    border: 1px solid rgba(255,68,85,0.3);
    color: #ff4455;
    padding: 12px 18px;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 12px;
    margin-bottom: 20px;
  }

  /* Risk badge */
  .badge-high   { background: rgba(255,68,85,0.1);  color: #ff4455; border: 1px solid rgba(255,68,85,0.3);  padding: 2px 10px; font-size: 11px; font-family: monospace; }
  .badge-medium { background: rgba(255,170,0,0.1);  color: #ffaa00; border: 1px solid rgba(255,170,0,0.3);  padding: 2px 10px; font-size: 11px; font-family: monospace; }
  .badge-low    { background: rgba(0,204,136,0.1);  color: #00cc88; border: 1px solid rgba(0,204,136,0.3); padding: 2px 10px; font-size: 11px; font-family: monospace; }

  /* Section titles */
  .section-title {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 10px;
    color: #5a6478;
    text-transform: uppercase;
    letter-spacing: 2px;
    margin-bottom: 12px;
    padding-bottom: 8px;
    border-bottom: 1px solid #1e2330;
  }

  /* Hide streamlit chrome */
  #MainMenu { visibility: hidden; }
  footer { visibility: hidden; }
  header { visibility: hidden; }
  .block-container { padding-top: 2rem; padding-bottom: 2rem; }
</style>
""", unsafe_allow_html=True)


# ── Pipeline runner (cached for performance) ──────────────────────────────────

@st.cache_data(ttl=0, show_spinner=False)  # ttl=0 means no auto-cache, refresh only on button click
def run_pipeline():
    """Runs the full pipeline and returns structured results."""
    # Step 1: Fetch weather
    raw_weather = fetch_all_hub_weather()

    # Step 2: Validate
    weather_map = validate_hub_weather(raw_weather)

    # Step 3: Country metadata
    country_codes = list(set(h["country"] for h in LOGISTICS_HUBS))
    country_data = fetch_country_data(country_codes)

    # Step 4: Assess corridors
    corridors = [
        assess_corridor_risk(c, weather_map, country_data)
        for c in ROAD_CORRIDORS
    ]

    # Step 5: Summary
    summary = build_summary(corridors, weather_map)

    # Sort by risk
    risk_order = {"high": 0, "medium": 1, "low": 2}
    corridors_sorted = sorted(corridors, key=lambda c: risk_order.get(c["risk_level"], 3))

    return {
        "corridors": corridors_sorted,
        "summary":   summary,
        "weather_map": {k: v.model_dump() if v else None for k, v in weather_map.items()},
        "fetched_at": datetime.utcnow().isoformat(),
    }


# ── Session state init ────────────────────────────────────────────────────────
if "data" not in st.session_state:
    st.session_state.data = None
if "last_fetched" not in st.session_state:
    st.session_state.last_fetched = None


# ── Header ────────────────────────────────────────────────────────────────────
col_title, col_btn = st.columns([5, 1])

with col_title:
    st.markdown("""
    <div class="dash-header">
      <div>
        <div class="dash-title">🚛 K+N Road Logistics</div>
        <div class="dash-subtitle">Weather Risk Dashboard · European Network</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

with col_btn:
    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
    refresh = st.button("⟳ Refresh Live Data", type="primary", use_container_width=True)


# ── Load data on first visit or refresh ──────────────────────────────────────
if st.session_state.data is None or refresh:
    with st.spinner("Fetching live weather across Europe..."):
        try:
            st.cache_data.clear()
            data = run_pipeline()
            st.session_state.data = data
            st.session_state.last_fetched = datetime.utcnow().strftime("%d %b %Y, %H:%M UTC")
        except Exception as e:
            st.error(f"Pipeline error: {e}")
            st.stop()

data = st.session_state.data
corridors = data["corridors"]
summary = data["summary"]

# Last fetched timestamp
if st.session_state.last_fetched:
    st.markdown(
        f'<div style="font-family:monospace;font-size:11px;color:#5a6478;margin-top:-16px;margin-bottom:20px;">'
        f'<span style="color:#00cc88">●</span> Last refreshed: {st.session_state.last_fetched}</div>',
        unsafe_allow_html=True
    )


# ── Alert banner ──────────────────────────────────────────────────────────────
high_count = summary["corridors_at_high_risk"]
if high_count > 0:
    top_disruption = summary.get("most_common_disruption", "")
    disruption_str = f" — Primary disruption: {top_disruption.replace('_', ' ')}" if top_disruption else ""
    st.markdown(
        f'<div class="alert-high">⚠ &nbsp; {high_count} corridor{"s" if high_count > 1 else ""} '
        f'at HIGH RISK{disruption_str}</div>',
        unsafe_allow_html=True
    )


# ── KPI Row ───────────────────────────────────────────────────────────────────
k1, k2, k3, k4, k5 = st.columns(5)

def kpi(col, label, value, sub, css_class):
    col.markdown(
        f'<div class="kpi-box {css_class}">'
        f'<div class="kpi-label">{label}</div>'
        f'<div class="kpi-value">{value}</div>'
        f'<div class="kpi-sub">{sub}</div>'
        f'</div>',
        unsafe_allow_html=True
    )

kpi(k1, "Corridors Monitored", summary["total_corridors"],       "European road network", "kpi-total")
kpi(k2, "High Risk",           summary["corridors_at_high_risk"], "Immediate action",      "kpi-high")
kpi(k3, "Medium Risk",         summary["corridors_at_medium_risk"],"Monitor closely",      "kpi-medium")
kpi(k4, "Clear",               summary["corridors_clear"],         "Normal operations",    "kpi-clear")

avg_temp = summary.get("avg_temp_across_network_c")
temp_str = f"{avg_temp}°C" if avg_temp is not None else "N/A"
kpi(k5, "Avg Network Temp",   temp_str, "Across all hubs", "kpi-temp")

st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)


# ── Main content ──────────────────────────────────────────────────────────────
left, right = st.columns([3, 2])

with left:
    # ── Map ───────────────────────────────────────────────────────────
    st.markdown('<div class="section-title">Corridor Risk Map</div>', unsafe_allow_html=True)

    color_map = {"high": "#ff4455", "medium": "#ffaa00", "low": "#00cc88"}
    hub_lookup = {h["id"]: h for h in LOGISTICS_HUBS}

    # Build map traces — one line per corridor
    fig = go.Figure()

    # Add corridor lines
    for c in corridors:
        o = hub_lookup.get(c["origin"]["hub_id"], {})
        d = hub_lookup.get(c["destination"]["hub_id"], {})
        if not o or not d:
            continue
        color = color_map[c["risk_level"]]
        fig.add_trace(go.Scattergeo(
            lon=[o["lng"], d["lng"], None],
            lat=[o["lat"], d["lat"], None],
            mode="lines",
            line=dict(width=2.5 if c["risk_level"] == "high" else 1.5, color=color),
            hoverinfo="skip",
            showlegend=False,
        ))

    # Add hub markers
    hub_lats = [h["lat"] for h in LOGISTICS_HUBS]
    hub_lngs = [h["lng"] for h in LOGISTICS_HUBS]
    hub_names = [h["name"] for h in LOGISTICS_HUBS]
    hub_ids = [h["id"] for h in LOGISTICS_HUBS]

    fig.add_trace(go.Scattergeo(
        lon=hub_lngs,
        lat=hub_lats,
        mode="markers+text",
        marker=dict(size=8, color="#4a9eff", symbol="circle"),
        text=hub_names,
        textposition="top center",
        textfont=dict(size=10, color="#c8d0e0", family="IBM Plex Mono"),
        hovertemplate="<b>%{text}</b><br>%{lon:.2f}°E, %{lat:.2f}°N<extra></extra>",
        showlegend=False,
    ))

    fig.update_layout(
        geo=dict(
            scope="europe",
            showland=True,
            landcolor="#111318",
            showocean=True,
            oceancolor="#0a0c10",
            showcountries=True,
            countrycolor="#1e2330",
            showframe=False,
            bgcolor="#0a0c10",
            projection_type="natural earth",
        ),
        paper_bgcolor="#0a0c10",
        plot_bgcolor="#0a0c10",
        margin=dict(l=0, r=0, t=0, b=0),
        height=420,
    )

    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    # ── Risk distribution bar chart ────────────────────────────────
    st.markdown('<div class="section-title">Risk Distribution</div>', unsafe_allow_html=True)

    risk_df = pd.DataFrame([
        {"Risk Level": "High",   "Corridors": summary["corridors_at_high_risk"],  "color": "#ff4455"},
        {"Risk Level": "Medium", "Corridors": summary["corridors_at_medium_risk"], "color": "#ffaa00"},
        {"Risk Level": "Clear",  "Corridors": summary["corridors_clear"],           "color": "#00cc88"},
    ])

    fig2 = px.bar(
        risk_df, x="Risk Level", y="Corridors",
        color="Risk Level",
        color_discrete_map={"High": "#ff4455", "Medium": "#ffaa00", "Clear": "#00cc88"},
        text="Corridors",
    )
    fig2.update_layout(
        paper_bgcolor="#0a0c10",
        plot_bgcolor="#111318",
        font=dict(family="IBM Plex Mono", color="#8892a4", size=11),
        showlegend=False,
        height=200,
        margin=dict(l=0, r=0, t=10, b=0),
        xaxis=dict(showgrid=False, tickfont=dict(color="#8892a4")),
        yaxis=dict(showgrid=True, gridcolor="#1e2330", tickfont=dict(color="#8892a4")),
    )
    fig2.update_traces(textposition="outside", textfont_color="#c8d0e0")
    st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False})


with right:
    # ── Corridors table ───────────────────────────────────────────────
    st.markdown('<div class="section-title">Active Corridors</div>', unsafe_allow_html=True)

    badge = {"high": "🔴", "medium": "🟡", "low": "🟢"}

    for c in corridors:
        risk = c["risk_level"]
        with st.expander(
            f"{badge[risk]} {c['origin']['city']} → {c['destination']['city']}  ·  {c['distance_km']:,} km",
            expanded=(risk == "high")
        ):
            r1, r2 = st.columns(2)
            r1.markdown(f"**Corridor ID:** `{c['corridor_id']}`")
            r2.markdown(f"**Risk:** `{risk.upper()}`")

            if c["disruptions"]:
                st.markdown(
                    "**Disruptions:** " +
                    " · ".join([f"`{d.replace('_', ' ')}`" for d in c["disruptions"]])
                )

            st.markdown(
                f'<div style="background:#111318;border-left:3px solid '
                f'{"#ff4455" if risk=="high" else "#ffaa00" if risk=="medium" else "#00cc88"};'
                f'padding:8px 12px;font-size:12px;color:#c8d0e0;margin-top:6px;">'
                f'{c["recommendation"]}</div>',
                unsafe_allow_html=True
            )

            w_col1, w_col2 = st.columns(2)
            for label, side, col in [("Origin", "origin", w_col1), ("Destination", "destination", w_col2)]:
                w = c["weather"][side]
                if w.get("available"):
                    col.markdown(f"**{label}: {c[side]['city']}**")
                    col.markdown(
                        f"🌡 `{w.get('temp_c', 'N/A')}°C` &nbsp; "
                        f"💨 `{w.get('wind_kmh', 'N/A')} km/h` &nbsp; "
                        f"🌧 `{w.get('precipitation', 0)} mm`"
                    )
                    col.markdown(f"Condition: `{w.get('condition', 'unknown').replace('_', ' ')}`")


# ── Hub temperature chart ─────────────────────────────────────────────────────
st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
st.markdown('<div class="section-title">Hub Temperature Overview</div>', unsafe_allow_html=True)

hub_temps = []
seen = set()
for c in corridors:
    for side in ["origin", "destination"]:
        city = c[side]["city"]
        w = c["weather"][side]
        if city not in seen and w.get("available") and w.get("temp_c") is not None:
            hub_temps.append({
                "Hub": city,
                "Temp (°C)": w["temp_c"],
                "Wind (km/h)": w.get("wind_kmh", 0),
                "Condition": w.get("condition", "").replace("_", " "),
            })
            seen.add(city)

if hub_temps:
    temp_df = pd.DataFrame(hub_temps).sort_values("Temp (°C)")
    colors = ["#4a9eff" if t > 0 else "#88aaff" if t > -5 else "#ff4455" for t in temp_df["Temp (°C)"]]

    fig3 = px.bar(
        temp_df, x="Hub", y="Temp (°C)",
        hover_data=["Wind (km/h)", "Condition"],
        text="Temp (°C)",
    )
    fig3.update_traces(marker_color=colors, texttemplate="%{text:.1f}°C", textposition="outside")
    fig3.update_layout(
        paper_bgcolor="#0a0c10",
        plot_bgcolor="#111318",
        font=dict(family="IBM Plex Mono", color="#8892a4", size=11),
        showlegend=False,
        height=260,
        margin=dict(l=0, r=0, t=20, b=0),
        xaxis=dict(showgrid=False, tickangle=-30),
        yaxis=dict(showgrid=True, gridcolor="#1e2330", zeroline=True, zerolinecolor="#2a3040"),
    )
    st.plotly_chart(fig3, use_container_width=True, config={"displayModeBar": False})


# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("""
<div style="border-top:1px solid #1e2330;margin-top:32px;padding-top:16px;
font-family:'IBM Plex Mono',monospace;font-size:10px;color:#5a6478;
display:flex;justify-content:space-between;">
  <span>Data: Open-Meteo (live weather) · REST Countries</span>
  <span>K+N Road Logistics · Operational Excellence · European Network</span>
</div>
""", unsafe_allow_html=True)