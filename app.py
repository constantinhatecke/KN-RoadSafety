"""
K+N Road Logistics — Live Operations Dashboard v3
--------------------------------------------------
Full corridor assessment with waypoint sampling.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import sys, os

sys.path.insert(0, os.path.dirname(__file__))

from src.api.client import (
    fetch_all_hub_weather,
    fetch_waypoint_weather,
    fetch_country_data,
    LOGISTICS_HUBS,
    ROAD_CORRIDORS,
)
from src.parsers.validator import validate_hub_weather, HubWeather
from src.transformers.transform import assess_corridor_risk, build_summary

# ── K+N Brand ─────────────────────────────────────────────────────────────────
KN_NAVY   = "#0D2240"
KN_CARD   = "#162E52"
KN_BORDER = "#1E4070"
KN_BLUE   = "#0099CC"
KN_WHITE  = "#FFFFFF"
KN_LIGHT  = "#C8D8E8"
KN_DIM    = "#7A94AA"
RISK_HIGH = "#E8394A"
RISK_MED  = "#F5A623"
RISK_LOW  = "#27AE60"

st.set_page_config(page_title="K+N Road Risk Dashboard", page_icon="🚛",
                   layout="wide", initial_sidebar_state="collapsed")

st.markdown(f"""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Source+Sans+3:wght@300;400;600;700&display=swap');
  html, body, [class*="css"] {{ font-family: 'Source Sans 3', sans-serif !important; }}
  .stApp {{ background-color: {KN_NAVY}; }}
  .block-container {{ padding-top: 1.5rem !important; padding-bottom: 2rem !important; max-width: 1400px; }}
  #MainMenu, footer, header {{ visibility: hidden; }}
  p, li, span, div, label {{ color: {KN_LIGHT} !important; }}
  h1,h2,h3,h4 {{ color: {KN_WHITE} !important; font-family: 'Source Sans 3', sans-serif !important; }}
  strong, b {{ color: {KN_WHITE} !important; }}
  code {{ background: {KN_BORDER} !important; color: #88CCEE !important; border: none !important; padding: 2px 8px !important; font-size: 13px !important; border-radius: 2px !important; }}
  .kn-header {{ padding-bottom: 16px; border-bottom: 2px solid {KN_BLUE}; margin-bottom: 20px; }}
  .kn-logo {{ font-size: 26px; font-weight: 700; color: {KN_WHITE}; }}
  .kn-logo-plus {{ color: {KN_BLUE}; }}
  .kn-subtitle {{ font-size: 12px; font-weight: 600; color: {KN_LIGHT}; text-transform: uppercase; letter-spacing: 1.5px; margin-top: 4px; }}
  .kn-ts {{ font-size: 13px; color: {KN_LIGHT}; margin-bottom: 20px; padding: 8px 14px; background: {KN_CARD}; border-left: 3px solid {KN_BLUE}; display: inline-block; }}
  .kpi-box {{ background: {KN_CARD}; border: 1px solid {KN_BORDER}; border-top: 3px solid {KN_BLUE}; padding: 20px; }}
  .kpi-label {{ font-size: 11px; font-weight: 700; color: {KN_LIGHT} !important; text-transform: uppercase; letter-spacing: 1.5px; margin-bottom: 10px; }}
  .kpi-value {{ font-size: 40px; font-weight: 700; line-height: 1; color: {KN_WHITE}; }}
  .kpi-sub {{ font-size: 13px; color: {KN_LIGHT} !important; margin-top: 8px; }}
  .kpi-high   {{ border-top-color: {RISK_HIGH}; }} .kpi-high .kpi-value   {{ color: {RISK_HIGH}; }}
  .kpi-medium {{ border-top-color: {RISK_MED};  }} .kpi-medium .kpi-value {{ color: {RISK_MED};  }}
  .kpi-clear  {{ border-top-color: {RISK_LOW};  }} .kpi-clear .kpi-value  {{ color: {RISK_LOW};  }}
  .kpi-temp   {{ border-top-color: #66CCFF;     }} .kpi-temp .kpi-value   {{ color: #66CCFF;     }}
  .kn-alert {{ background: rgba(232,57,74,0.12); border: 1px solid rgba(232,57,74,0.35); border-left: 4px solid {RISK_HIGH}; color: #FFB8BE !important; padding: 13px 18px; font-size: 14px; font-weight: 600; margin-bottom: 20px; }}
  .kn-section {{ font-size: 11px; font-weight: 700; color: {KN_LIGHT} !important; text-transform: uppercase; letter-spacing: 2px; padding-bottom: 10px; border-bottom: 1px solid {KN_BORDER}; margin-bottom: 14px; }}
  [data-testid="stExpander"] {{ background: {KN_CARD} !important; border: 1px solid {KN_BORDER} !important; border-radius: 0 !important; margin-bottom: 5px !important; }}
  [data-testid="stExpander"]:hover {{ border-color: {KN_BLUE} !important; }}
  [data-testid="stExpander"] summary p,
  [data-testid="stExpander"] summary span {{ color: {KN_WHITE} !important; font-weight: 600 !important; font-size: 14px !important; }}
  [data-testid="stExpanderDetails"] {{ background: #0F2040 !important; border-top: 1px solid {KN_BORDER} !important; padding: 12px 16px !important; }}
  [data-testid="stExpanderDetails"] p,
  [data-testid="stExpanderDetails"] span,
  [data-testid="stExpanderDetails"] label {{ color: {KN_LIGHT} !important; font-size: 13px !important; }}
  [data-testid="stExpanderDetails"] strong {{ color: {KN_WHITE} !important; }}
  [data-testid="stExpanderDetails"] code {{ background: {KN_BORDER} !important; color: #88CCEE !important; }}
  .kn-rec {{ background: {KN_NAVY}; padding: 10px 14px; font-size: 13px; color: {KN_LIGHT} !important; margin-top: 10px; line-height: 1.6; }}
  .wp-row {{ display: flex; align-items: center; gap: 10px; padding: 7px 0; border-bottom: 1px solid {KN_BORDER}; font-size: 12px; }}
  .wp-row:last-child {{ border-bottom: none; }}
  .wp-name {{ color: {KN_WHITE}; font-weight: 600; min-width: 160px; }}
  .wp-type-hr {{ background: rgba(232,57,74,0.15); color: {RISK_HIGH}; border: 1px solid rgba(232,57,74,0.3); padding: 1px 8px; font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 1px; }}
  .wp-type-iv {{ background: rgba(0,153,204,0.12); color: {KN_BLUE}; border: 1px solid rgba(0,153,204,0.3); padding: 1px 8px; font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 1px; }}
  .wp-type-ep {{ background: rgba(200,216,232,0.1); color: {KN_LIGHT}; border: 1px solid {KN_BORDER}; padding: 1px 8px; font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 1px; }}
  .wp-risk-high   {{ color: {RISK_HIGH}; font-weight: 700; }}
  .wp-risk-medium {{ color: {RISK_MED};  font-weight: 700; }}
  .wp-risk-low    {{ color: {RISK_LOW};  font-weight: 600; }}
  .worst-point-box {{ background: rgba(232,57,74,0.08); border: 1px solid rgba(232,57,74,0.25); border-left: 3px solid {RISK_HIGH}; padding: 10px 14px; margin: 8px 0; font-size: 13px; }}
  .stButton > button {{ background-color: {KN_BLUE} !important; color: {KN_WHITE} !important; border: none !important; font-weight: 700 !important; font-size: 13px !important; border-radius: 2px !important; padding: 10px 20px !important; }}
  .stButton > button:hover {{ background-color: #007AA3 !important; }}
  .kn-footer {{ border-top: 1px solid {KN_BORDER}; margin-top: 40px; padding-top: 16px; font-size: 12px; color: {KN_DIM} !important; display: flex; justify-content: space-between; }}
</style>
""", unsafe_allow_html=True)


# ── Pipeline ──────────────────────────────────────────────────────────────────
@st.cache_data(ttl=0, show_spinner=False)
def run_pipeline():
    raw_weather  = fetch_all_hub_weather()
    weather_map  = validate_hub_weather(raw_weather)
    country_codes = list(set(h["country"] for h in LOGISTICS_HUBS))
    country_data  = fetch_country_data(country_codes)

    corridor_waypoints = {}
    for corridor in ROAD_CORRIDORS:
        corridor_waypoints[corridor["id"]] = fetch_waypoint_weather(corridor)

    corridors = [
        assess_corridor_risk(
            corridor         = c,
            hub_weather_map  = weather_map,
            waypoint_weather = corridor_waypoints.get(c["id"], []),
            country_data     = country_data,
        )
        for c in ROAD_CORRIDORS
    ]
    summary    = build_summary(corridors, weather_map)
    risk_order = {"high": 0, "medium": 1, "low": 2}
    return {
        "corridors":   sorted(corridors, key=lambda c: risk_order.get(c["risk_level"], 3)),
        "summary":     summary,
        "weather_map": {k: v.model_dump() if v else None for k, v in weather_map.items()},
    }


# ── Session state ─────────────────────────────────────────────────────────────
if "data" not in st.session_state:
    st.session_state.data = None
if "last_fetched" not in st.session_state:
    st.session_state.last_fetched = None


# ── Header ────────────────────────────────────────────────────────────────────
col_title, col_btn = st.columns([5, 1])
with col_title:
    st.markdown(f"""
    <div class="kn-header">
      <div class="kn-logo">KUEHNE<span class="kn-logo-plus">+</span>NAGEL</div>
      <div class="kn-subtitle">Road Logistics &nbsp;·&nbsp; Weather Risk Dashboard &nbsp;·&nbsp; European Network</div>
    </div>""", unsafe_allow_html=True)
with col_btn:
    st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)
    refresh = st.button("⟳  Refresh Live Data", type="primary", use_container_width=True)


# ── Fetch data ────────────────────────────────────────────────────────────────
if st.session_state.data is None or refresh:
    with st.spinner("Fetching live weather and sampling corridor waypoints..."):
        try:
            st.cache_data.clear()
            st.session_state.data = run_pipeline()
            st.session_state.last_fetched = datetime.utcnow().strftime("%d %b %Y, %H:%M UTC")
        except Exception as e:
            st.error(f"Pipeline error: {e}")
            st.stop()

data      = st.session_state.data
corridors = data["corridors"]
summary   = data["summary"]

if st.session_state.last_fetched:
    total_wp = sum(c.get("waypoint_count", 0) for c in corridors)
    st.markdown(
        f'<div class="kn-ts">'
        f'<span style="color:{RISK_LOW};font-size:10px">●</span>&nbsp; '
        f'Live data &nbsp;·&nbsp; Last refreshed: '
        f'<strong style="color:{KN_WHITE}">{st.session_state.last_fetched}</strong>'
        f'&nbsp;&nbsp;·&nbsp;&nbsp;{total_wp} waypoints assessed across {len(corridors)} corridors'
        f'</div>', unsafe_allow_html=True
    )

# ── Alert ─────────────────────────────────────────────────────────────────────
high_count = summary["corridors_at_high_risk"]
hr_triggers = summary.get("high_risk_area_triggers", 0)
if high_count > 0:
    disp     = summary.get("most_common_disruption", "")
    disp_str = f"&nbsp;·&nbsp; Primary: <strong>{disp.replace('_',' ').title()}</strong>" if disp else ""
    hr_str   = f"&nbsp;·&nbsp; <strong>{hr_triggers} high-risk area{'s' if hr_triggers != 1 else ''}</strong> triggered" if hr_triggers else ""
    st.markdown(
        f'<div class="kn-alert">⚠&nbsp;&nbsp;'
        f'{high_count} corridor{"s" if high_count > 1 else ""} at HIGH RISK'
        f'{disp_str}{hr_str}</div>', unsafe_allow_html=True
    )


# ── KPIs ──────────────────────────────────────────────────────────────────────
def kpi(col, label, value, sub, css_class):
    col.markdown(
        f'<div class="kpi-box {css_class}"><div class="kpi-label">{label}</div>'
        f'<div class="kpi-value">{value}</div><div class="kpi-sub">{sub}</div></div>',
        unsafe_allow_html=True)

k1, k2, k3, k4, k5 = st.columns(5)
avg_temp = summary.get("avg_temp_across_network_c")
kpi(k1, "Corridors Monitored", summary["total_corridors"],          "European road network", "kpi-total")
kpi(k2, "High Risk",           summary["corridors_at_high_risk"],   "Immediate action",      "kpi-high")
kpi(k3, "Medium Risk",         summary["corridors_at_medium_risk"], "Monitor closely",       "kpi-medium")
kpi(k4, "Clear",               summary["corridors_clear"],          "Normal operations",     "kpi-clear")
kpi(k5, "Avg Network Temp",    f"{avg_temp}°C" if avg_temp else "N/A", "Across all hubs",   "kpi-temp")

st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)


# ── Main grid ─────────────────────────────────────────────────────────────────
left, right = st.columns([3, 2])
color_map  = {"high": RISK_HIGH, "medium": RISK_MED, "low": RISK_LOW}
hub_lookup = {h["id"]: h for h in LOGISTICS_HUBS}

with left:
    st.markdown('<div class="kn-section">Corridor Risk Map</div>', unsafe_allow_html=True)

    fig = go.Figure()

    # Corridor lines
    for c in corridors:
        o = hub_lookup.get(c["origin"]["hub_id"], {})
        d = hub_lookup.get(c["destination"]["hub_id"], {})
        if not o or not d:
            continue
        fig.add_trace(go.Scattergeo(
            lon=[o["lng"], d["lng"], None], lat=[o["lat"], d["lat"], None],
            mode="lines",
            line=dict(width=3 if c["risk_level"] == "high" else 2, color=color_map[c["risk_level"]]),
            hoverinfo="skip", showlegend=False,
        ))

    # Hub markers
    fig.add_trace(go.Scattergeo(
        lon=[h["lng"] for h in LOGISTICS_HUBS],
        lat=[h["lat"] for h in LOGISTICS_HUBS],
        mode="markers+text",
        marker=dict(size=9, color=KN_BLUE, symbol="circle", line=dict(width=1.5, color=KN_WHITE)),
        text=[h["name"] for h in LOGISTICS_HUBS],
        textposition="top center",
        textfont=dict(size=11, color=KN_WHITE),
        hovertemplate="<b>%{text}</b><extra></extra>",
        showlegend=False,
    ))

    # High-risk waypoint markers (diamond shape, red)
    hr_lats, hr_lngs, hr_names = [], [], []
    for c in corridors:
        for pt in c.get("all_points", []):
            if pt.get("type") == "high_risk":
                hr_lats.append(pt["lat"])
                hr_lngs.append(pt["lng"])
                risk_icon = "🔴" if pt["risk_level"] == "high" else "🟡" if pt["risk_level"] == "medium" else "🟢"
                hr_names.append(f"{risk_icon} {pt['name']}<br>{pt.get('risk_note','')}")

    if hr_lats:
        fig.add_trace(go.Scattergeo(
            lon=hr_lngs, lat=hr_lats,
            mode="markers",
            marker=dict(size=10, color=RISK_HIGH, symbol="diamond",
                        line=dict(width=1.5, color=KN_WHITE)),
            text=hr_names,
            hovertemplate="%{text}<extra></extra>",
            showlegend=False,
            name="High-Risk Areas",
        ))

    fig.update_layout(
        geo=dict(scope="europe", showland=True, landcolor="#1A3A5C",
                 showocean=True, oceancolor=KN_NAVY, showcountries=True,
                 countrycolor="#1E4070", showframe=False, bgcolor=KN_NAVY,
                 projection_type="natural earth"),
        paper_bgcolor=KN_NAVY, plot_bgcolor=KN_NAVY,
        margin=dict(l=0, r=0, t=0, b=0), height=440,
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    st.markdown(
        f'<div style="font-size:11px;color:{KN_DIM};margin-top:-8px;margin-bottom:16px;">'
        f'● Hubs &nbsp;·&nbsp; ◆ High-risk geographic areas &nbsp;·&nbsp; '
        f'<span style="color:{RISK_HIGH}">■</span> High risk &nbsp;'
        f'<span style="color:{RISK_MED}">■</span> Medium &nbsp;'
        f'<span style="color:{RISK_LOW}">■</span> Clear</div>',
        unsafe_allow_html=True
    )

    # Risk distribution chart
    st.markdown('<div class="kn-section">Risk Distribution</div>', unsafe_allow_html=True)
    risk_df = pd.DataFrame([
        {"Risk Level": "High Risk",   "Corridors": summary["corridors_at_high_risk"]},
        {"Risk Level": "Medium Risk", "Corridors": summary["corridors_at_medium_risk"]},
        {"Risk Level": "Clear",       "Corridors": summary["corridors_clear"]},
    ])
    fig2 = px.bar(risk_df, x="Risk Level", y="Corridors", color="Risk Level",
        color_discrete_map={"High Risk": RISK_HIGH, "Medium Risk": RISK_MED, "Clear": RISK_LOW},
        text="Corridors")
    fig2.update_layout(
        paper_bgcolor=KN_NAVY, plot_bgcolor=KN_CARD,
        font=dict(family="Source Sans 3", color=KN_LIGHT, size=13),
        showlegend=False, height=220, margin=dict(l=0, r=0, t=10, b=0),
        xaxis=dict(showgrid=False, tickfont=dict(color=KN_WHITE, size=13)),
        yaxis=dict(showgrid=True, gridcolor=KN_BORDER, tickfont=dict(color=KN_LIGHT, size=12)),
    )
    fig2.update_traces(textposition="outside", textfont=dict(color=KN_WHITE, size=14))
    st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False})


with right:
    st.markdown('<div class="kn-section">Active Corridors</div>', unsafe_allow_html=True)
    badge = {"high": "🔴", "medium": "🟡", "low": "🟢"}
    bc    = {"high": RISK_HIGH, "medium": RISK_MED, "low": RISK_LOW}

    for c in corridors:
        risk      = c["risk_level"]
        wp_count  = c.get("waypoint_count", 0)
        worst     = c.get("worst_point")

        with st.expander(
            f"{badge[risk]}  {c['origin']['city']} → {c['destination']['city']}  ·  {c['distance_km']:,} km",
            expanded=(risk == "high")
        ):
            r1, r2 = st.columns(2)
            r1.markdown(f"**Corridor:** `{c['corridor_id']}`")
            r2.markdown(f"**Risk:** `{risk.upper()}`")
            st.markdown(f"**Points assessed:** `{wp_count}` (endpoints + interval + high-risk areas)")

            # Worst point callout
            if worst and worst["risk_level"] != "low":
                type_label = "⚠ High-Risk Area" if worst.get("type") == "high_risk" else "📍 Interval Waypoint" if worst.get("type") == "interval" else "🏙 City Hub"
                risk_note  = f"<br><em>{worst['risk_note']}</em>" if worst.get("risk_note") else ""
                st.markdown(
                    f'<div class="worst-point-box">'
                    f'<strong style="color:{RISK_HIGH}">Worst point: {worst["name"]}</strong> &nbsp; {type_label}'
                    f'{risk_note}</div>',
                    unsafe_allow_html=True
                )

            if c["disruptions"]:
                st.markdown("**Disruptions:** " + "  ".join(
                    [f"`{d.replace('_',' ')}`" for d in c["disruptions"]]
                ))

            st.markdown(
                f'<div class="kn-rec" style="border-left:3px solid {bc[risk]};">'
                f'<strong style="color:{KN_WHITE}">Recommendation:</strong><br>'
                f'{c["recommendation"]}</div>', unsafe_allow_html=True
            )

            # Full waypoint breakdown
            all_pts = c.get("all_points", [])
            if all_pts:
                st.markdown(
                    f'<div style="margin-top:12px;font-size:11px;font-weight:700;'
                    f'color:{KN_DIM};text-transform:uppercase;letter-spacing:1.5px;">'
                    f'Point-by-point assessment ({len(all_pts)} points)</div>',
                    unsafe_allow_html=True
                )
                for pt in all_pts:
                    pt_risk  = pt["risk_level"]
                    type_tag = (
                        f'<span class="wp-type-hr">High-Risk Area</span>' if pt["type"] == "high_risk"
                        else f'<span class="wp-type-iv">Interval</span>'   if pt["type"] == "interval"
                        else f'<span class="wp-type-ep">Endpoint</span>'
                    )
                    risk_class = f"wp-risk-{pt_risk}"
                    w = pt.get("weather", {})
                    temp_str = f"{w.get('temp_c','?')}°C" if w.get("available") else "N/A"
                    wind_str = f"{w.get('wind_kmh','?')} km/h" if w.get("available") else ""
                    st.markdown(
                        f'<div class="wp-row">'
                        f'{type_tag}'
                        f'<span class="wp-name">{pt["name"]}</span>'
                        f'<span class="{risk_class}">{pt_risk.upper()}</span>'
                        f'<span style="color:{KN_DIM};font-size:12px;margin-left:auto;">'
                        f'{temp_str} &nbsp; {wind_str}</span>'
                        f'</div>',
                        unsafe_allow_html=True
                    )


# ── Hub temperature chart ─────────────────────────────────────────────────────
st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
st.markdown('<div class="kn-section">Hub Temperature Overview</div>', unsafe_allow_html=True)

hub_temps, seen = [], set()
for c in corridors:
    for side in ["origin", "destination"]:
        city = c[side]["city"]
        w    = c["weather"][side]
        if city not in seen and w.get("available") and w.get("temp_c") is not None:
            hub_temps.append({
                "Hub": city, "Temp (°C)": w["temp_c"],
                "Wind (km/h)": w.get("wind_kmh", 0),
                "Condition": w.get("condition", "").replace("_", " ").title(),
            })
            seen.add(city)

if hub_temps:
    temp_df = pd.DataFrame(hub_temps).sort_values("Temp (°C)")
    colors  = [RISK_HIGH if t <= -5 else "#66CCFF" if t <= 0 else KN_BLUE for t in temp_df["Temp (°C)"]]
    fig3    = px.bar(temp_df, x="Hub", y="Temp (°C)", hover_data=["Wind (km/h)", "Condition"], text="Temp (°C)")
    fig3.update_traces(marker_color=colors, texttemplate="%{text:.1f}°C",
                       textposition="outside", textfont=dict(color=KN_WHITE, size=12))
    fig3.update_layout(
        paper_bgcolor=KN_NAVY, plot_bgcolor=KN_CARD,
        font=dict(family="Source Sans 3", color=KN_LIGHT, size=13),
        showlegend=False, height=280, margin=dict(l=0, r=0, t=24, b=0),
        xaxis=dict(showgrid=False, tickangle=-30, tickfont=dict(color=KN_WHITE, size=13)),
        yaxis=dict(showgrid=True, gridcolor=KN_BORDER, zeroline=True,
                   zerolinecolor=KN_BORDER, tickfont=dict(color=KN_LIGHT, size=12)),
    )
    st.plotly_chart(fig3, use_container_width=True, config={"displayModeBar": False})


# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="kn-footer">
  <span>Data: Open-Meteo (live weather) &nbsp;·&nbsp; REST Countries API</span>
  <span>Kuehne+Nagel &nbsp;·&nbsp; Road Logistics &nbsp;·&nbsp; Operational Excellence &nbsp;·&nbsp; v3</span>
</div>
""", unsafe_allow_html=True)
