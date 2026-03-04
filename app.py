"""
K+N Road Logistics — Live Operations Dashboard v5
--------------------------------------------------
Clean K+N branded dashboard with:
  - No emojis — professional text labels throughout
  - 24h forecast timeline per corridor (NOW / +6H / +12H / +24H)
  - Deterioration warnings for currently-safe corridors
  - Named waypoints: "WP2 — Cologne" format
  - Interactive sidebar filters and map layer toggles
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import sys, os

sys.path.insert(0, os.path.dirname(__file__))

from src.api.client import (
    fetch_all_hub_weather, fetch_waypoint_weather,
    fetch_country_data, LOGISTICS_HUBS, ROAD_CORRIDORS,
)
from src.parsers.validator import validate_hub_weather, HubWeather
from src.transformers.transform import assess_corridor_risk, build_summary

# ── Brand colours ─────────────────────────────────────────────────────────────
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
RISK_DET  = "#FF8C42"   # orange — deteriorating (clear now, worsening soon)
COLOR_MAP = {"high": RISK_HIGH, "medium": RISK_MED, "low": RISK_LOW}

st.set_page_config(
    page_title="K+N Road Risk Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(f"""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Source+Sans+3:wght@300;400;600;700&display=swap');
  html, body, [class*="css"] {{ font-family: 'Source Sans 3', sans-serif !important; }}
  .stApp {{ background-color: {KN_NAVY}; }}
  .block-container {{ padding-top: 1.2rem !important; padding-bottom: 2rem !important; max-width: 1400px; }}
  #MainMenu, footer, header {{ visibility: hidden; }}
  p, li, span, div, label {{ color: {KN_LIGHT} !important; }}
  h1,h2,h3,h4 {{ color: {KN_WHITE} !important; font-family: 'Source Sans 3', sans-serif !important; }}
  strong, b {{ color: {KN_WHITE} !important; }}
  code {{ background: {KN_BORDER} !important; color: #88CCEE !important; border: none !important; padding: 2px 8px !important; font-size: 13px !important; border-radius: 2px !important; }}

  /* Sidebar */
  [data-testid="stSidebar"] {{ background-color: #0a1a30 !important; border-right: 1px solid {KN_BORDER}; }}
  [data-testid="stSidebar"] p,
  [data-testid="stSidebar"] label,
  [data-testid="stSidebar"] span {{ color: {KN_LIGHT} !important; }}
  [data-testid="stSidebar"] h1,
  [data-testid="stSidebar"] h2,
  [data-testid="stSidebar"] h3 {{ color: {KN_WHITE} !important; }}

  /* Header */
  .kn-header {{ padding-bottom: 14px; border-bottom: 2px solid {KN_BLUE}; margin-bottom: 18px; }}
  .kn-logo {{ font-size: 24px; font-weight: 700; color: {KN_WHITE}; }}
  .kn-logo-plus {{ color: {KN_BLUE}; }}
  .kn-subtitle {{ font-size: 11px; font-weight: 600; color: {KN_LIGHT}; text-transform: uppercase; letter-spacing: 1.5px; margin-top: 3px; }}
  .kn-ts {{ font-size: 12px; color: {KN_LIGHT}; margin-bottom: 16px; padding: 7px 12px; background: {KN_CARD}; border-left: 3px solid {KN_BLUE}; display: inline-block; }}

  /* KPIs */
  .kpi-box {{ background: {KN_CARD}; border: 1px solid {KN_BORDER}; border-top: 3px solid {KN_BLUE}; padding: 18px; }}
  .kpi-label {{ font-size: 10px; font-weight: 700; color: {KN_LIGHT} !important; text-transform: uppercase; letter-spacing: 1.5px; margin-bottom: 8px; }}
  .kpi-value {{ font-size: 36px; font-weight: 700; line-height: 1; color: {KN_WHITE}; }}
  .kpi-sub {{ font-size: 12px; color: {KN_LIGHT} !important; margin-top: 6px; }}
  .kpi-high   {{ border-top-color: {RISK_HIGH}; }} .kpi-high .kpi-value   {{ color: {RISK_HIGH}; }}
  .kpi-medium {{ border-top-color: {RISK_MED};  }} .kpi-medium .kpi-value {{ color: {RISK_MED};  }}
  .kpi-clear  {{ border-top-color: {RISK_LOW};  }} .kpi-clear .kpi-value  {{ color: {RISK_LOW};  }}
  .kpi-det    {{ border-top-color: {RISK_DET};  }} .kpi-det .kpi-value    {{ color: {RISK_DET};  }}
  .kpi-temp   {{ border-top-color: #66CCFF;     }} .kpi-temp .kpi-value   {{ color: #66CCFF;     }}

  /* Alerts */
  .kn-alert-high {{ background: rgba(232,57,74,0.10); border: 1px solid rgba(232,57,74,0.35); border-left: 4px solid {RISK_HIGH}; color: #FFB8BE !important; padding: 11px 16px; font-size: 14px; font-weight: 600; margin-bottom: 10px; }}
  .kn-alert-det  {{ background: rgba(255,140,66,0.10); border: 1px solid rgba(255,140,66,0.35); border-left: 4px solid {RISK_DET}; color: #FFCCA0 !important; padding: 11px 16px; font-size: 14px; font-weight: 600; margin-bottom: 10px; }}

  /* Section headers */
  .kn-section {{ font-size: 10px; font-weight: 700; color: {KN_DIM} !important; text-transform: uppercase; letter-spacing: 2px; padding-bottom: 8px; border-bottom: 1px solid {KN_BORDER}; margin-bottom: 12px; }}

  /* Risk badges — inline text */
  .badge {{ display: inline-block; padding: 2px 10px; font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.8px; }}
  .badge-high   {{ background: rgba(232,57,74,0.15); color: {RISK_HIGH}; border: 1px solid rgba(232,57,74,0.4); }}
  .badge-medium {{ background: rgba(245,166,35,0.15); color: {RISK_MED};  border: 1px solid rgba(245,166,35,0.4); }}
  .badge-low    {{ background: rgba(39,174,96,0.15);  color: {RISK_LOW};  border: 1px solid rgba(39,174,96,0.4);  }}
  .badge-det    {{ background: rgba(255,140,66,0.15); color: {RISK_DET};  border: 1px solid rgba(255,140,66,0.4); }}

  /* Timeline table */
  .timeline-table {{ width: 100%; border-collapse: collapse; margin: 10px 0; }}
  .timeline-table th {{ font-size: 10px; font-weight: 700; color: {KN_DIM}; text-transform: uppercase; letter-spacing: 1.5px; padding: 5px 8px; border-bottom: 1px solid {KN_BORDER}; text-align: center; }}
  .timeline-table td {{ padding: 7px 8px; text-align: center; border-bottom: 1px solid {KN_BORDER}; }}
  .timeline-table tr:last-child td {{ border-bottom: none; }}
  .tl-high   {{ color: {RISK_HIGH}; font-weight: 700; font-size: 12px; }}
  .tl-medium {{ color: {RISK_MED};  font-weight: 700; font-size: 12px; }}
  .tl-low    {{ color: {RISK_LOW};  font-weight: 600; font-size: 12px; }}
  .tl-now    {{ background: rgba(0,153,204,0.08); }}

  /* Expanders */
  [data-testid="stExpander"] {{ background: {KN_CARD} !important; border: 1px solid {KN_BORDER} !important; border-radius: 0 !important; margin-bottom: 4px !important; }}
  [data-testid="stExpander"]:hover {{ border-color: {KN_BLUE} !important; }}
  [data-testid="stExpander"] summary p,
  [data-testid="stExpander"] summary span {{ color: {KN_WHITE} !important; font-weight: 600 !important; font-size: 13px !important; }}
  [data-testid="stExpanderDetails"] {{ background: #0F2040 !important; border-top: 1px solid {KN_BORDER} !important; padding: 12px 14px !important; }}
  [data-testid="stExpanderDetails"] p,
  [data-testid="stExpanderDetails"] span,
  [data-testid="stExpanderDetails"] label {{ color: {KN_LIGHT} !important; font-size: 13px !important; }}
  [data-testid="stExpanderDetails"] strong {{ color: {KN_WHITE} !important; }}
  [data-testid="stExpanderDetails"] code {{ background: {KN_BORDER} !important; color: #88CCEE !important; }}

  /* Deterioration callout */
  .det-box {{ background: rgba(255,140,66,0.08); border: 1px solid rgba(255,140,66,0.3); border-left: 3px solid {RISK_DET}; padding: 9px 12px; margin: 8px 0; font-size: 13px; color: #FFCCA0 !important; }}

  /* Worst point callout */
  .worst-box {{ background: rgba(232,57,74,0.08); border: 1px solid rgba(232,57,74,0.25); border-left: 3px solid {RISK_HIGH}; padding: 9px 12px; margin: 8px 0; font-size: 13px; }}

  /* Recommendation */
  .kn-rec {{ background: {KN_NAVY}; padding: 9px 12px; font-size: 13px; color: {KN_LIGHT} !important; margin-top: 8px; line-height: 1.6; }}

  /* Waypoint rows */
  .wp-row {{ display: flex; align-items: center; gap: 10px; padding: 6px 0; border-bottom: 1px solid {KN_BORDER}; font-size: 12px; }}
  .wp-row:last-child {{ border-bottom: none; }}
  .wp-name {{ color: {KN_WHITE}; font-weight: 600; min-width: 160px; }}
  .wp-tag {{ padding: 1px 7px; font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 1px; white-space: nowrap; }}
  .wp-tag-hr {{ background: rgba(232,57,74,0.12); color: {RISK_HIGH}; border: 1px solid rgba(232,57,74,0.3); }}
  .wp-tag-iv {{ background: rgba(0,153,204,0.10); color: {KN_BLUE};   border: 1px solid rgba(0,153,204,0.3); }}
  .wp-tag-ep {{ background: rgba(200,216,232,0.08); color: {KN_LIGHT}; border: 1px solid {KN_BORDER}; }}
  .wp-risk-high   {{ color: {RISK_HIGH}; font-weight: 700; min-width: 55px; }}
  .wp-risk-medium {{ color: {RISK_MED};  font-weight: 700; min-width: 55px; }}
  .wp-risk-low    {{ color: {RISK_LOW};  font-weight: 600; min-width: 55px; }}

  /* Button */
  .stButton > button {{ background-color: {KN_BLUE} !important; color: {KN_WHITE} !important; border: none !important; font-weight: 700 !important; font-size: 13px !important; border-radius: 2px !important; padding: 9px 18px !important; }}
  .stButton > button:hover {{ background-color: #007AA3 !important; }}

  /* Footer */
  .kn-footer {{ border-top: 1px solid {KN_BORDER}; margin-top: 32px; padding-top: 14px; font-size: 11px; color: {KN_DIM} !important; display: flex; justify-content: space-between; }}
</style>
""", unsafe_allow_html=True)


# ── Helpers ───────────────────────────────────────────────────────────────────
def risk_badge(risk: str, deteriorating: bool = False) -> str:
    if deteriorating and risk == "low":
        return '<span class="badge badge-det">DETERIORATING</span>'
    return f'<span class="badge badge-{risk}">{risk.upper()}</span>'

def tl_cell(risk: str) -> str:
    return f'<td class="tl-{risk}">{risk.upper()}</td>'


# ── Pipeline ──────────────────────────────────────────────────────────────────
@st.cache_data(ttl=0, show_spinner=False)
def run_pipeline():
    raw_weather   = fetch_all_hub_weather()
    country_codes = list(set(h["country"] for h in LOGISTICS_HUBS))
    country_data  = fetch_country_data(country_codes)
    corridor_wps  = {c["id"]: fetch_waypoint_weather(c) for c in ROAD_CORRIDORS}
    corridors     = [
        assess_corridor_risk(c, raw_weather, corridor_wps.get(c["id"], []), country_data)
        for c in ROAD_CORRIDORS
    ]
    summary    = build_summary(corridors, raw_weather)
    risk_order = {"high": 0, "medium": 1, "low": 2}
    return {
        "corridors":   sorted(corridors, key=lambda c: (
            0 if c["risk_level"] == "high"
            else 1 if c.get("deteriorating")
            else 2 if c["risk_level"] == "medium"
            else 3
        )),
        "summary":     summary,
        "raw_weather": raw_weather,
    }


# ── Session state ─────────────────────────────────────────────────────────────
if "data" not in st.session_state:
    st.session_state.data = None
if "last_fetched" not in st.session_state:
    st.session_state.last_fetched = None


# ════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown(f"<div style='font-size:18px;font-weight:700;color:{KN_WHITE};padding-bottom:2px;'>KUEHNE+NAGEL</div>", unsafe_allow_html=True)
    st.markdown(f"<div style='font-size:10px;color:{KN_DIM};text-transform:uppercase;letter-spacing:1.5px;margin-bottom:20px;'>Road Logistics Operations</div>", unsafe_allow_html=True)

    st.markdown(f"<div style='font-size:10px;font-weight:700;color:{KN_DIM};text-transform:uppercase;letter-spacing:1.5px;margin-bottom:8px;'>Filter Corridors</div>", unsafe_allow_html=True)
    risk_filter = st.radio(
        label="risk_filter", label_visibility="collapsed",
        options=["All Corridors", "High Risk Only", "Deteriorating Only", "Medium Risk Only", "Clear Only"],
        index=0,
    )

    st.markdown("<hr style='border-color:#1E4070;margin:18px 0;'>", unsafe_allow_html=True)
    st.markdown(f"<div style='font-size:10px;font-weight:700;color:{KN_DIM};text-transform:uppercase;letter-spacing:1.5px;margin-bottom:8px;'>Map Layers</div>", unsafe_allow_html=True)
    show_interval_wps = st.toggle("Interval waypoints", value=True)
    show_hr_areas     = st.toggle("High-risk areas", value=True)
    show_hub_labels   = st.toggle("Hub labels", value=True)

    st.markdown("<hr style='border-color:#1E4070;margin:18px 0;'>", unsafe_allow_html=True)

    if st.session_state.data:
        s = st.session_state.data["summary"]
        st.markdown(f"<div style='font-size:10px;font-weight:700;color:{KN_DIM};text-transform:uppercase;letter-spacing:1.5px;margin-bottom:10px;'>Network Status</div>", unsafe_allow_html=True)
        for label, val, color in [
            ("Total Corridors",    s["total_corridors"],           KN_LIGHT),
            ("High Risk",          s["corridors_at_high_risk"],    RISK_HIGH),
            ("Deteriorating",      s["deteriorating_corridors"],   RISK_DET),
            ("Medium Risk",        s["corridors_at_medium_risk"],  RISK_MED),
            ("Clear",              s["corridors_clear"],           RISK_LOW),
        ]:
            st.markdown(
                f'<div style="display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid {KN_BORDER};">'
                f'<span style="font-size:12px;color:{KN_LIGHT};">{label}</span>'
                f'<span style="font-size:14px;font-weight:700;color:{color};">{val}</span>'
                f'</div>', unsafe_allow_html=True
            )
        avg_t = s.get("avg_temp_across_network_c")
        if avg_t is not None:
            st.markdown(
                f'<div style="margin-top:12px;padding:10px;background:{KN_CARD};border-left:3px solid #66CCFF;">'
                f'<div style="font-size:10px;color:{KN_DIM};text-transform:uppercase;letter-spacing:1px;">Avg Network Temp</div>'
                f'<div style="font-size:22px;font-weight:700;color:#66CCFF;">{avg_t}°C</div>'
                f'</div>', unsafe_allow_html=True
            )

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
    refresh_sidebar = st.button("Refresh Live Data", use_container_width=True, type="primary")


# ════════════════════════════════════════════════════════════════════════════
# HEADER
# ════════════════════════════════════════════════════════════════════════════
col_title, col_btn = st.columns([6, 1])
with col_title:
    st.markdown(f"""
    <div class="kn-header">
      <div class="kn-logo">KUEHNE<span class="kn-logo-plus">+</span>NAGEL</div>
      <div class="kn-subtitle">Road Logistics &nbsp;·&nbsp; Weather Risk Dashboard &nbsp;·&nbsp; European Network &nbsp;·&nbsp; 24h Forecast</div>
    </div>""", unsafe_allow_html=True)
with col_btn:
    st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)
    refresh_main = st.button("Refresh", type="primary", use_container_width=True)

refresh = refresh_main or refresh_sidebar


# ── Fetch data ────────────────────────────────────────────────────────────────
if st.session_state.data is None or refresh:
    with st.spinner("Fetching live weather, forecast data, and geocoding waypoints..."):
        try:
            st.cache_data.clear()
            st.session_state.data = run_pipeline()
            st.session_state.last_fetched = datetime.utcnow().strftime("%d %b %Y, %H:%M UTC")
            st.rerun()
        except Exception as e:
            st.error(f"Pipeline error: {e}")
            st.stop()

data      = st.session_state.data
corridors = data["corridors"]
summary   = data["summary"]

# Apply filter
filter_map = {
    "All Corridors":       None,
    "High Risk Only":      "high",
    "Deteriorating Only":  "deteriorating",
    "Medium Risk Only":    "medium",
    "Clear Only":          "low",
}
active_filter = filter_map[risk_filter]
if active_filter == "deteriorating":
    filtered = [c for c in corridors if c.get("deteriorating")]
elif active_filter:
    filtered = [c for c in corridors if c["risk_level"] == active_filter]
else:
    filtered = corridors

# Timestamp
if st.session_state.last_fetched:
    total_wp = sum(c.get("waypoint_count", 0) for c in corridors)
    st.markdown(
        f'<div class="kn-ts">Live data &nbsp;·&nbsp; Refreshed: '
        f'<strong style="color:{KN_WHITE}">{st.session_state.last_fetched}</strong>'
        f'&nbsp;&nbsp;·&nbsp;&nbsp;{total_wp} waypoints assessed &nbsp;·&nbsp; 24h forecast active</div>',
        unsafe_allow_html=True
    )

# Alert banners
high_count  = summary["corridors_at_high_risk"]
det_count   = summary.get("deteriorating_corridors", 0)
hr_triggers = summary.get("high_risk_area_triggers", 0)

if high_count > 0:
    disp    = summary.get("most_common_disruption", "")
    d_str   = f" &nbsp;·&nbsp; Primary disruption: <strong>{disp.replace('_',' ').title()}</strong>" if disp else ""
    hr_str  = f" &nbsp;·&nbsp; <strong>{hr_triggers} high-risk area{'s' if hr_triggers!=1 else ''} triggered</strong>" if hr_triggers else ""
    st.markdown(f'<div class="kn-alert-high">HIGH RISK &nbsp;·&nbsp; {high_count} corridor{"s" if high_count>1 else ""} require immediate attention{d_str}{hr_str}</div>', unsafe_allow_html=True)

if det_count > 0:
    st.markdown(f'<div class="kn-alert-det">FORECAST WARNING &nbsp;·&nbsp; {det_count} corridor{"s" if det_count>1 else ""} currently clear but conditions will deteriorate within 24h &nbsp;·&nbsp; Brief drivers before departure</div>', unsafe_allow_html=True)

# KPIs
def kpi(col, label, value, sub, css_class):
    col.markdown(
        f'<div class="kpi-box {css_class}"><div class="kpi-label">{label}</div>'
        f'<div class="kpi-value">{value}</div><div class="kpi-sub">{sub}</div></div>',
        unsafe_allow_html=True)

k1, k2, k3, k4, k5, k6 = st.columns(6)
avg_t = summary.get("avg_temp_across_network_c")
kpi(k1, "Corridors",     summary["total_corridors"],          "Monitored",          "kpi-total")
kpi(k2, "High Risk",     summary["corridors_at_high_risk"],   "Act now",            "kpi-high")
kpi(k3, "Deteriorating", summary["deteriorating_corridors"],  "Will worsen <24h",   "kpi-det")
kpi(k4, "Medium Risk",   summary["corridors_at_medium_risk"], "Monitor",            "kpi-medium")
kpi(k5, "Clear",         summary["corridors_clear"],          "Normal ops",         "kpi-clear")
kpi(k6, "Avg Temp",      f"{avg_t}°C" if avg_t else "N/A",   "Network average",    "kpi-temp")

st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════════════
# MAIN GRID
# ════════════════════════════════════════════════════════════════════════════
left, right = st.columns([3, 2])
hub_lookup  = {h["id"]: h for h in LOGISTICS_HUBS}

with left:
    st.markdown('<div class="kn-section">Corridor Risk Map</div>', unsafe_allow_html=True)

    fig = go.Figure()

    # Corridor lines
    for c in filtered:
        o = hub_lookup.get(c["origin"]["hub_id"], {})
        d = hub_lookup.get(c["destination"]["hub_id"], {})
        if not o or not d:
            continue
        line_color = RISK_DET if (c.get("deteriorating") and c["risk_level"] == "low") else COLOR_MAP[c["risk_level"]]
        fig.add_trace(go.Scattergeo(
            lon=[o["lng"], d["lng"], None], lat=[o["lat"], d["lat"], None],
            mode="lines",
            line=dict(width=3 if c["risk_level"] == "high" else 2, color=line_color),
            hoverinfo="skip", showlegend=False,
        ))

    # Interval waypoints
    if show_interval_wps:
        iv_lats, iv_lngs, iv_colors, iv_texts = [], [], [], []
        for c in filtered:
            for pt in c.get("all_points", []):
                if pt["type"] == "interval":
                    iv_lats.append(pt["lat"])
                    iv_lngs.append(pt["lng"])
                    iv_colors.append(COLOR_MAP[pt["risk_level"]])
                    w    = pt.get("weather", {})
                    temp = f"{w.get('temp_c','?')}°C" if w.get("available") else "N/A"
                    wind = f"{w.get('wind_kmh','?')} km/h" if w.get("available") else ""
                    cond = w.get("condition","").replace("_"," ").title() if w.get("available") else ""
                    iv_texts.append(
                        f"<b>{pt['name']}</b><br>"
                        f"Corridor: {c['corridor_id']}<br>"
                        f"Risk: {pt['risk_level'].upper()}<br>"
                        f"Temp: {temp} &nbsp; Wind: {wind}<br>{cond}"
                    )
        if iv_lats:
            fig.add_trace(go.Scattergeo(
                lon=iv_lngs, lat=iv_lats, mode="markers",
                marker=dict(size=7, color=iv_colors, symbol="circle", line=dict(width=1, color=KN_NAVY)),
                text=iv_texts, hovertemplate="%{text}<extra></extra>", showlegend=False,
            ))

    # High-risk area diamonds
    if show_hr_areas:
        hr_lats, hr_lngs, hr_colors, hr_texts = [], [], [], []
        seen_hr = set()
        for c in filtered:
            for pt in c.get("all_points", []):
                if pt["type"] == "high_risk" and pt["name"] not in seen_hr:
                    seen_hr.add(pt["name"])
                    hr_lats.append(pt["lat"])
                    hr_lngs.append(pt["lng"])
                    hr_colors.append(COLOR_MAP[pt["risk_level"]])
                    w    = pt.get("weather", {})
                    temp = f"{w.get('temp_c','?')}°C" if w.get("available") else "N/A"
                    wind = f"{w.get('wind_kmh','?')} km/h" if w.get("available") else ""
                    hr_texts.append(
                        f"<b>High-Risk Area: {pt['name']}</b><br>"
                        f"{pt.get('risk_note','')}<br>"
                        f"Risk: {pt['risk_level'].upper()}<br>"
                        f"Temp: {temp} &nbsp; Wind: {wind}"
                    )
        if hr_lats:
            fig.add_trace(go.Scattergeo(
                lon=hr_lngs, lat=hr_lats, mode="markers",
                marker=dict(size=11, color=hr_colors, symbol="diamond", line=dict(width=1.5, color=KN_WHITE)),
                text=hr_texts, hovertemplate="%{text}<extra></extra>", showlegend=False,
            ))

    # Hub markers
    fig.add_trace(go.Scattergeo(
        lon=[h["lng"] for h in LOGISTICS_HUBS],
        lat=[h["lat"] for h in LOGISTICS_HUBS],
        mode="markers" + ("+text" if show_hub_labels else ""),
        marker=dict(size=9, color=KN_BLUE, symbol="circle", line=dict(width=1.5, color=KN_WHITE)),
        text=[h["name"] for h in LOGISTICS_HUBS] if show_hub_labels else [],
        textposition="top center", textfont=dict(size=11, color=KN_WHITE),
        hovertemplate="<b>%{text}</b><extra></extra>", showlegend=False,
    ))

    fig.update_layout(
        geo=dict(scope="europe", showland=True, landcolor="#1A3A5C",
                 showocean=True, oceancolor=KN_NAVY, showcountries=True,
                 countrycolor="#1E4070", showframe=False, bgcolor=KN_NAVY,
                 projection_type="natural earth"),
        paper_bgcolor=KN_NAVY, plot_bgcolor=KN_NAVY,
        margin=dict(l=0, r=0, t=0, b=0), height=420,
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    st.markdown(
        f'<div style="font-size:11px;color:{KN_DIM};margin-top:-6px;margin-bottom:14px;">'
        f'Circle = Hub &nbsp;·&nbsp; Diamond = High-risk area &nbsp;·&nbsp; Small dot = Interval waypoint &nbsp;·&nbsp; '
        f'<span style="color:{RISK_HIGH}">Red</span> = High &nbsp;'
        f'<span style="color:{RISK_MED}">Amber</span> = Medium &nbsp;'
        f'<span style="color:{RISK_LOW}">Green</span> = Clear &nbsp;'
        f'<span style="color:{RISK_DET}">Orange</span> = Deteriorating<br>'
        f'<em style="font-size:10px;">Hover any point for weather details</em></div>',
        unsafe_allow_html=True
    )

    # Risk distribution
    st.markdown('<div class="kn-section">Current Risk Distribution</div>', unsafe_allow_html=True)
    risk_df = pd.DataFrame([
        {"Status": "High Risk",    "Corridors": summary["corridors_at_high_risk"]},
        {"Status": "Deteriorating","Corridors": summary["deteriorating_corridors"]},
        {"Status": "Medium Risk",  "Corridors": summary["corridors_at_medium_risk"]},
        {"Status": "Clear",        "Corridors": summary["corridors_clear"]},
    ])
    fig2 = px.bar(risk_df, x="Status", y="Corridors", color="Status",
        color_discrete_map={
            "High Risk": RISK_HIGH, "Deteriorating": RISK_DET,
            "Medium Risk": RISK_MED, "Clear": RISK_LOW,
        }, text="Corridors")
    fig2.update_layout(
        paper_bgcolor=KN_NAVY, plot_bgcolor=KN_CARD,
        font=dict(family="Source Sans 3", color=KN_LIGHT, size=13),
        showlegend=False, height=210, margin=dict(l=0, r=0, t=8, b=0),
        xaxis=dict(showgrid=False, tickfont=dict(color=KN_WHITE, size=12)),
        yaxis=dict(showgrid=True, gridcolor=KN_BORDER, tickfont=dict(color=KN_LIGHT, size=11)),
    )
    fig2.update_traces(textposition="outside", textfont=dict(color=KN_WHITE, size=13))
    st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False})


# ── Corridor cards ────────────────────────────────────────────────────────────
with right:
    label_sfx = f" — {risk_filter}" if active_filter else ""
    st.markdown(f'<div class="kn-section">Corridors{label_sfx} ({len(filtered)} shown)</div>', unsafe_allow_html=True)

    bc = {"high": RISK_HIGH, "medium": RISK_MED, "low": RISK_LOW}

    if not filtered:
        st.markdown(f'<div style="color:{KN_DIM};font-size:13px;padding:16px 0;">No corridors match the selected filter.</div>', unsafe_allow_html=True)

    for c in filtered:
        risk         = c["risk_level"]
        deteriorating = c.get("deteriorating", False)
        det_at       = c.get("deterioration_at")
        worst        = c.get("worst_point")
        timeline     = c.get("timeline", {})

        # Expander label — no emojis, use text prefix
        risk_prefix = (
            "[HIGH]"   if risk == "high"
            else "[WARN]" if deteriorating
            else "[MED]"  if risk == "medium"
            else "[OK]"
        )
        expanded = risk == "high" or deteriorating

        with st.expander(
            f"{risk_prefix}  {c['origin']['city']} to {c['destination']['city']}  —  {c['distance_km']:,} km",
            expanded=expanded
        ):
            r1, r2 = st.columns(2)
            r1.markdown(f"**Corridor:** `{c['corridor_id']}`")
            r2.markdown(f"**Current risk:** `{risk.upper()}`")
            st.markdown(f"**Points assessed:** `{c.get('waypoint_count', 0)}`")

            # Deterioration warning
            if deteriorating:
                future_risk = timeline.get(f"+{det_at}h", "unknown")
                st.markdown(
                    f'<div class="det-box">'
                    f'<strong style="color:{RISK_DET}">Forecast Warning</strong> &nbsp;·&nbsp; '
                    f'Corridor is currently {risk.upper()} but will reach '
                    f'<strong>{future_risk.upper()}</strong> within {det_at}h. '
                    f'Brief drivers before departure.</div>',
                    unsafe_allow_html=True
                )

            # Worst point
            if worst and worst["risk_level"] != "low":
                type_label = (
                    "High-Risk Geographic Area" if worst.get("type") == "high_risk"
                    else "Interval Waypoint"        if worst.get("type") == "interval"
                    else "City Hub"
                )
                note_str = f"<br><em style='color:{KN_DIM}'>{worst['risk_note']}</em>" if worst.get("risk_note") else ""
                st.markdown(
                    f'<div class="worst-box">'
                    f'<strong style="color:{RISK_HIGH}">Worst point: {worst["name"]}</strong>'
                    f'<span style="color:{KN_DIM};font-size:12px;"> &nbsp;·&nbsp; {type_label}</span>'
                    f'{note_str}</div>', unsafe_allow_html=True
                )

            if c["disruptions"]:
                st.markdown("**Active disruptions:** " + "  ".join(
                    [f"`{d.replace('_',' ')}`" for d in c["disruptions"]]
                ))

            # Forecast timeline table
            if timeline:
                st.markdown(
                    f'<div style="margin-top:12px;font-size:10px;font-weight:700;color:{KN_DIM};'
                    f'text-transform:uppercase;letter-spacing:1.5px;margin-bottom:6px;">24h Risk Forecast</div>',
                    unsafe_allow_html=True
                )
                st.markdown(f"""
                <table class="timeline-table">
                  <tr>
                    <th class="tl-now">Now</th>
                    <th>+6h</th>
                    <th>+12h</th>
                    <th>+24h</th>
                  </tr>
                  <tr>
                    <td class="tl-now {f"tl-{timeline.get('now','low')}"}">{timeline.get('now','?').upper()}</td>
                    {tl_cell(timeline.get('+6h','low'))}
                    {tl_cell(timeline.get('+12h','low'))}
                    {tl_cell(timeline.get('+24h','low'))}
                  </tr>
                </table>
                """, unsafe_allow_html=True)

            # Recommendation
            st.markdown(
                f'<div class="kn-rec" style="border-left:3px solid {bc.get(risk, KN_BORDER)};">'
                f'<strong style="color:{KN_WHITE}">Recommendation:</strong><br>'
                f'{c["recommendation"]}</div>', unsafe_allow_html=True
            )

            # Point-by-point breakdown
            all_pts = c.get("all_points", [])
            if all_pts:
                st.markdown(
                    f'<div style="margin-top:12px;font-size:10px;font-weight:700;color:{KN_DIM};'
                    f'text-transform:uppercase;letter-spacing:1.5px;padding-bottom:6px;'
                    f'border-bottom:1px solid {KN_BORDER};">Point-by-point assessment</div>',
                    unsafe_allow_html=True
                )
                for pt in all_pts:
                    pt_risk   = pt["risk_level"]
                    tag_class = ("wp-tag-hr" if pt["type"]=="high_risk" else "wp-tag-iv" if pt["type"]=="interval" else "wp-tag-ep")
                    tag_label = ("High-Risk Area" if pt["type"]=="high_risk" else "Interval" if pt["type"]=="interval" else "Hub")
                    w         = pt.get("weather", {})
                    temp_str  = f"{w.get('temp_c','?')}°C" if w.get("available") else "N/A"
                    wind_str  = f"{w.get('wind_kmh','?')} km/h" if w.get("available") else ""
                    st.markdown(
                        f'<div class="wp-row">'
                        f'<span class="wp-tag {tag_class}">{tag_label}</span>'
                        f'<span class="wp-name">{pt["name"]}</span>'
                        f'<span class="wp-risk-{pt_risk}">{pt_risk.upper()}</span>'
                        f'<span style="color:{KN_DIM};font-size:11px;margin-left:auto;">'
                        f'{temp_str} &nbsp; {wind_str}</span>'
                        f'</div>', unsafe_allow_html=True
                    )


# ── Hub temperatures ──────────────────────────────────────────────────────────
st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
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
                "Condition": w.get("condition","").replace("_"," ").title(),
            })
            seen.add(city)

if hub_temps:
    temp_df = pd.DataFrame(hub_temps).sort_values("Temp (°C)")
    colors  = [RISK_HIGH if t <= -5 else "#66CCFF" if t <= 0 else KN_BLUE for t in temp_df["Temp (°C)"]]
    fig3    = px.bar(temp_df, x="Hub", y="Temp (°C)", hover_data=["Wind (km/h)","Condition"], text="Temp (°C)")
    fig3.update_traces(marker_color=colors, texttemplate="%{text:.1f}°C",
                       textposition="outside", textfont=dict(color=KN_WHITE, size=12))
    fig3.update_layout(
        paper_bgcolor=KN_NAVY, plot_bgcolor=KN_CARD,
        font=dict(family="Source Sans 3", color=KN_LIGHT, size=13),
        showlegend=False, height=270, margin=dict(l=0, r=0, t=20, b=0),
        xaxis=dict(showgrid=False, tickangle=-30, tickfont=dict(color=KN_WHITE, size=12)),
        yaxis=dict(showgrid=True, gridcolor=KN_BORDER, zeroline=True,
                   zerolinecolor=KN_BORDER, tickfont=dict(color=KN_LIGHT, size=12)),
    )
    st.plotly_chart(fig3, use_container_width=True, config={"displayModeBar": False})

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="kn-footer">
  <span>Data: Open-Meteo (live + 24h forecast) &nbsp;·&nbsp; REST Countries &nbsp;·&nbsp; Nominatim (geocoding)</span>
  <span>Kuehne+Nagel &nbsp;·&nbsp; Road Logistics &nbsp;·&nbsp; Operational Excellence &nbsp;·&nbsp; v5</span>
</div>
""", unsafe_allow_html=True)
