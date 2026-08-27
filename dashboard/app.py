# -*- coding: utf-8 -*-
"""
app.py — Application Streamlit « Dashboard Météo — DataLake ».
===============================================================

Panneaux :
    - Vue d'ensemble  (KPIs, carte de France, évolution, événements extrêmes)
    - Prédictions ML  (via ml_panel.py)
    - Bulletin IA     (via genai_panel.py)

Auteur : Soufiane — Équipe DataLake Météo
"""

from __future__ import annotations

import os
import sys
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st

# ---------------------------------------------------------------------------
# Chemins d'import robustes (Docker /opt/project ou lancement local)
# ---------------------------------------------------------------------------
_CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if _CURRENT_DIR not in sys.path:
    sys.path.insert(0, _CURRENT_DIR)
_SCRIPTS_DIR = os.path.join(os.path.dirname(_CURRENT_DIR), "scripts")
if os.path.isdir(_SCRIPTS_DIR) and _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

import gold_reader  # noqa: E402
from ml_panel import render_ml_panel  # noqa: E402
from genai_panel import render_genai_panel  # noqa: E402

# ---------------------------------------------------------------------------
# Constantes (coordonnées des 5 villes)
# ---------------------------------------------------------------------------
CITY_COORDS = {
    "Paris": {"lat": 48.8534, "lon": 2.3488},
    "Lyon": {"lat": 45.7640, "lon": 4.8357},
    "Marseille": {"lat": 43.2965, "lon": 5.3698},
    "Bordeaux": {"lat": 44.8378, "lon": -0.5792},
    "Lille": {"lat": 50.6292, "lon": 3.0573},
}


def _matches_city(series: pd.Series, city: str) -> pd.Series:
    """Comparaison de ville insensible à la casse et aux espaces."""
    return series.astype(str).str.strip().str.lower() == city.strip().lower()


def _select_source_row(grp: pd.DataFrame) -> pd.DataFrame:
    """Dans un groupe (ville, date), privilégie la source OPENMETEO."""
    if "source" not in grp.columns or grp.empty:
        return grp
    preferred = grp[grp["source"].astype(str).str.upper() == "OPENMETEO"]
    return preferred if not preferred.empty else grp


def _event_emoji(event_type: str) -> str:
    """Badge textuel (emoji) pour un type d'événement extrême."""
    e = str(event_type).lower()
    if any(k in e for k in ("heat", "chaleur", "canicule")):
        return "🔥 Chaleur"
    if any(k in e for k in ("cold", "froid", "freeze", "gel")):
        return "❄️ Froid"
    if any(k in e for k in ("snow", "neige")):
        return "🌨️ Neige"
    if any(k in e for k in ("wind", "vent", "storm", "tempete")):
        return "💨 Vent"
    if any(k in e for k in ("rain", "pluie", "precip", "flood", "inondation")):
        return "🌧️ Pluie"
    return f"⚠️ {event_type}"


def _event_badge_html(event_type: str) -> str:
    """Badge HTML coloré pour un type d'événement extrême."""
    e = str(event_type).lower()
    if any(k in e for k in ("heat", "chaleur", "canicule")):
        color, bg, icon = "#b71c1c", "#fdecea", "🔥"
    elif any(k in e for k in ("cold", "froid", "freeze", "gel")):
        color, bg, icon = "#0d47a1", "#e3f2fd", "❄️"
    elif any(k in e for k in ("snow", "neige")):
        color, bg, icon = "#4a148c", "#f3e5f5", "🌨️"
    elif any(k in e for k in ("wind", "vent", "storm", "tempete")):
        color, bg, icon = "#1b5e20", "#e8f5e9", "💨"
    else:
        color, bg, icon = "#e65100", "#fff3e0", "⚠️"
    return (f'<span style="background:{bg};color:{color};padding:2px 10px;'
            f'border-radius:12px;font-weight:600;">{icon} {event_type}</span>')


def _autorefresh_disabled() -> bool:
    """True si le paramètre d'URL autorefresh=false est présent."""
    try:
        return st.query_params.get("autorefresh") == "false"
    except Exception:
        try:
            params = st.experimental_get_query_params()
            return params.get("autorefresh", [""])[0] == "false"
        except Exception:
            return False


def _setup_autorefresh(interval_seconds: int) -> None:
    """Active l'auto-refresh, avec repli sur un bouton manuel."""
    if _autorefresh_disabled():
        st.caption("Auto-refresh désactivé (autorefresh=false).")
        if st.button("🔄 Rafraîchir"):
            st.rerun()
        return
    try:
        from streamlit_autorefresh import st_autorefresh
        st_autorefresh(interval=interval_seconds * 1000, key="refresh")
    except Exception:
        if st.button("🔄 Rafraîchir"):
            st.rerun()


def render_overview(config: dict) -> None:
    """Panneau « Vue d'ensemble »."""
    st.header("Vue d'ensemble")

    daily = gold_reader.read_gold_table("daily_aggregates")
    if daily.empty:
        st.info("Aucune donnée Gold pour le moment. Lancez les DAGs Airflow (voir README).")
        st.stop()

    daily = daily.copy()
    daily["dt"] = pd.to_datetime(daily["dt"], errors="coerce")
    daily = daily.dropna(subset=["dt"])
    if daily.empty:
        st.info("Aucune donnée Gold pour le moment. Lancez les DAGs Airflow (voir README).")
        st.stop()

    cities = (config.get("dashboard") or {}).get("villes") or list(CITY_COORDS.keys())
    last_dt = daily["dt"].max()
    last_day = daily[daily["dt"] == last_dt]

    st.subheader(f"Températures au {last_dt.strftime('%d/%m/%Y')}")

    cols = st.columns(len(cities))
    for col, city in zip(cols, cities):
        if "city" in last_day.columns:
            grp = last_day[_matches_city(last_day["city"], city)]
        else:
            grp = pd.DataFrame()
        if grp.empty:
            col.metric(city, "—", help="Aucune donnée pour cette date")
            continue
        row = _select_source_row(grp).iloc[0]
        temp_avg = row.get("temp_avg")
        temp_min = row.get("temp_min")
        temp_max = row.get("temp_max")
        value = f"{float(temp_avg):.1f} °C" if pd.notna(temp_avg) else "—"
        col.metric(city, value)
        parts = []
        if pd.notna(temp_min):
            parts.append(f"min {float(temp_min):.1f} °C")
        if pd.notna(temp_max):
            parts.append(f"max {float(temp_max):.1f} °C")
        if parts:
            col.caption(" · ".join(parts))

    _render_map(last_day, cities)
    _render_temperature_trend(daily)
    _render_extreme_events(last_dt)


def _render_map(last_day: pd.DataFrame, cities: list) -> None:
    """Carte de France des températures (scatter mapbox open-street-map)."""
    st.subheader("Carte de France — températures du jour")
    if "city" not in last_day.columns:
        st.info("Colonne 'city' absente — carte impossible.")
        return
    records = []
    for city in cities:
        coord = CITY_COORDS.get(city)
        if coord is None:
            continue
        grp = last_day[_matches_city(last_day["city"], city)]
        if grp.empty:
            continue
        row = _select_source_row(grp).iloc[0]
        temp = row.get("temp_avg")
        if pd.notna(temp):
            records.append({
                "city": city,
                "lat": coord["lat"],
                "lon": coord["lon"],
                "temp_avg": float(temp),
            })
    if not records:
        st.info("Aucune température à cartographier pour le moment.")
        return
    map_df = pd.DataFrame(records)
    fig = px.scatter_mapbox(
        map_df,
        lat="lat",
        lon="lon",
        color="temp_avg",
        size="temp_avg",
        hover_name="city",
        color_continuous_scale="RdBu_r",
        zoom=5,
        mapbox_style="open-street-map",
        labels={"temp_avg": "Température (°C)"},
        size_max=35,
    )
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0}, height=450)
    st.plotly_chart(fig, use_container_width=True)


def _render_temperature_trend(daily: pd.DataFrame) -> None:
    """Évolution des températures sur 30 jours (source OPENMETEO)."""
    st.subheader("Évolution des températures — 30 derniers jours (Open-Meteo)")
    last_dt = daily["dt"].max()
    window = daily[daily["dt"] >= (last_dt - pd.Timedelta(days=30))]
    if "source" in window.columns:
        window = window[window["source"].astype(str).str.upper() == "OPENMETEO"]
    if window.empty:
        st.info("Pas de données OPENMETEO sur les 30 derniers jours.")
        return
    window = window.sort_values("dt")
    fig = px.line(
        window,
        x="dt",
        y="temp_avg",
        color="city",
        markers=True,
        labels={"dt": "Date", "temp_avg": "Température moyenne (°C)", "city": "Ville"},
    )
    fig.update_layout(height=400, legend_title_text="Ville")
    st.plotly_chart(fig, use_container_width=True)


def _render_extreme_events(last_dt: pd.Timestamp) -> None:
    """Tableau des événements extrêmes des 7 derniers jours (badges colorés)."""
    st.subheader("Événements extrêmes — 7 derniers jours")
    events = gold_reader.read_gold_table("extreme_events")
    if events.empty or "dt" not in events.columns:
        st.info("Aucun événement extrême disponible pour le moment.")
        return
    events = events.copy()
    events["dt"] = pd.to_datetime(events["dt"], errors="coerce")
    events = events.dropna(subset=["dt"])
    events = events[events["dt"] >= (last_dt - pd.Timedelta(days=7))]
    events = events.sort_values("dt", ascending=False)
    if events.empty:
        st.info("Aucun événement extrême sur les 7 derniers jours.")
        return

    columns = ["dt", "city", "source", "event_type", "severity", "value", "detail"]
    available = [c for c in columns if c in events.columns]
    table = events[available].copy()
    table["dt"] = table["dt"].dt.strftime("%d/%m/%Y")
    if "event_type" in table.columns:
        table["Type"] = table["event_type"].apply(_event_emoji)

    st.dataframe(table)

    legend = " ".join(
        _event_badge_html(name)
        for name in sorted(events["event_type"].dropna().astype(str).unique())
    )
    st.markdown(legend, unsafe_allow_html=True)


def main() -> None:
    """Point d'entrée de l'application Streamlit."""
    config = gold_reader.load_config()
    theme = config.get("theme") or {}
    title = (config.get("dashboard") or {}).get("title", "🌤️ Dashboard Météo — DataLake")
    interval = int((config.get("refresh") or {}).get("interval_seconds", 30))

    st.set_page_config(page_title=title, layout="wide", page_icon="🌤️")

    background_color = theme.get("backgroundColor", "#f7f9fc")
    st.markdown(
        f"<style>.stApp {{ background-color: {background_color}; }}</style>",
        unsafe_allow_html=True,
    )

    st.sidebar.title("🌤️ DataLake Météo")
    page = st.sidebar.radio(
        "Navigation",
        ["Vue d'ensemble", "Prédictions ML", "Bulletin IA"],
    )

    _setup_autorefresh(interval)
    st.caption(f"Dernier rafraîchissement : {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")

    if page == "Vue d'ensemble":
        render_overview(config)
    elif page == "Prédictions ML":
        render_ml_panel(gold_reader)
    else:
        render_genai_panel(gold_reader)


if __name__ == "__main__":
    main()
