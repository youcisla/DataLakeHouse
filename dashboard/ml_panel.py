# -*- coding: utf-8 -*-
"""
ml_panel.py : panneau « Prédictions ML » du dashboard Streamlit.
=================================================================

Affiche les KPIs (RMSE, MAE, prédictions J+1), les graphiques
« Prévisions vs réalité » et « Erreurs de prédiction », la confiance moyenne
par ville ainsi que le tableau des prochaines prédictions.

Auteur : Soufiane, équipe DataLake Météo
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


def render_ml_panel(reader) -> None:
    """Affiche le panneau des prédictions ML à partir de la table ml_predictions."""
    st.header("Prédictions ML")

    df = reader.read_gold_table("ml_predictions")
    if df.empty:
        st.info("Aucune prédiction ML disponible pour le moment. "
                "Lancez le DAG de prédiction (voir README).")
        return

    df = df.copy()
    if "dt" in df.columns:
        df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
        df = df.dropna(subset=["dt"])

    if df.empty:
        st.info("Aucune prédiction ML exploitable pour le moment.")
        return

    # Fenêtre des 14 derniers jours
    cutoff = df["dt"].max() - pd.Timedelta(days=14)
    recent = df[df["dt"] >= cutoff].copy()

    # ------------------------------------------------------------------
    # KPIs : RMSE, MAE, nombre de prédictions J+1
    # ------------------------------------------------------------------
    has_actual = "temp_actual" in recent.columns and recent["temp_actual"].notna().any()
    has_pred = "temp_predicted" in recent.columns and recent["temp_predicted"].notna().any()

    if has_actual and has_pred:
        valid = recent[recent["temp_actual"].notna() & recent["temp_predicted"].notna()]
        if not valid.empty:
            errors = valid["temp_predicted"] - valid["temp_actual"]
            rmse = float(np.sqrt((errors ** 2).mean()))
            mae = float(errors.abs().mean())
        else:
            rmse = float("nan")
            mae = float("nan")
    else:
        rmse = float("nan")
        mae = float("nan")

    n_future = int(recent["temp_actual"].isna().sum()) if "temp_actual" in recent.columns else 0

    col1, col2, col3 = st.columns(3)
    col1.metric("RMSE (°C)", f"{rmse:.2f}" if not np.isnan(rmse) else "n.d.")
    col2.metric("MAE (°C)", f"{mae:.2f}" if not np.isnan(mae) else "n.d.")
    col3.metric("Prédictions J+1 à venir", n_future)

    _render_predictions_vs_actual(recent)
    _render_errors(recent)
    _render_confidence(recent)
    _render_future_table(recent)


def _render_predictions_vs_actual(recent: pd.DataFrame) -> None:
    """Graphique comparant températures réelles et prédites par ville."""
    st.subheader("Prévisions vs réalité")
    if "city" not in recent.columns:
        st.info("Colonne 'city' absente : graphique impossible.")
        return

    fig = go.Figure()
    for city, grp in recent.groupby("city"):
        grp = grp.sort_values("dt")
        if "temp_actual" in grp.columns and grp["temp_actual"].notna().any():
            fig.add_trace(go.Scatter(
                x=grp["dt"], y=grp["temp_actual"], mode="lines+markers",
                name=f"{city} (réel)", line=dict(width=2),
            ))
        if "temp_predicted" in grp.columns and grp["temp_predicted"].notna().any():
            fig.add_trace(go.Scatter(
                x=grp["dt"], y=grp["temp_predicted"], mode="markers",
                name=f"{city} (prévu)", marker=dict(symbol="x", size=9),
            ))
    if not fig.data:
        st.info("Aucune donnée réelle/prédite à afficher.")
        return
    fig.update_layout(height=420, xaxis_title="Date", yaxis_title="Température (°C)",
                      legend_title_text="Série")
    st.plotly_chart(fig, use_container_width=True)


def _render_errors(recent: pd.DataFrame) -> None:
    """Graphique des erreurs absolues (bar chart) ou histogramme."""
    st.subheader("Erreurs de prédiction")
    if "error_abs" not in recent.columns:
        if {"temp_actual", "temp_predicted"}.issubset(recent.columns):
            recent = recent.copy()
            recent["error_abs"] = (recent["temp_predicted"] - recent["temp_actual"]).abs()
        else:
            st.info("Colonne 'error_abs' absente.")
            return
    err = recent[recent["error_abs"].notna()].copy()
    if err.empty:
        st.info("Pas encore d'erreur calculable (aucune température réelle disponible).")
        return
    err = err.sort_values("dt")
    if "city" in err.columns:
        fig = px.bar(err, x="dt", y="error_abs", color="city",
                     labels={"dt": "Date", "error_abs": "Erreur absolue (°C)", "city": "Ville"})
    else:
        fig = px.histogram(err, x="error_abs", nbins=30,
                           labels={"error_abs": "Erreur absolue (°C)"})
    fig.update_layout(height=350)
    st.plotly_chart(fig, use_container_width=True)


def _render_confidence(recent: pd.DataFrame) -> None:
    """Barres de progression de la confiance moyenne par ville."""
    st.subheader("Confiance moyenne par ville")
    if "confidence" not in recent.columns or not recent["confidence"].notna().any():
        st.info("Aucune information de confiance disponible.")
        return
    if "city" not in recent.columns:
        st.info("Colonne 'city' absente : indicateur impossible.")
        return
    for city, grp in recent.groupby("city"):
        conf = grp["confidence"].dropna()
        if conf.empty:
            continue
        mean_conf = float(conf.mean())
        # Gère une confiance exprimée en % (0-100) ou en fraction (0-1)
        display_conf = mean_conf / 100.0 if mean_conf > 1.0 else mean_conf
        st.markdown(f"**{city}**")
        st.progress(min(max(display_conf, 0.0), 1.0))
        if mean_conf <= 1.0:
            st.caption(f"Confiance moyenne : {mean_conf:.1%}")
        else:
            st.caption(f"Confiance moyenne : {mean_conf:.0f} %")


def _render_future_table(recent: pd.DataFrame) -> None:
    """Tableau des prochaines prédictions (temp_actual null → J+1)."""
    st.subheader("Prochaines prédictions (J+1)")
    if "temp_actual" not in recent.columns:
        st.info("Colonne 'temp_actual' absente.")
        return
    future = recent[recent["temp_actual"].isna()].copy()
    if future.empty:
        st.info("Aucune prédiction J+1 en attente.")
        return
    columns = ["dt", "city", "source", "temp_predicted", "model_version", "confidence"]
    available = [c for c in columns if c in future.columns]
    table = future[available].copy()
    if "dt" in table.columns:
        table["dt"] = table["dt"].dt.strftime("%d/%m/%Y")
    table["Échéance"] = "J+1"
    rename = {"dt": "Date", "city": "Ville", "source": "Source",
              "temp_predicted": "Temp. prévue (°C)", "model_version": "Modèle",
              "confidence": "Confiance"}
    table = table.rename(columns=rename)
    st.dataframe(table)
