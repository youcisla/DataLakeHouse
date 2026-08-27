# -*- coding: utf-8 -*-
"""
genai_panel.py — Panneau « Bulletin IA » du dashboard Streamlit.
=================================================================

Lit le dernier bulletin.json de la table Gold ai_insights et l'affiche
dans une carte stylisée (bulletin généré par le LLM local via Ollama,
avec repli « fallback »).

Auteur : Soufiane — Équipe DataLake Météo
"""

from __future__ import annotations

import html as _html
import json
import os
import tempfile

import streamlit as st

_AI_INSIGHTS_DIR = "/gold/meteo/ai_insights"


def _escape(text: str) -> str:
    """Échappe le texte pour un affichage HTML sûr."""
    return _html.escape(str(text))


def _latest_dt_entry(entries: list) -> str:
    """Retourne l'entrée de partition la plus récente (ex. dt=2025-03-15)."""
    dated = [e for e in entries if e.startswith("dt=")]
    if dated:
        return sorted(dated, reverse=True)[0]
    candidates = [e for e in entries if e and not e.startswith(("_", "."))]
    if candidates:
        return sorted(candidates, reverse=True)[0]
    return None


def _read_bulletin(reader, remote_file: str) -> dict:
    """Télécharge et parse bulletin.json ; retourne None en cas d'échec."""
    tmp_path = None
    try:
        fd, tmp_path = tempfile.mkstemp(suffix=".json", prefix="bulletin_")
        os.close(fd)
        reader.download_file(remote_file, tmp_path)
        with open(tmp_path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return None
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass


def render_genai_panel(reader) -> None:
    """Affiche le panneau « Bulletin IA » à partir de la table ai_insights."""
    st.header("Bulletin IA")

    try:
        entries = reader.webhdfs_list(_AI_INSIGHTS_DIR)
    except Exception:
        entries = []

    if not entries:
        st.info("Aucun bulletin IA disponible pour le moment. "
                "Lancez le DAG GenAI (voir README).")
        return

    latest = _latest_dt_entry(entries)
    if latest is None:
        st.info("Aucun bulletin IA disponible pour le moment.")
        return

    remote_file = f"{_AI_INSIGHTS_DIR}/{latest}/bulletin.json"
    bulletin = _read_bulletin(reader, remote_file)
    if bulletin is None:
        st.info("Impossible de lire bulletin.json — le fichier est peut-être en cours d'écriture.")
        return

    text = bulletin.get("bulletin", "")
    model = bulletin.get("model", "inconnu")
    source = bulletin.get("source", "inconnu")
    generated_at = bulletin.get("generated_at", "inconnu")
    dt = bulletin.get("dt", latest.replace("dt=", ""))

    card = f"""
    <div style="background: linear-gradient(135deg, #eef6ff 0%, #e8eef5 100%);
                border-left: 6px solid #0e7ac4; border-radius: 12px;
                padding: 22px 26px; margin: 10px 0 18px 0;
                box-shadow: 0 2px 8px rgba(15, 33, 55, 0.08);">
        <div style="font-size: 1.15rem; font-weight: 600; color: #0f2137; margin-bottom: 10px;">
            📋 Bulletin du {_escape(dt)}
        </div>
        <div style="white-space: pre-wrap; color: #0f2137; line-height: 1.65;">
            {_escape(text)}
        </div>
    </div>
    """
    st.markdown(card, unsafe_allow_html=True)
    st.caption(f"🤖 Modèle : {_escape(model)} · Source : {_escape(source)} · "
               f"Généré le : {_escape(generated_at)}")
