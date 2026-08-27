# -*- coding: utf-8 -*-
"""
genai_summary.py : bulletin météo génératif (DataLake Météo).
==============================================================
Génère un bulletin météo en français (style présentateur professionnel) à partir
des agrégats Gold (daily_aggregates), des événements extrêmes (extreme_events) et
des prédictions ML (ml_predictions). Le texte est produit par Ollama (LLM local) ;
en cas d'échec ou de réponse vide, un bulletin de repli rédigé par règles est
généré.

Résultat : /gold/meteo/ai_insights/dt={date}/bulletin.json (+ marqueur _SUCCESS).

Auteur : Sara, équipe DataLake Météo
"""

from __future__ import annotations

import argparse
import logging
import os
import tempfile
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import pandas as pd
import requests

import hdfs_utils

logger = logging.getLogger(__name__)

OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://ollama:11434")
LLM_MODEL = os.environ.get("LLM_MODEL", "llama3.2:3b")
OLLAMA_TIMEOUT = 120


# ---------------------------------------------------------------------------
# Chargement des données Gold via WebHDFS
# ---------------------------------------------------------------------------
def download_parquet_dir(remote_dir: str, local_dir: str) -> pd.DataFrame:
    """
    Télécharge tous les fichiers .parquet d'un répertoire HDFS et les concatène.

    Retourne un DataFrame vide si le répertoire n'existe pas ou ne contient
    aucun fichier Parquet.
    """
    if not hdfs_utils.hdfs_exists(remote_dir):
        logger.warning("Répertoire HDFS absent : %s", remote_dir)
        return pd.DataFrame()

    os.makedirs(local_dir, exist_ok=True)
    frames: List[pd.DataFrame] = []
    for name in sorted(hdfs_utils.hdfs_list(remote_dir)):
        if not name.endswith(".parquet"):
            continue
        local_path = os.path.join(local_dir, name)
        hdfs_utils.hdfs_download(f"{remote_dir}/{name}", local_path)
        frames.append(pd.read_parquet(local_path))

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _concat(frames: List[pd.DataFrame]) -> pd.DataFrame:
    """Concatène des DataFrames potentiellement vides en ignorant les vides."""
    non_empty = [f for f in frames if f is not None and not f.empty]
    return pd.concat(non_empty, ignore_index=True) if non_empty else pd.DataFrame()


# ---------------------------------------------------------------------------
# Construction du prompt / bulletin
# ---------------------------------------------------------------------------
def _available(df: pd.DataFrame, cols: List[str]) -> List[str]:
    """Retourne les colonnes de 'cols' réellement présentes dans df."""
    return [c for c in cols if c in df.columns]


def _build_prompt(daily: pd.DataFrame, events: pd.DataFrame, preds: pd.DataFrame) -> str:
    """Construit un prompt français structuré pour le LLM."""
    lines: List[str] = [
        "Tu es un présentateur météo professionnel francophone.",
        "Rédige un bulletin météo en français, clair et structuré, à partir des données ci-dessous.",
        "",
    ]

    lines.append("### CONDITIONS OBSERVÉES (2 derniers jours) ###")
    if daily.empty:
        lines.append("(aucune donnée d'agrégat disponible)")
    else:
        cols = _available(daily, ["dt", "city", "temp_avg", "temp_min", "temp_max",
                                  "precip_sum", "wind_avg", "snow_sum"])
        d = daily[cols].copy()
        for c in ["temp_avg", "temp_min", "temp_max", "precip_sum", "wind_avg", "snow_sum"]:
            if c in d.columns:
                d[c] = d[c].round(2)
        lines.append(d.to_string(index=False))
    lines.append("")

    lines.append("### ÉVÉNEMENTS NOTABLES (2 derniers jours) ###")
    if events.empty:
        lines.append("Aucun événement notable.")
    else:
        cols = _available(events, ["dt", "city", "event_type", "severity", "value", "threshold", "detail"])
        lines.append(events[cols].to_string(index=False))
    lines.append("")

    lines.append("### PRÉVISIONS 24H (modèle ML) ###")
    if preds.empty:
        lines.append("(aucune prédiction ML disponible)")
    else:
        cols = _available(preds, ["dt", "city", "temp_predicted", "confidence", "temp_actual"])
        p = preds[cols].copy()
        for c in ["temp_predicted", "temp_actual"]:
            if c in p.columns:
                p[c] = p[c].round(2)
        if "confidence" in p.columns:
            p["confidence"] = p["confidence"].round(3)
        lines.append(p.to_string(index=False))
    lines.append("")

    lines += [
        "Consignes de rédaction :",
        "- Résume les conditions par ville : température, précipitations, vent.",
        "- Mentionne les événements notables s'il y en a.",
        "- Donne la tendance des 24 prochaines heures à partir des prédictions ML.",
        "- Ton professionnel, phrases complètes, sans inventer de chiffres.",
    ]
    return "\n".join(lines)


def _fmt(value: object, ndigits: int = 1) -> str:
    """Formate un nombre (ou NaN/None) en chaîne lisible."""
    try:
        v = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return "N/A"
    if pd.isna(v):
        return "N/A"
    return f"{v:.{ndigits}f}"


def _fallback_bulletin(
    daily: pd.DataFrame,
    events: pd.DataFrame,
    preds: pd.DataFrame,
    date: str,
) -> str:
    """Bulletin de repli rédigé par règles (aucun appel LLM)."""
    lines: List[str] = ["Bulletin automatique (fallback)", ""]
    lines.append(f"Bulletin météo du {date} :")

    # Conditions observées par ville (jour J en priorité).
    if daily.empty:
        lines.append("- Données d'agrégat indisponibles pour cette date.")
    else:
        focus = daily[daily["dt"] == date] if (daily["dt"] == date).any() else daily
        lines.append("- Conditions observées :")
        for _, row in focus.iterrows():
            city = row.get("city", "?")
            temp_avg = _fmt(row.get("temp_avg"))
            temp_min = _fmt(row.get("temp_min"))
            temp_max = _fmt(row.get("temp_max"))
            precip = _fmt(row.get("precip_sum"))
            wind = _fmt(row.get("wind_avg"))
            lines.append(
                f"  • {city} : {temp_avg} °C en moyenne (min {temp_min} / max {temp_max}), "
                f"précipitations {precip} mm, vent moyen {wind} km/h."
            )

    # Événements notables.
    if events.empty:
        lines.append("- Aucun événement météo notable signalé.")
    else:
        lines.append("- Événements notables :")
        for _, row in events.iterrows():
            etype = row.get("event_type", "événement")
            city = row.get("city", "?")
            sev = row.get("severity", "?")
            detail = row.get("detail", "")
            lines.append(f"  • {etype} ({sev}) à {city} : {detail}".rstrip())

    # Prévisions 24h.
    if preds.empty:
        lines.append("- Prévisions 24h : aucune prédiction ML disponible.")
    else:
        lines.append("- Prévisions 24h (modèle ML) :")
        for _, row in preds.iterrows():
            city = row.get("city", "?")
            pred = _fmt(row.get("temp_predicted"))
            conf = _fmt(row.get("confidence"), ndigits=2)
            lines.append(f"  • {city} : température prévue {pred} °C (confiance {conf}).")

    lines.append("")
    lines.append("(Bulletin généré automatiquement sans modèle de langage.)")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Appel Ollama
# ---------------------------------------------------------------------------
def _call_ollama(prompt: str) -> Optional[str]:
    """Appelle Ollama /api/generate et retourne le texte, ou None en cas d'échec."""
    payload = {
        "model": LLM_MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0.7},
    }
    try:
        resp = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json=payload,
            timeout=OLLAMA_TIMEOUT,
        )
        resp.raise_for_status()
        text = (resp.json().get("response") or "").strip()
        return text or None
    except Exception as exc:  # noqa: BLE001 - tout échec bascule en fallback
        logger.warning("Échec de l'appel Ollama (%s) : %s", OLLAMA_URL, exc)
        return None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main(argv: Optional[List[str]] = None) -> int:
    """Point d'entrée CLI : génère et publie le bulletin d'une date donnée."""
    parser = argparse.ArgumentParser(
        description="Génère un bulletin météo (LLM ou fallback) pour une date."
    )
    parser.add_argument("--date", default=None,
                        help="Date du bulletin (YYYY-MM-DD, défaut : aujourd'hui).")
    parser.add_argument("--force", action="store_true",
                        help="Régénère le bulletin même si _SUCCESS existe déjà.")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    date = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    prev_date = (datetime.strptime(date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")

    insights_dir = f"/gold/meteo/ai_insights/dt={date}"

    # Idempotence : ne rien réécrire si le marqueur _SUCCESS existe déjà.
    if hdfs_utils.has_success(insights_dir) and not args.force:
        logger.info("Bulletin déjà généré pour %s (utilisez --force pour régénérer).", date)
        return 0

    with tempfile.TemporaryDirectory() as tmpdir:
        daily = _concat([
            download_parquet_dir(f"/gold/meteo/daily_aggregates/dt={date}", os.path.join(tmpdir, "daily_j")),
            download_parquet_dir(f"/gold/meteo/daily_aggregates/dt={prev_date}", os.path.join(tmpdir, "daily_j1")),
        ])
        events = _concat([
            download_parquet_dir(f"/gold/meteo/extreme_events/dt={date}", os.path.join(tmpdir, "events_j")),
            download_parquet_dir(f"/gold/meteo/extreme_events/dt={prev_date}", os.path.join(tmpdir, "events_j1")),
        ])
        preds = download_parquet_dir(f"/gold/meteo/ml_predictions/dt={date}", os.path.join(tmpdir, "preds_j"))

        prompt = _build_prompt(daily, events, preds)

        bulletin = _call_ollama(prompt)
        source = "ollama"
        model_used = LLM_MODEL
        if bulletin is None:
            bulletin = _fallback_bulletin(daily, events, preds, date)
            source = "fallback"
            model_used = "fallback"

    payload = {
        "dt": date,
        "bulletin": bulletin,
        "model": model_used,
        "source": source,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    hdfs_utils.hdfs_write_json(f"{insights_dir}/bulletin.json", payload)
    hdfs_utils.write_success(insights_dir)

    print(f"Bulletin publié : {insights_dir}/bulletin.json")
    print(f"Source : {source} | Modèle : {model_used}")
    print("-" * 60)
    print(bulletin)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
