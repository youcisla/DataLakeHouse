# -*- coding: utf-8 -*-
"""
export_showcase.py : exporte les sorties Gold vers site/data.json (vitrine en ligne).
=====================================================================================
Lit les tables Gold sur HDFS (WebHDFS, via hdfs_utils) et produit un instantané
JSON consommé par la page statique site/index.html, déployable sur Vercel.

Lecture :
    - daily_aggregates  : dernier jour disponible (températures par ville)
    - extreme_events    : 7 derniers jours (événements extrêmes)
    - climate_profile   : profil climatique mensuel (petit volume)
    - /models/...       : métriques du dernier modèle XGBoost
    - ai_insights       : dernier bulletin météo généré

Si HDFS est indisponible ou vide, un instantané d'exemple est écrit : la page
reste fonctionnelle et affiche un état « en attente ».

Usage : python export_showcase.py [--out /opt/project/site/data.json]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__))))
import hdfs_utils  # noqa: E402

logger = logging.getLogger("export_showcase")

SITE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "site")
MODEL_PREFIX = "temperature_predictor_v"


def read_gold_table(table: str, max_partitions: Optional[int] = None) -> pd.DataFrame:
    """Lit une table Gold (parquet partitionné) via WebHDFS, bornée aux dernières partitions."""
    remote = "/gold/meteo/" + table
    if not hdfs_utils.hdfs_exists(remote):
        return pd.DataFrame()
    try:
        entries = hdfs_utils.hdfs_list(remote)
    except IOError:
        return pd.DataFrame()
    parts = sorted([e for e in entries if "=" in e])
    if max_partitions and len(parts) > max_partitions:
        parts = parts[-max_partitions:]

    frames: List[pd.DataFrame] = []
    with tempfile.TemporaryDirectory() as tmp:
        for part in parts:
            part_dir = remote + "/" + part
            try:
                files = hdfs_utils.hdfs_list(part_dir)
            except IOError:
                continue
            for fname in files:
                if not fname.endswith(".parquet"):
                    continue
                local = os.path.join(tmp, part + "__" + fname)
                try:
                    hdfs_utils.hdfs_download(part_dir + "/" + fname, local)
                    frames.append(pd.read_parquet(local))
                except (IOError, OSError) as exc:
                    logger.warning("Lecture %s impossible : %s", part_dir, exc)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def read_ml_metrics() -> Dict[str, Any]:
    """Métriques du dernier modèle XGBoost versionné sous /models."""
    empty: Dict[str, Any] = {
        "available": False, "model_version": None, "rmse": None, "mae": None,
        "r2": None, "beats_baseline": None, "trained_at": None,
    }
    if not hdfs_utils.hdfs_exists("/models"):
        return empty
    versions: List[int] = []
    try:
        names = hdfs_utils.hdfs_list("/models")
    except IOError:
        return empty
    for name in names:
        if name.startswith(MODEL_PREFIX) and name[len(MODEL_PREFIX):].isdigit():
            versions.append(int(name[len(MODEL_PREFIX):]))
    if not versions:
        return empty
    version = max(versions)
    try:
        metrics = hdfs_utils.hdfs_read_json("/models/" + MODEL_PREFIX + str(version) + "/metrics.json")
    except IOError:
        return empty
    return {
        "available": True,
        "model_version": version,
        "rmse": metrics.get("rmse_test"),
        "mae": metrics.get("mae_test"),
        "r2": metrics.get("r2_test"),
        "beats_baseline": metrics.get("beats_baseline"),
        "trained_at": metrics.get("trained_at"),
    }


def read_latest_bulletin() -> Dict[str, Any]:
    """Dernier bulletin généré sous /gold/meteo/ai_insights."""
    empty: Dict[str, Any] = {
        "available": False, "bulletin": "", "model": None, "generated_at": None,
    }
    remote = "/gold/meteo/ai_insights"
    if not hdfs_utils.hdfs_exists(remote):
        return empty
    try:
        entries = sorted([e for e in hdfs_utils.hdfs_list(remote) if e.startswith("dt=")])
    except IOError:
        return empty
    if not entries:
        return empty
    latest = entries[-1]
    try:
        bulletin = hdfs_utils.hdfs_read_json(remote + "/" + latest + "/bulletin.json")
    except IOError:
        return empty
    return {
        "available": True,
        "bulletin": bulletin.get("bulletin", ""),
        "model": bulletin.get("model"),
        "generated_at": bulletin.get("generated_at"),
    }


def _num(value: Any, digits: int = 1) -> Optional[float]:
    try:
        if value is None or (isinstance(value, float) and value != value):
            return None
        return round(float(value), digits)
    except (TypeError, ValueError):
        return None


def build_snapshot() -> Dict[str, Any]:
    """Construit l'instantané JSON à partir des sorties Gold réelles."""
    daily = read_gold_table("daily_aggregates", max_partitions=1)
    cities: List[Dict[str, Any]] = []
    if not daily.empty:
        daily = daily.copy()
        daily["dt"] = pd.to_datetime(daily["dt"], errors="coerce")
        daily = daily.dropna(subset=["dt"])
        if not daily.empty:
            last_dt = daily["dt"].max()
            last = daily[daily["dt"] == last_dt]
            for _, row in last.iterrows():
                cities.append({
                    "city": str(row.get("city", "")),
                    "temp_avg": _num(row.get("temp_avg")),
                    "temp_min": _num(row.get("temp_min")),
                    "temp_max": _num(row.get("temp_max")),
                    "precip_sum": _num(row.get("precip_sum")),
                    "wind_avg": _num(row.get("wind_avg")),
                })

    extreme = read_gold_table("extreme_events", max_partitions=7)
    events: List[Dict[str, Any]] = []
    if not extreme.empty:
        for _, row in extreme.head(20).iterrows():
            events.append({
                "dt": str(row.get("dt", "")),
                "city": str(row.get("city", "")),
                "event_type": str(row.get("event_type", "")),
                "severity": str(row.get("severity", "")),
                "value": _num(row.get("value")),
                "detail": str(row.get("detail", "")),
            })

    climate_df = read_gold_table("climate_profile")
    climate: List[Dict[str, Any]] = []
    if not climate_df.empty:
        for _, row in climate_df.head(60).iterrows():
            climate.append({
                "city": str(row.get("city", "")),
                "month": int(row.get("month", 0)) if row.get("month") is not None else 0,
                "season": str(row.get("season", "")),
                "temp_normal": _num(row.get("temp_normal")),
                "precip_avg": _num(row.get("precip_avg")),
                "rain_days": _num(row.get("rain_days"), 0),
            })

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source": "hdfs",
        "cities": cities,
        "extreme_events": events,
        "ml": read_ml_metrics(),
        "ai": read_latest_bulletin(),
        "climate": climate,
    }


PLACEHOLDER = {
    "generated_at": None,
    "source": "placeholder",
    "cities": [],
    "extreme_events": [],
    "ml": {"available": False, "model_version": None, "rmse": None, "mae": None,
           "r2": None, "beats_baseline": None, "trained_at": None},
    "ai": {"available": False,
           "bulletin": "En attente du premier run du pipeline : le bulletin météo sera généré automatiquement.",
           "model": None, "generated_at": None},
    "climate": [],
}


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Exporte les sorties Gold vers site/data.json")
    parser.add_argument("--out", default=os.path.join(SITE_DIR, "data.json"),
                        help="Chemin du fichier JSON de sortie")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s [export_showcase] %(message)s")

    try:
        snapshot = build_snapshot()
        if not snapshot["cities"] and not snapshot["climate"] and not snapshot["ml"]["available"]:
            logger.info("Aucune donnée Gold disponible : instantané d'exemple écrit.")
            snapshot = dict(PLACEHOLDER)
    except (IOError, OSError) as exc:
        logger.warning("HDFS injoignable (%s) : instantané d'exemple écrit.", exc)
        snapshot = dict(PLACEHOLDER)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(snapshot, fh, ensure_ascii=False, indent=2)
        fh.write(chr(10))
    logger.info("Instantané écrit : %s", args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
