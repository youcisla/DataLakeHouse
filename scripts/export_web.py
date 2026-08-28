# -*- coding: utf-8 -*-
"""
export_web.py : exporte les tables Gold en JSON pour l'interface web.
======================================================================
Le site Next.js est heberge sur Vercel : il n'a AUCUN acces reseau au cluster
local (HDFS, Kafka et Spark tournent sur localhost, derriere Docker). La donnee
doit donc voyager avec le site, sous forme d'instantane JSON.

Consequence assumee : le site affiche l'etat du datalake au moment du dernier
``make export-web``, et reste consultable meme cluster eteint, depuis
n'importe quel telephone, le jour de la soutenance. La date de l'export est
affichee dans l'interface pour que personne ne prenne un instantane pour du
temps reel.

Sortie : ``web/public/data/*.json`` (+ ``meta.json``), lisibles tels quels par
le build Next.js.

Usage :
    python scripts/export_web.py [--days 120] [--out web/public/data]
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import math
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "dashboard"))

logger = logging.getLogger("export_web")

#: Tables Gold exportees, avec leur nom de fichier JSON.
GOLD_TABLES: Dict[str, str] = {
    "daily_aggregates": "daily.json",
    "weekly_trends": "weekly.json",
    "extreme_events": "extremes.json",
    "climate_profile": "climate.json",
    "ml_predictions": "predictions.json",
}

#: Coordonnees des villes suivies (pour la carte du site).
CITY_COORDS: Dict[str, List[float]] = {
    "Paris": [48.8566, 2.3522],
    "Lyon": [45.7640, 4.8357],
    "Marseille": [43.2965, 5.3698],
    "Bordeaux": [44.8378, -0.5792],
    "Lille": [50.6292, 3.0573],
}


# ---------------------------------------------------------------------------
# Fonctions pures (testables sans HDFS ni pandas)
# ---------------------------------------------------------------------------

def clean_value(value: Any) -> Any:
    """
    Rend une valeur serialisable en JSON, sans jamais inventer de donnee.

    NaN, NaT et infini deviennent ``null`` : le front sait afficher un trou,
    il ne saurait pas deviner qu'un 0 est en realite une mesure manquante.
    """
    if value is None:
        return None
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return round(value, 3)
    if isinstance(value, (int, bool, str)):
        return value
    # dates, Timestamp, Decimal, numpy scalars...
    for attr in ("isoformat", "item"):
        if hasattr(value, attr):
            try:
                converted = getattr(value, attr)()
            except Exception:  # noqa: BLE001
                continue
            if attr == "item":
                return clean_value(converted)
            return str(converted)[:19]
    text = str(value)
    return None if text.lower() in ("nan", "nat", "none", "") else text


def clean_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Applique :func:`clean_value` a chaque champ de chaque ligne."""
    return [{key: clean_value(val) for key, val in row.items()} for row in records]


def city_marker(city: str, temperature: Any) -> Optional[Dict[str, Any]]:
    """Point de carte d'une ville, ou None si la ville n'a pas de coordonnees."""
    coords = CITY_COORDS.get(city)
    if not coords:
        return None
    return {"city": city, "lat": coords[0], "lon": coords[1],
            "temperature": clean_value(temperature)}


def summarize(daily: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    KPIs d'en-tete calcules sur le dernier jour disponible.

    Les valeurs manquantes sont ignorees (jamais comptees comme 0), et
    ``None`` est renvoye si aucune mesure n'est exploitable.
    """
    if not daily:
        return {"cities": 0, "observations": 0, "temp_avg": None,
                "precip_total": None, "last_day": None}

    last_day = max((row.get("dt") for row in daily if row.get("dt")), default=None)
    latest = [row for row in daily if row.get("dt") == last_day]

    temps = [row["temp_avg"] for row in latest
             if isinstance(row.get("temp_avg"), (int, float))]
    precip = [row["precip_sum"] for row in latest
              if isinstance(row.get("precip_sum"), (int, float))]
    observations = sum(row.get("n_obs") or 0 for row in daily
                       if isinstance(row.get("n_obs"), (int, float)))

    return {
        "cities": len({row.get("city") for row in latest if row.get("city")}),
        "observations": int(observations),
        "temp_avg": round(sum(temps) / len(temps), 1) if temps else None,
        "precip_total": round(sum(precip), 1) if precip else None,
        "last_day": last_day,
    }


# ---------------------------------------------------------------------------
# Lecture des tables Gold (WebHDFS, via le lecteur du dashboard)
# ---------------------------------------------------------------------------

def read_table(table: str, days: int) -> List[Dict[str, Any]]:
    """
    Lit une table Gold et la renvoie en liste de dicts.

    Une table absente (bonus ML pas encore execute, cluster eteint) renvoie une
    liste vide : l'export doit produire un site coherent, pas echouer.
    """
    try:
        import gold_reader
    except ImportError as exc:
        logger.error("gold_reader indisponible (%s) : export impossible.", exc)
        return []

    date_from = (dt.date.today() - dt.timedelta(days=days)).isoformat()
    try:
        frame = gold_reader.read_gold_table(table, date_from=date_from)
    except Exception as exc:  # noqa: BLE001 - table absente, HDFS muet...
        logger.warning("Table %s illisible (%s) : exportee vide.", table, exc)
        return []
    if frame is None or getattr(frame, "empty", True):
        logger.warning("Table %s vide.", table)
        return []
    return clean_records(frame.to_dict("records"))


def write_json(path: Path, payload: Any) -> int:
    """Ecrit un JSON compact et retourne sa taille en octets."""
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    path.write_text(text, encoding="utf-8")
    return len(text.encode("utf-8"))


def run(args: argparse.Namespace) -> int:
    out_dir = Path(args.out)
    if not out_dir.is_absolute():
        out_dir = ROOT / out_dir

    exported: Dict[str, int] = {}
    daily: List[Dict[str, Any]] = []

    for table, filename in GOLD_TABLES.items():
        rows = read_table(table, args.days)
        if table == "daily_aggregates":
            daily = rows
        exported[table] = len(rows)
        size = write_json(out_dir / filename, rows)
        logger.info("%-20s %6d ligne(s)  %8.1f Ko  -> %s",
                    table, len(rows), size / 1024, filename)

    # Bulletin IA du jour (texte libre, table a part).
    bulletin = read_table("ai_insights", args.days)
    write_json(out_dir / "bulletin.json", bulletin[-1] if bulletin else None)

    # Carte : derniere temperature connue par ville.
    last_day = max((row.get("dt") for row in daily if row.get("dt")), default=None)
    markers = [m for m in (
        city_marker(row.get("city"), row.get("temp_avg"))
        for row in daily if row.get("dt") == last_day) if m]
    write_json(out_dir / "map.json", markers)

    meta = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "window_days": args.days,
        "rows": exported,
        "cities": sorted({row.get("city") for row in daily if row.get("city")}),
        "summary": summarize(daily),
        "source_batch": "Météo-France : données climatologiques quotidiennes",
        "source_stream": "Open-Meteo (Kafka → Spark Structured Streaming)",
    }
    write_json(out_dir / "meta.json", meta)

    total = sum(exported.values())
    logger.info("Export termine : %d ligne(s) au total vers %s", total, out_dir)

    if total == 0 and not args.allow_empty:
        # Un site vide qui se deploie sans broncher est un faux vert : on
        # echoue bruyamment plutot que de publier une vitrine sans donnees.
        logger.error("AUCUNE ligne exportee : les tables Gold sont vides.")
        logger.error("Lancez d'abord `make all` (le pipeline doit avoir reussi), "
                     "puis relancez `make export-web`.")
        logger.error("Pour generer quand meme un site vide : "
                     "python export_web.py --allow-empty")
        return 1
    return 0


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Exporte les tables Gold en JSON pour le site Next.js")
    parser.add_argument("--days", type=int, default=int(os.environ.get("WEB_WINDOW_DAYS", "180")),
                        help="Profondeur d'historique exportee (jours).")
    parser.add_argument("--out", default="web/public/data",
                        help="Repertoire de sortie des JSON.")
    parser.add_argument("--allow-empty", action="store_true",
                        help="Ne pas echouer si les tables Gold sont vides.")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(levelname)s [export_web] %(message)s")
    return run(parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
