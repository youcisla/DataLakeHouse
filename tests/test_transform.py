# -*- coding: utf-8 -*-
"""
test_transform.py — Tests unitaires SANS Spark des transformations Silver/Gold.
===============================================================================
Ces tests n'importent ni Spark ni HDFS : ils valident uniquement les fonctions
pures de silver_transform et gold_transform (avec pandas pour la logique de
déduplication).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

# Rend les scripts importables (équivalent PYTHONPATH=/opt/project/scripts).
SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import silver_transform
import gold_transform


# ---------------------------------------------------------------------------
# silver_transform — fonctions pures
# ---------------------------------------------------------------------------

def test_parse_city_country():
    assert silver_transform.parse_city_country("PARIS  , FR") == ("Paris", "FR")
    assert silver_transform.parse_city_country("LYON-BRON , FR") == ("Lyon-Bron", "FR")
    # Sans virgule : nom nettoyé seul, pas de pays.
    assert silver_transform.parse_city_country("MARSEILLE") == ("Marseille", "")
    assert silver_transform.parse_city_country("") == ("", "")
    # Espaces et casse.
    assert silver_transform.parse_city_country("  nice , fr  ") == ("Nice", "FR")


def test_is_missing():
    for value in [-9999, -999, 9999, None]:
        assert silver_transform.is_missing(value) is True
    for value in [0, 22.5]:
        assert silver_transform.is_missing(value) is False


def test_to_celsius():
    assert silver_transform.to_celsius(220) == 22.0
    assert silver_transform.to_celsius(-9999) is None
    assert silver_transform.to_celsius(5) == 0.5


def test_validate_required_columns():
    required = ["STATION", "NAME", "DATE"]
    assert silver_transform.validate_required_columns(["STATION"], required) == ["NAME", "DATE"]
    assert silver_transform.validate_required_columns(["STATION", "NAME", "DATE"], required) == []


def test_deduplication():
    df = pd.DataFrame({
        "station_id": ["A", "A", "B", "C", "C"],
        "timestamp": ["2025-01-01", "2025-01-01", "2025-01-01", "2025-01-02", "2025-01-02"],
        "temperature": [10.0, 10.5, 20.0, 30.0, 30.1],
    })
    assert len(df) == 5

    deduped = df.drop_duplicates(subset=silver_transform.dedup_keys())
    assert len(deduped) == 3
    assert deduped.duplicated(subset=silver_transform.dedup_keys()).sum() == 0


def test_silver_schema_columns():
    required = [
        "station_id", "station_name", "city", "country",
        "latitude", "longitude", "elevation", "timestamp",
        "temperature", "precipitation", "wind_speed", "snow",
        "source", "dt",
    ]
    for col in required:
        assert col in silver_transform.SILVER_SCHEMA, f"Colonne manquante : {col}"


# ---------------------------------------------------------------------------
# gold_transform — fonctions pures
# ---------------------------------------------------------------------------

THRESHOLDS = {
    "heatwave": 35.0,
    "heavy_rain": 20.0,
    "strong_wind": 20.0,
    "cold_snap": -10.0,
}


def _row(city="Paris", dt="2025-07-15", temp_max=20.0, temp_min=8.0,
         precip_sum=0.0, wind_avg=5.0):
    return {
        "city": city,
        "dt": dt,
        "temp_max": temp_max,
        "temp_min": temp_min,
        "precip_sum": precip_sum,
        "wind_avg": wind_avg,
    }


def test_classify_extreme():
    # Canicule en alerte (37.2 < 1.25 * 35 = 43.75).
    events = gold_transform.classify_extreme(_row(temp_max=37.2), THRESHOLDS)
    assert len(events) == 1
    assert events[0]["event_type"] == "canicule"
    assert events[0]["severity"] == "alerte"
    assert events[0]["value"] == 37.2
    assert events[0]["threshold"] == 35.0

    # Canicule extrême (>= 43.75).
    events = gold_transform.classify_extreme(_row(temp_max=45.0), THRESHOLDS)
    assert events[0]["severity"] == "extreme"

    # Fortes pluies.
    events = gold_transform.classify_extreme(_row(precip_sum=25.0), THRESHOLDS)
    assert any(e["event_type"] == "fortes_pluies" for e in events)

    # Vents violents.
    events = gold_transform.classify_extreme(_row(wind_avg=22.0), THRESHOLDS)
    assert any(e["event_type"] == "vents_violents" for e in events)

    # Vague de froid en alerte (-11 > -12.5).
    events = gold_transform.classify_extreme(_row(temp_min=-11.0), THRESHOLDS)
    cold = [e for e in events if e["event_type"] == "vague_de_froid"]
    assert cold and cold[0]["severity"] == "alerte"

    # Vague de froid extrême (-13 <= -12.5).
    events = gold_transform.classify_extreme(_row(temp_min=-13.0), THRESHOLDS)
    cold = [e for e in events if e["event_type"] == "vague_de_froid"]
    assert cold and cold[0]["severity"] == "extreme"

    # Aucun événement.
    assert gold_transform.classify_extreme(_row(), THRESHOLDS) == []


def test_linear_slope():
    # y = 2x + 1 -> pente = 2.
    xs = [0.0, 1.0, 2.0, 3.0]
    ys = [1.0, 3.0, 5.0, 7.0]
    assert abs(gold_transform.linear_slope(xs, ys) - 2.0) < 1e-9
    # n < 2 -> 0.
    assert gold_transform.linear_slope([1.0], [2.0]) == 0.0
    assert gold_transform.linear_slope([], []) == 0.0
