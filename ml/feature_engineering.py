# -*- coding: utf-8 -*-
"""
feature_engineering.py — Ingénierie de features météo (DataLake Météo).
=======================================================================
Transforme les données Silver (observations par localisation) en un jeu de
features tabulaires prêt pour l'entraînement d'un modèle de prévision de
température à J+1 (24 h).

La fonction :func:`build_features` est PURE : elle n'effectue aucune E/S
(lecture HDFS, Spark, réseau) et peut donc être testée unitairement sans
cluster. Le CLI (`python feature_engineering.py`) lit la couche Silver via
Spark, applique `build_features`, puis téléverse le résultat sur HDFS via
`hdfs_utils`.

Auteur : Sara — Équipe DataLake Météo
"""

from __future__ import annotations

import argparse
import logging
import os
import tempfile
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder

import hdfs_utils

logger = logging.getLogger(__name__)

# Colonnes Silver minimales attendues en entrée de build_features.
REQUIRED_COLUMNS: List[str] = [
    "city", "station_id", "timestamp", "temperature",
    "precipitation", "wind_speed", "source", "dt",
]

# Correspondance mois -> saison (saisons météorologiques, hémisphère nord).
_SEASON_BY_MONTH: Dict[int, str] = {
    12: "hiver", 1: "hiver", 2: "hiver",
    3: "printemps", 4: "printemps", 5: "printemps",
    6: "été", 7: "été", 8: "été",
    9: "automne", 10: "automne", 11: "automne",
}


def _engineer_location(group: pd.DataFrame) -> pd.DataFrame:
    """Calcule les features séquentielles d'UNE localisation (triée par temps)."""
    group = group.sort_values("timestamp").reset_index(drop=True)
    out = group.copy()

    temp = out["temperature"]

    # Lags (valeurs passées) : température à J-1, J-2 et J-7.
    out["temp_lag1"] = temp.shift(1)
    out["temp_lag2"] = temp.shift(2)
    out["temp_lag7"] = temp.shift(7)

    # Moyennes mobiles CENTRÉES (fenêtre symétrique), comme demandé par le
    # cahier des charges. NOTE : une fenêtre centrée inclut de l'information
    # future ; pour un pipeline strictement causal, utiliser center=False.
    out["temp_ma3"] = temp.rolling(window=3, center=True).mean()
    out["temp_ma7"] = temp.rolling(window=7, center=True).mean()
    out["temp_std7"] = temp.rolling(window=7, center=True).std()

    # Anomalie de température par rapport à la moyenne mobile 7 jours.
    out["temp_anomaly"] = temp - out["temp_ma7"]

    # Agrégats passés (fenêtre glissante causale).
    out["precip_sum3"] = out["precipitation"].rolling(window=3).sum()
    out["wind_ma7"] = out["wind_speed"].rolling(window=7).mean()

    # Cible : température à J+1 (prévision à 24 h).
    out["target"] = temp.shift(-1)

    return out


def build_features(
    df: pd.DataFrame,
    *,
    drop_target_na: bool = True,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Construit le jeu de features ML à partir des données Silver.

    Paramètres
    ----------
    df : pd.DataFrame
        Données Silver avec au minimum les colonnes de REQUIRED_COLUMNS.
    drop_target_na : bool
        Si True (défaut, utilisé à l'entraînement), supprime les lignes sans
        cible (dernier point de chaque série). Si False (utilisé à l'inférence),
        conserve ces lignes afin de pouvoir prédire le futur (cible inconnue).

    Retour
    ------
    (df_features, encoders) : tuple
        - df_features : DataFrame enrichi (lags, moyennes mobiles, features
          temporelles, colonne `location`, encodage et cible `target`).
        - encoders : dictionnaire des encodeurs ajustés (ex. {"city": ...}).
    """
    if df is None or df.empty:
        raise ValueError("Le DataFrame d'entrée est vide ou None.")

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Colonnes Silver manquantes : {missing}")

    data = df.copy()
    data["timestamp"] = pd.to_datetime(data["timestamp"])
    data["source"] = data["source"].astype(str).str.upper()

    # Clé de localisation : station_id pour NOAA, ville pour Open-Meteo.
    data["location"] = np.where(
        data["source"] == "NOAA",
        data["station_id"].astype(str),
        data["city"].astype(str),
    )

    data = data.sort_values(["location", "timestamp"]).reset_index(drop=True)

    # Features séquentielles, calculées indépendamment par localisation.
    pieces: List[pd.DataFrame] = []
    for _, group in data.groupby("location", sort=False):
        pieces.append(_engineer_location(group))
    data = pd.concat(pieces, ignore_index=True)

    # Features temporelles dérivées du timestamp.
    ts = data["timestamp"].dt
    data["month"] = ts.month.astype(int)
    data["dayofyear"] = ts.dayofyear.astype(int)
    data["dayofweek"] = ts.dayofweek.astype(int)
    data["is_weekend"] = (ts.dayofweek >= 5).astype(int)
    data["season"] = data["month"].map(_SEASON_BY_MONTH)
    data["year"] = ts.year.astype(int)

    # Encodage de la ville (mapping conservé pour la traçabilité).
    city_encoder = LabelEncoder()
    data["city_encoded"] = city_encoder.fit_transform(data["city"].astype(str))
    encoders: Dict[str, Any] = {"city": city_encoder}

    # Toujours retirer les lignes sans historique suffisant (< 7 jours).
    data = data.dropna(subset=["temp_lag7"])
    # À l'entraînement, retirer également les lignes sans cible (J+1).
    if drop_target_na:
        data = data.dropna(subset=["target"])

    return data.reset_index(drop=True), encoders


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _build_spark_session():
    """Crée une session Spark (import paresseux : le module s'importe sans Spark)."""
    from pyspark.sql import SparkSession  # pylint: disable=import-outside-toplevel
    return SparkSession.builder.appName("feature_engineering_meteo").getOrCreate()


def main(argv: Optional[List[str]] = None) -> int:
    """Point d'entrée CLI : Silver HDFS -> features Parquet HDFS."""
    parser = argparse.ArgumentParser(
        description="Ingénierie de features météo (Silver -> features Parquet HDFS)."
    )
    parser.add_argument("--days", type=int, default=730,
                        help="Nombre de jours d'historique à traiter (défaut : 730).")
    parser.add_argument("--max-locations", type=int, default=200,
                        help="Nombre maximal de localisations (défaut : 200).")
    parser.add_argument("--output", default="/features/meteo/features.parquet",
                        help="Chemin HDFS de sortie (défaut : /features/meteo/features.parquet).")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    from pyspark.sql import functions as F  # pylint: disable=import-outside-toplevel

    spark = _build_spark_session()

    df = spark.read.parquet("/silver/meteo")

    # Localisation calculée côté Spark pour sélectionner les plus riches.
    df = df.withColumn(
        "location",
        F.when(F.col("source") == "NOAA", F.col("station_id").cast("string"))
         .otherwise(F.col("city").cast("string")),
    )

    # Fenêtre temporelle : dt >= max(dt) - days.
    max_dt = df.select(F.max("dt").alias("max_dt")).collect()[0]["max_dt"]
    cutoff = (datetime.strptime(max_dt, "%Y-%m-%d") - timedelta(days=args.days)).strftime("%Y-%m-%d")
    logger.info("Fenêtre temporelle : dt >= %s (max(dt) = %s)", cutoff, max_dt)
    df = df.filter(F.col("dt") >= cutoff)

    # Sélection des --max-locations localisations ayant le plus d'observations.
    top_locations = [
        row["location"]
        for row in df.groupBy("location").count()
                   .orderBy(F.desc("count")).limit(args.max_locations)
                   .select("location").collect()
    ]
    df = df.filter(F.col("location").isin(top_locations))
    logger.info("Localisations sélectionnées : %d", len(top_locations))

    pdf = df.toPandas()
    logger.info("Lignes Silver lues : %d", len(pdf))

    features, _encoders = build_features(pdf)

    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = os.path.join(tmpdir, "features.parquet")
        features.to_parquet(local_path, index=False)
        hdfs_utils.hdfs_upload(local_path, args.output)

    print(f"Lignes (features) : {len(features)}")
    print(f"Localisations     : {features['location'].nunique()}")
    print(f"Colonnes produites: {list(features.columns)}")
    print(f"Output HDFS       : {args.output}")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
