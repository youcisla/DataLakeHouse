# -*- coding: utf-8 -*-
"""
inference.py : inférence de température à J+1 (DataLake Météo).
================================================================
Charge le meilleur modèle (`/models/temperature_predictor_v{N}`), construit les
features sur les `--days` derniers jours de Silver, prédit la température du
lendemain pour chaque localisation, puis écrit les prédictions dans
`/gold/meteo/ml_predictions` (partitionné par `dt`, overwrite dynamique).

Auteur : Sara, équipe DataLake Météo
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import tempfile
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import joblib
import numpy as np
import pandas as pd

import hdfs_utils

# ml/ n'est pas dans PYTHONPATH : on ajoute le répertoire du script et son parent
# afin de pouvoir importer "ml.feature_engineering".
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)
_PARENT_DIR = os.path.dirname(_SCRIPT_DIR)
if _PARENT_DIR not in sys.path:
    sys.path.insert(0, _PARENT_DIR)

from ml.feature_engineering import build_features  # noqa: E402

logger = logging.getLogger(__name__)

MODEL_PREFIX = "temperature_predictor_v"
GOLD_PREDICTIONS = "/gold/meteo/ml_predictions"


def _build_spark_session():
    """Crée une session Spark (import paresseux : le module s'importe sans Spark)."""
    from pyspark.sql import SparkSession  # pylint: disable=import-outside-toplevel
    return SparkSession.builder.appName("inference_meteo").getOrCreate()


def model_available() -> bool:
    """
    Un modele entraine est-il disponible sous /models ?

    Sur un cluster neuf, aucun modele n'existe encore : l'inference doit alors
    se retirer proprement (comme le bulletin GenAI a son fallback) plutot que
    de faire echouer tout le DAG Gold.
    """
    import hdfs_utils

    if not hdfs_utils.hdfs_exists("/models"):
        return False
    for name in hdfs_utils.hdfs_list("/models"):
        if re.match(rf"{MODEL_PREFIX}(\d+)$", name):
            return True
    return False


def _resolve_version(version: str) -> int:
    """Résout la version demandée ('latest' -> max, sinon entier)."""
    if version != "latest":
        return int(version)
    if not hdfs_utils.hdfs_exists("/models"):
        raise RuntimeError("Aucun répertoire /models sur HDFS : entraînez d'abord un modèle.")
    versions: List[int] = []
    for name in hdfs_utils.hdfs_list("/models"):
        match = re.match(rf"{MODEL_PREFIX}(\d+)$", name)
        if match:
            versions.append(int(match.group(1)))
    if not versions:
        raise RuntimeError("Aucun modèle temperature_predictor_v* trouvé sous /models.")
    return max(versions)


def _load_model(version: int) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """Télécharge et charge l'artefact (modèle + encoders + features) et ses métriques."""
    model_dir = f"/models/{MODEL_PREFIX}{version}"
    with tempfile.TemporaryDirectory() as tmpdir:
        local_model = os.path.join(tmpdir, "model.joblib")
        hdfs_utils.hdfs_download(f"{model_dir}/model.joblib", local_model)
        artifact = joblib.load(local_model)
    metrics = hdfs_utils.hdfs_read_json(f"{model_dir}/metrics.json")
    return artifact, metrics


def _apply_encoders(X: pd.DataFrame, encoders: Dict[str, Any]) -> pd.DataFrame:
    """Applique les encodeurs sauvegardés (catégories inconnues -> -1)."""
    X = X.copy()
    for col, le in encoders.items():
        if col not in X.columns:
            continue
        known = set(le.classes_)
        X[col] = X[col].astype(str).map(
            lambda v: int(le.transform([v])[0]) if v in known else -1
        ).astype(int)
    return X


def main(argv: Optional[List[str]] = None) -> int:
    """Point d'entrée CLI : prédit J+1 et écrit /gold/meteo/ml_predictions."""
    parser = argparse.ArgumentParser(
        description="Prédit la température à J+1 et écrit les prédictions Gold."
    )
    parser.add_argument("--days", type=int, default=30,
                        help="Nombre de jours de Silver à utiliser (défaut : 30).")
    parser.add_argument("--version", default="latest",
                        help="Version du modèle ('latest' ou entier, défaut : latest).")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    from pyspark.sql import functions as F  # pylint: disable=import-outside-toplevel

    if args.version == "latest" and not model_available():
        print("Aucun modele sous /models : inference sautee "
              "(lancez l'entrainement, puis relancez le DAG Gold).")
        print("Le DAG n'echoue PAS : les predictions sont un bonus, "
              "elles apparaitront des qu'un modele existera.")
        return 0

    version = _resolve_version(args.version)
    artifact, metrics = _load_model(version)

    model = artifact["model"]
    encoders: Dict[str, Any] = artifact.get("encoders", {})
    feature_names: List[str] = artifact["feature_names"]
    rmse_test = float(metrics.get("rmse_test", 2.0))
    logger.info("Modèle chargé : %s (v%d, rmse_test=%.3f)", model, version, rmse_test)

    spark = _build_spark_session()

    df = spark.read.parquet("/silver/meteo")
    max_dt = df.select(F.max("dt").alias("max_dt")).collect()[0]["max_dt"]
    cutoff = (datetime.strptime(max_dt, "%Y-%m-%d") - timedelta(days=args.days)).strftime("%Y-%m-%d")
    logger.info("Fenêtre Silver : dt >= %s (max(dt) = %s)", cutoff, max_dt)
    df = df.filter(F.col("dt") >= cutoff)

    pdf = df.toPandas()
    logger.info("Lignes Silver lues : %d", len(pdf))

    # Même pipeline de features que l'entraînement, mais en CONSERVANT les lignes
    # sans cible (prévision du futur).
    features, _ = build_features(pdf, drop_target_na=False)

    missing = [c for c in feature_names if c not in features.columns]
    if missing:
        raise RuntimeError(f"Colonnes features manquantes à l'inférence : {missing}")

    X = _apply_encoders(features[feature_names], encoders)
    predictions = model.predict(X)

    result = pd.DataFrame({
        "dt": features["dt"],
        "location": features["location"],
        "city": features["city"],
        "source": features["source"],
        "temp_actual": features["target"],  # NaN si J+1 n'est pas encore connu
        "temp_predicted": predictions,
    })

    # Erreur absolue (si la valeur réelle est connue) et indice de confiance.
    result["error_abs"] = np.where(
        result["temp_actual"].notna(),
        (result["temp_actual"] - result["temp_predicted"]).abs(),
        np.nan,
    )
    result["model_version"] = int(version)
    result["confidence"] = np.where(
        result["temp_actual"].notna(),
        np.clip(1.0 - result["error_abs"] / 10.0, 0.0, 1.0),
        np.clip(1.0 - rmse_test / 10.0, 0.0, 1.0),
    )

    # Partition dt = date de la prédiction = date de la ligne + 1 jour.
    result["dt"] = (
        pd.to_datetime(result["dt"]) + pd.Timedelta(days=1)
    ).dt.strftime("%Y-%m-%d")

    columns = ["dt", "location", "city", "source", "temp_actual",
               "temp_predicted", "error_abs", "model_version", "confidence"]
    result = result[columns]

    # Écriture Gold partitionnée avec overwrite dynamique des partitions.
    sdf = spark.createDataFrame(result)
    sdf.write \
        .mode("overwrite") \
        .partitionBy("dt") \
        .option("partitionOverwriteMode", "dynamic") \
        .parquet(GOLD_PREDICTIONS)

    # Marqueurs d'idempotence : par partition dt + racine.
    for d in sorted(result["dt"].unique()):
        hdfs_utils.write_success(f"{GOLD_PREDICTIONS}/dt={d}")
    hdfs_utils.write_success(GOLD_PREDICTIONS)

    # Tableau récapitulatif : 5 prédictions les plus récentes (J+1).
    preview = result.sort_values("dt", ascending=False).head(5)
    print("=== Aperçu des prédictions à J+1 (5 lignes) ===")
    print(preview.to_string(index=False))
    print(f"\nTotal prédictions écrites : {len(result)} (v{version})")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
