# -*- coding: utf-8 -*-
"""
train_model.py : entraînement du modèle de prévision de température (DataLake Météo).
=====================================================================================
Entraîne un XGBRegressor (avec recherche d'hyperparamètres GridSearchCV) sur le jeu
de features produit par `ml/feature_engineering.py`, puis publie le modèle et ses
métriques sur HDFS sous `/models/temperature_predictor_v{N}`.

Le split est TEMPOREL (jamais aléatoire) : tri par (location, timestamp), puis
train < validation < test sur l'axe du temps.

Auteur : Sara, équipe DataLake Météo
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import tempfile
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import joblib
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import GridSearchCV
from sklearn.preprocessing import LabelEncoder

import hdfs_utils

logger = logging.getLogger(__name__)

# Colonnes à EXCLURE des features (métadonnées, identifiants, cible).
EXCLUDED_COLUMNS: List[str] = [
    "target", "timestamp", "dt", "location", "city",
    "station_id", "station_name", "source",
]

MODEL_PREFIX = "temperature_predictor_v"
RMSE_OBJECTIVE = 2.0  # objectif de RMSE test, en °C


def _load_features(remote_path: str) -> pd.DataFrame:
    """Télécharge le parquet de features HDFS puis le lit dans un DataFrame."""
    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = os.path.join(tmpdir, "features.parquet")
        hdfs_utils.hdfs_download(remote_path, local_path)
        df = pd.read_parquet(local_path)
    return df


def _temporal_split(
    df: pd.DataFrame,
    test_size: float,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Split temporel : train < validation < test sur l'axe du temps.

    Les dates sont triées, puis découpées en 3 plages contiguës :
    (1 - 2*test_size) pour l'entraînement, test_size pour la validation et
    test_size pour le test.
    """
    df = df.sort_values(["location", "timestamp"]).reset_index(drop=True)
    # Normaliser dt en chaîne : train_end/val_end sont des chaînes, et comparer
    # une datetime à une chaîne lève une TypeError.
    df = df.copy()
    df["dt"] = df["dt"].astype(str)
    dates = np.sort(df["dt"].unique())
    if len(dates) < 3:
        raise ValueError("Pas assez de dates distinctes pour un split temporel.")

    train_end = dates[int(len(dates) * (1 - 2 * test_size)) - 1]
    val_end = dates[int(len(dates) * (1 - test_size)) - 1]

    train = df[df["dt"] <= train_end]
    val = df[(df["dt"] > train_end) & (df["dt"] <= val_end)]
    test = df[df["dt"] > val_end]

    if train.empty or val.empty or test.empty:
        raise ValueError(
            f"Split temporel invalide (train={len(train)}, val={len(val)}, "
            f"test={len(test)}) : augmentez l'historique."
        )
    return train, val, test


def _split_xy(subset: pd.DataFrame) -> Tuple[pd.DataFrame, np.ndarray, List[str]]:
    """Sépare X (features) et y (cible) et retourne la liste des colonnes features."""
    feature_cols = [c for c in subset.columns if c not in EXCLUDED_COLUMNS]
    return subset[feature_cols].copy(), subset["target"].to_numpy(), feature_cols


def _fit_encoders(
    X: pd.DataFrame,
) -> Tuple[pd.DataFrame, Dict[str, LabelEncoder]]:
    """Encode les colonnes catégorielles restantes (season, country, ...)."""
    encoders: Dict[str, LabelEncoder] = {}
    for col in X.select_dtypes(include=["object", "category"]).columns:
        le = LabelEncoder()
        X[col] = le.fit_transform(X[col].astype(str))
        encoders[col] = le
    return X, encoders


def _apply_encoders(
    X: pd.DataFrame,
    encoders: Dict[str, LabelEncoder],
) -> pd.DataFrame:
    """Applique des encodeurs déjà ajustés (catégories inconnues -> -1)."""
    X = X.copy()
    for col, le in encoders.items():
        if col not in X.columns:
            continue
        known = set(le.classes_)
        X[col] = X[col].astype(str).map(
            lambda v: int(le.transform([v])[0]) if v in known else -1
        ).astype(int)
    return X


def _evaluate(model: Any, X: pd.DataFrame, y: np.ndarray) -> Tuple[float, float, float]:
    """Calcule (rmse, mae, r2) d'un modèle sur un jeu de données."""
    pred = model.predict(X)
    rmse = float(np.sqrt(mean_squared_error(y, pred)))
    mae = float(mean_absolute_error(y, pred))
    r2 = float(r2_score(y, pred))
    return rmse, mae, r2


def _next_version() -> int:
    """Calcule la prochaine version : max(versions existantes sous /models) + 1."""
    if not hdfs_utils.hdfs_exists("/models"):
        return 1
    versions: List[int] = []
    for name in hdfs_utils.hdfs_list("/models"):
        match = re.match(rf"{MODEL_PREFIX}(\d+)$", name)
        if match:
            versions.append(int(match.group(1)))
    return (max(versions) + 1) if versions else 1


def main(argv: Optional[List[str]] = None) -> int:
    """Point d'entrée CLI : entraîne et publie un modèle versionné."""
    parser = argparse.ArgumentParser(
        description="Entraîne un XGBRegressor de prévision de température à J+1."
    )
    parser.add_argument("--features", default="/features/meteo/features.parquet",
                        help="Chemin HDFS du parquet de features (défaut : /features/meteo/features.parquet).")
    parser.add_argument("--version", type=int, default=None,
                        help="Version forcée ; sinon max(versions existantes) + 1.")
    parser.add_argument("--test-size", type=float, default=0.15,
                        help="Fraction des dates pour validation et test (défaut : 0.15).")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    df = _load_features(args.features)
    logger.info("Features chargées : %d lignes, %d colonnes", len(df), len(df.columns))

    train, val, test = _temporal_split(df, args.test_size)
    logger.info("Split temporel : train=%d, val=%d, test=%d",
                len(train), len(val), len(test))

    X_train, y_train, feature_names = _split_xy(train)
    X_val, y_val, _ = _split_xy(val)
    X_test, y_test, _ = _split_xy(test)

    # Encodage ajusté sur train uniquement, appliqué à val et test.
    X_train, encoders = _fit_encoders(X_train)
    X_val = _apply_encoders(X_val, encoders)
    X_test = _apply_encoders(X_test, encoders)

    # Modèle de base + recherche sur une petite grille d'hyperparamètres.
    base_model = xgb.XGBRegressor(
        random_state=42,
        n_estimators=200,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.9,
        colsample_bytree=0.9,
    )
    param_grid = {
        "n_estimators": [100, 200],
        "max_depth": [4, 6],
        "learning_rate": [0.05, 0.1],
    }
    grid = GridSearchCV(
        base_model,
        param_grid,
        cv=3,
        scoring="neg_root_mean_squared_error",
        n_jobs=-1,
        verbose=0,
    )

    # Entraînement sur train + validation ; évaluation finale sur test.
    logger.info("Recherche d'hyperparamètres (GridSearchCV) ...")
    X_train_val = pd.concat([X_train, X_val], ignore_index=True)
    y_train_val = np.concatenate([y_train, y_val])
    grid.fit(X_train_val, y_train_val)
    best_model = grid.best_estimator_
    logger.info("Meilleurs hyperparamètres : %s", grid.best_params_)

    rmse_train, _, _ = _evaluate(best_model, X_train, y_train)
    rmse_val, _, _ = _evaluate(best_model, X_val, y_val)
    rmse_test, mae_test, r2_test = _evaluate(best_model, X_test, y_test)

    version = args.version if args.version is not None else _next_version()
    model_dir = f"/models/{MODEL_PREFIX}{version}"

    # Artefact : meilleur modèle + encoders + liste des features.
    with tempfile.TemporaryDirectory() as tmpdir:
        artifact = {
            "model": best_model,
            "encoders": encoders,
            "feature_names": feature_names,
        }
        local_model = os.path.join(tmpdir, "model.joblib")
        joblib.dump(artifact, local_model)
        hdfs_utils.hdfs_upload(local_model, f"{model_dir}/model.joblib")

    metrics = {
        "rmse_test": round(rmse_test, 4),
        "mae_test": round(mae_test, 4),
        "r2_test": round(r2_test, 4),
        "rmse_train": round(rmse_train, 4),
        "rmse_val": round(rmse_val, 4),
        "best_params": grid.best_params_,
        "n_samples": int(len(df)),
        "model_version": int(version),
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "feature_names": feature_names,
    }
    hdfs_utils.hdfs_write_json(f"{model_dir}/metrics.json", metrics)
    hdfs_utils.write_success(model_dir)

    print(f"Modèle publié : {model_dir}")
    print(f"RMSE train = {rmse_train:.4f} °C | val = {rmse_val:.4f} °C | test = {rmse_test:.4f} °C")
    print(f"MAE test   = {mae_test:.4f} °C | R² test = {r2_test:.4f}")

    if rmse_test < RMSE_OBJECTIVE:
        print(f"🎉 Fierté ! RMSE test = {rmse_test:.4f} °C < {RMSE_OBJECTIVE} °C (objectif atteint).")
    else:
        print(f"⚠️  Attention : RMSE test = {rmse_test:.4f} °C >= {RMSE_OBJECTIVE} °C (objectif non atteint).")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
