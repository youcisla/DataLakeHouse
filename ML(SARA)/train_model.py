"""Entraine un modele XGBoost pour predire la temperature du lendemain.

Exemple :
    python train_model.py
    python train_model.py --data gold_weather.parquet --model weather_xgboost_model.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from xgboost import XGBRegressor


REQUIRED_COLUMNS = {
    "date",
    "temperature",
    "humidite",
    "vent",
    "precipitation",
}
FEATURE_COLUMNS = [
    "temperature",
    "humidite",
    "vent",
    "precipitation",
    "temp_hier",
    "humidite_hier",
    "moy_mobile_temp_3j",
]
TARGET_COLUMN = "target_temp_demain"


def read_weather_data(data_path: str | Path) -> pd.DataFrame:
    """Lit un CSV ou un dataset Parquet local, partage ou distant."""
    path = str(data_path)
    clean_path = path.split("?", maxsplit=1)[0].rstrip("/").lower()

    if clean_path.endswith(".csv"):
        return pd.read_csv(path)
    if clean_path.endswith(".parquet") or "." not in clean_path.rsplit("/", maxsplit=1)[-1]:
        return pd.read_parquet(path)
    raise ValueError(
        f"Format non supporte pour {path!r}. Utilisez un fichier .csv ou .parquet."
    )


def load_and_prepare_data(data_path: str | Path) -> pd.DataFrame:
    """Charge les donnees, construit les variables temporelles et nettoie les NA."""
    data = read_weather_data(data_path)

    missing_columns = REQUIRED_COLUMNS - set(data.columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"Colonnes manquantes dans {data_path}: {missing}")

    # Le tri doit preceder tous les calculs temporels.
    data["date"] = pd.to_datetime(data["date"], errors="raise")
    data = data.sort_values("date").reset_index(drop=True)
    if data["date"].duplicated().any():
        raise ValueError("Les donnees contiennent plusieurs mesures pour la meme date.")

    numeric_columns = ["temperature", "humidite", "vent", "precipitation"]
    data[numeric_columns] = data[numeric_columns].apply(pd.to_numeric, errors="coerce")

    # Les variables de retard utilisent uniquement les observations precedentes.
    data["temp_hier"] = data["temperature"].shift(1)
    data["humidite_hier"] = data["humidite"].shift(1)
    data["moy_mobile_temp_3j"] = (
        data["temperature"].rolling(window=3, min_periods=3).mean()
    )

    # shift(-1) associe chaque ligne a la temperature observee le lendemain.
    data[TARGET_COLUMN] = data["temperature"].shift(-1)
    data = data.dropna(subset=FEATURE_COLUMNS + [TARGET_COLUMN]).copy()

    if len(data) < 2:
        raise ValueError(
            "Pas assez de lignes apres le feature engineering pour separer train et test."
        )

    return data


def train_and_evaluate(
    data: pd.DataFrame,
    model_path: Path,
    metrics_path: Path = Path("weather_model_metrics.json"),
    train_ratio: float = 0.8,
) -> float:
    """Entraine, evalue et sauvegarde le modele et ses metadonnees."""
    split_index = int(len(data) * train_ratio)
    if split_index < 1 or split_index >= len(data):
        raise ValueError("La proportion train doit laisser au moins une ligne dans train et test.")

    X = data[FEATURE_COLUMNS]
    y = data[TARGET_COLUMN]

    # Aucun shuffle: les observations futures ne doivent pas influencer le passe.
    X_train, X_test = X.iloc[:split_index], X.iloc[split_index:]
    y_train, y_test = y.iloc[:split_index], y.iloc[split_index:]

    model = XGBRegressor(
        objective="reg:squarederror",
        n_estimators=200,
        max_depth=3,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        n_jobs=1,
    )
    model.fit(X_train, y_train)

    predictions = model.predict(X_test)
    rmse = mean_squared_error(y_test, predictions) ** 0.5
    mae = mean_absolute_error(y_test, predictions)
    r2 = r2_score(y_test, predictions)

    # Baseline utile: prediction naive egale a la temperature de la veille.
    baseline_predictions = X_test["temp_hier"]
    baseline_rmse = mean_squared_error(y_test, baseline_predictions) ** 0.5

    feature_importance = dict(
        sorted(
            zip(FEATURE_COLUMNS, model.feature_importances_),
            key=lambda item: item[1],
            reverse=True,
        )
    )
    metrics = {
        "model": "XGBRegressor",
        "target": TARGET_COLUMN,
        "features": FEATURE_COLUMNS,
        "train_rows": len(X_train),
        "test_rows": len(X_test),
        "rmse": round(float(rmse), 6),
        "mae": round(float(mae), 6),
        "r2": round(float(r2), 6),
        "baseline_yesterday_rmse": round(float(baseline_rmse), 6),
        "beats_baseline": bool(rmse < baseline_rmse),
        "feature_importance": {
            name: round(float(value), 6)
            for name, value in feature_importance.items()
        },
    }

    model_path.parent.mkdir(parents=True, exist_ok=True)
    model.save_model(model_path)
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    print(f"Lignes utilisees : {len(data)} (train: {len(X_train)}, test: {len(X_test)})")
    print(f"RMSE sur le jeu de test : {rmse:.4f} degres C")
    print(f"Baseline (temperature d'hier) : {baseline_rmse:.4f} degres C")
    print(f"MAE : {mae:.4f} | R2 : {r2:.4f}")
    print(f"Top feature : {next(iter(feature_importance))}")
    print(f"Modele sauvegarde dans : {model_path}")
    print(f"Metriques sauvegardees dans : {metrics_path}")
    return rmse


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data",
        type=str,
        default="meteo_mock.csv",
        help="Fichier CSV, fichier Parquet ou dossier Parquet partitionne.",
    )
    parser.add_argument(
        "--model",
        type=Path,
        default=Path("weather_xgboost_model.json"),
        help="Chemin du modele XGBoost a sauvegarder.",
    )
    parser.add_argument(
        "--metrics",
        type=Path,
        default=Path("weather_model_metrics.json"),
        help="Chemin du fichier JSON de metriques.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    prepared_data = load_and_prepare_data(args.data)
    train_and_evaluate(prepared_data, args.model, args.metrics)


if __name__ == "__main__":
    main()