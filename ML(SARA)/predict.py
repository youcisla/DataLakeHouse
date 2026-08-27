"""Realise une prediction de temperature et genere un bulletin meteo.

Le fichier d'entree doit contenir au moins les trois dernieres observations
avec les colonnes : date, temperature, humidite, vent, precipitation.

Exemple :
    python predict.py --data meteo_mock.csv
    python predict.py --data gold_weather.csv --model weather_xgboost_model.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
from xgboost import DMatrix, XGBRegressor


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


def prepare_latest_observation(data_path: str | Path) -> tuple[pd.DataFrame, pd.Series]:
    """Prepare les features de la derniere observation disponible."""
    data = read_weather_data(data_path)

    missing_columns = REQUIRED_COLUMNS - set(data.columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"Colonnes manquantes dans {data_path}: {missing}")

    data["date"] = pd.to_datetime(data["date"], errors="raise")
    data = data.sort_values("date").reset_index(drop=True)
    if data["date"].duplicated().any():
        raise ValueError("Les donnees contiennent plusieurs mesures pour la meme date.")

    numeric_columns = ["temperature", "humidite", "vent", "precipitation"]
    data[numeric_columns] = data[numeric_columns].apply(pd.to_numeric, errors="coerce")

    # Ces calculs doivent rester identiques a ceux de train_model.py.
    data["temp_hier"] = data["temperature"].shift(1)
    data["humidite_hier"] = data["humidite"].shift(1)
    data["moy_mobile_temp_3j"] = data["temperature"].rolling(
        window=3, min_periods=3
    ).mean()

    latest = data.dropna(subset=FEATURE_COLUMNS).tail(1)
    if latest.empty:
        raise ValueError(
            "Au moins trois observations valides sont necessaires pour calculer les features."
        )

    return latest[FEATURE_COLUMNS], latest.iloc[0]


def generate_weather_bulletin(predicted_temperature: float, weather: pd.Series) -> str:
    """Genere un bulletin simple a partir de la prediction et des conditions actuelles."""
    if predicted_temperature >= 30:
        message = f"Alerte : forte chaleur prevue demain avec {predicted_temperature:.1f} degres C."
    elif predicted_temperature <= 5:
        message = f"Alerte : temps froid prevu demain avec {predicted_temperature:.1f} degres C."
    elif abs(predicted_temperature - weather["temperature"]) <= 2:
        message = f"Temps stable prevu demain, autour de {predicted_temperature:.1f} degres C."
    elif predicted_temperature > weather["temperature"]:
        message = f"Hausse des temperatures prevue demain, jusqu'a {predicted_temperature:.1f} degres C."
    else:
        message = f"Baisse des temperatures prevue demain, autour de {predicted_temperature:.1f} degres C."

    conditions = (
        f" Humidite actuelle : {weather['humidite']:.0f} %, "
        f"vent : {weather['vent']:.1f} km/h, "
        f"precipitations : {weather['precipitation']:.1f} mm."
    )
    return message + conditions


def explain_prediction(model: XGBRegressor, features: pd.DataFrame) -> str:
    """Identifie la feature qui influence le plus cette prediction locale."""
    contributions = model.get_booster().predict(
        DMatrix(features), pred_contribs=True
    )[0][:-1]  # La derniere valeur est le biais du modele.
    ranked = sorted(
        zip(FEATURE_COLUMNS, contributions),
        key=lambda item: abs(item[1]),
        reverse=True,
    )
    feature_name, contribution = ranked[0]
    direction = "augmente" if contribution >= 0 else "diminue"
    return f"Facteur principal : {feature_name} {direction} la prediction ({contribution:+.2f} degres C)."


def predict(
    data_path: str | Path,
    model_path: Path,
    metrics_path: Path = Path("weather_model_metrics.json"),
    output_path: Path | None = None,
) -> tuple[float, str]:
    """Predit, mesure l'incertitude, genere un bulletin et peut exporter un JSON."""
    features, latest_weather = prepare_latest_observation(data_path)

    model = XGBRegressor()
    model.load_model(model_path)
    predicted_temperature = float(model.predict(features)[0])
    bulletin = generate_weather_bulletin(predicted_temperature, latest_weather)
    explanation = explain_prediction(model, features)

    prediction_date = latest_weather["date"] + pd.Timedelta(days=1)
    metrics = {}
    if metrics_path.exists():
        metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    uncertainty = float(metrics.get("rmse", 0))
    result = {
        "prediction_date": prediction_date.strftime("%Y-%m-%d"),
        "predicted_temperature": round(predicted_temperature, 2),
        "estimated_interval": {
            "lower": round(predicted_temperature - uncertainty, 2),
            "upper": round(predicted_temperature + uncertainty, 2),
            "confidence_note": "interval indicatif base sur le RMSE du jeu de test",
        },
        "latest_observation": {
            "date": latest_weather["date"].strftime("%Y-%m-%d"),
            "temperature": float(latest_weather["temperature"]),
            "humidite": float(latest_weather["humidite"]),
            "vent": float(latest_weather["vent"]),
            "precipitation": float(latest_weather["precipitation"]),
        },
        "model_rmse": metrics.get("rmse"),
        "explanation": explanation,
        "bulletin": bulletin,
    }
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(result, indent=2), encoding="utf-8")

    print(f"Prediction pour le {prediction_date:%Y-%m-%d} : {predicted_temperature:.2f} degres C")
    if uncertainty:
        print(
            f"Intervalle indicatif : [{predicted_temperature - uncertainty:.2f}, "
            f"{predicted_temperature + uncertainty:.2f}] degres C"
        )
    print(f"Explication : {explanation}")
    print(f"Bulletin : {bulletin}")
    if output_path is not None:
        print(f"Resultat JSON sauvegarde dans : {output_path}")
    return predicted_temperature, bulletin


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=str, default="meteo_mock.csv")
    parser.add_argument(
        "--model", type=Path, default=Path("weather_xgboost_model.json")
    )
    parser.add_argument(
        "--metrics", type=Path, default=Path("weather_model_metrics.json")
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("weather_prediction.json"),
        help="Fichier JSON de sortie pour la Gold ou un dashboard.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    predict(args.data, args.model, args.metrics, args.output)


if __name__ == "__main__":
    main()