# -*- coding: utf-8 -*-
"""
kafka_producer.py : producteur Kafka (source temps réel Open-Meteo)
====================================================================
Interroge l'API Open-Meteo (https://open-meteo.com) toutes les 5 minutes
pour les 5 villes surveillées (Paris, Lyon, Marseille, Bordeaux, Lille)
et publie chaque relevé au format JSON sur le topic Kafka "meteo-stream".

Le producteur s'arrête automatiquement lorsque le quota Bronze est atteint
(voir BRONZE_QUOTA_GB) : contrôle via WebHDFS à chaque lot de 5 envois.

Les dépendances lourdes (requests, kafka, hdfs_utils) sont importées À
L'INTÉRIEUR des fonctions : le module reste importable et ses fonctions pures
testables sans broker Kafka ni cluster HDFS (cf. tests/test_medallion.py).

Auteur : Youcef, équipe DataLake Météo
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# Chemin des utilitaires HDFS (hdfs_utils.py), importable seulement quand utile.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger("kafka_producer")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
API_URL = "https://api.open-meteo.com/v1/forecast"
DEFAULT_CITIES = [
    {"city": "Paris", "latitude": 48.8534, "longitude": 2.3488},
    {"city": "Lyon", "latitude": 45.764, "longitude": 4.8357},
    {"city": "Marseille", "latitude": 43.2965, "longitude": 5.3698},
    {"city": "Bordeaux", "latitude": 44.8378, "longitude": -0.5792},
    {"city": "Lille", "latitude": 50.6292, "longitude": 3.0573},
]

_QUOTA_CHECK_EVERY = 5          # contrôle du quota tous les N envois
_HTTP_TIMEOUT = 30
_RETRIES = 3

#: Tentatives de connexion au broker au démarrage.
_CONNECT_RETRIES = 12
_CONNECT_DELAY = 5


def load_cities() -> List[Dict[str, Any]]:
    """Charge la liste des villes depuis OPENMETEO_CITIES (JSON) ou défaut."""
    raw = os.environ.get("OPENMETEO_CITIES")
    if raw:
        try:
            cities = json.loads(raw)
            if isinstance(cities, list) and cities:
                return cities
        except json.JSONDecodeError:
            logger.warning("OPENMETEO_CITIES illisible, villes par défaut utilisées.")
    return DEFAULT_CITIES


def build_record(city: Dict[str, Any], current: Dict[str, Any]) -> Dict[str, Any]:
    """Construit le relevé JSON au format du contrat Bronze/Open-Meteo."""
    return {
        "city": city["city"],
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "timestamp": current.get("time"),
        "temperature": current.get("temperature_2m"),
        "windspeed": current.get("wind_speed_10m"),
        "winddirection": current.get("wind_direction_10m"),
        "weathercode": current.get("weather_code"),
        "precipitation": current.get("precipitation"),
        "source": "OPENMETEO",
    }


#: Bornes physiques plausibles (records mondiaux, avec marge).
#: Une valeur hors bornes est une anomalie de la source, pas une mesure :
#: elle est mise à NULL plutôt que de contaminer les agrégats.
MEASUREMENT_BOUNDS: Dict[str, tuple] = {
    "temperature_2m": (-95.0, 65.0),      # °C
    "wind_speed_10m": (0.0, 130.0),       # m/s
    "wind_direction_10m": (0.0, 360.0),   # degrés
    "precipitation": (0.0, 2000.0),       # mm
    "weather_code": (0.0, 100.0),         # code WMO
}


def validate_measurements(current: Dict[str, Any]) -> Dict[str, Any]:
    """
    Met à NULL toute mesure non numérique ou physiquement impossible.

    Retourne un NOUVEAU dict, sans modifier l'entrée. Les clés inconnues sont
    laissées telles quelles (le contrat Open-Meteo peut évoluer).
    """
    checked = dict(current)
    for key, (low, high) in MEASUREMENT_BOUNDS.items():
        if key not in checked:
            continue
        value = checked[key]
        if value is None:
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            logger.warning("Mesure %s non numérique (%r) : ignorée.", key, value)
            checked[key] = None
            continue
        if number != number or not (low <= number <= high):  # NaN ou hors bornes
            logger.warning("Mesure %s hors bornes physiques (%r) : ignorée.", key, value)
            checked[key] = None
    return checked


def has_usable_measurement(record: Dict[str, Any]) -> bool:
    """
    Le relevé contient-il au moins une mesure exploitable ?

    Un relevé entièrement vide n'apporte rien en Bronze et fait grossir le
    datalake pour rien : il est écarté (et journalisé) plutôt que publié.
    """
    return any(record.get(key) is not None
               for key in ("temperature", "windspeed", "precipitation"))


def fetch_current(city: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Interroge l'API Open-Meteo (conditions actuelles) avec retries."""
    import requests

    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "current": "temperature_2m,wind_speed_10m,wind_direction_10m,precipitation,weather_code",
        "timezone": "UTC",
    }
    import requests

    for attempt in range(1, _RETRIES + 1):
        try:
            resp = requests.get(API_URL, params=params, timeout=_HTTP_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            current = data.get("current") or {}
            # Une mesure absente reste absente (None), jamais 0.0. Un 0.0 serait
            # une température parfaitement plausible en hiver : rien n'échouerait,
            # mais les moyennes Gold seraient tirées vers zéro et le modèle ML
            # apprendrait sur des valeurs inventées.
            return validate_measurements(current)
        except requests.RequestException as exc:
            logger.warning("Echec API Open-Meteo pour %s (tentative %d/%d) : %s",
                           city["city"], attempt, _RETRIES, exc)
            time.sleep(2 * attempt)
    return None


def check_quota(quota_gb: float) -> bool:
    """True si le quota Bronze est atteint (le producteur doit s'arrêter)."""
    import hdfs_utils

    if quota_gb <= 0:
        return False
    import hdfs_utils

    try:
        return hdfs_utils.quota_reached("/bronze", quota_gb)
    except IOError as exc:
        logger.warning("Quota non vérifiable (%s) : poursuite.", exc)
        return False


def connect_producer(bootstrap: str, topic: str):
    """
    Ouvre le producteur Kafka, en patientant si le broker n'est pas encore prêt.

    Le constructeur de KafkaProducer est paresseux : il ne se connecte pas.
    On force donc une vraie requête de métadonnées via partitions_for, qui
    lève KafkaTimeoutError tant que le broker ne répond pas.
    """
    import json as _json

    from kafka import KafkaProducer
    from kafka.errors import KafkaError

    for attempt in range(1, _CONNECT_RETRIES + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap,
                value_serializer=lambda v: _json.dumps(v).encode("utf-8"),
                acks="all",
                retries=5,
                linger_ms=200,
            )
            producer.partitions_for(topic)  # bootstrap réel, bloquant
            return producer
        except KafkaError as exc:
            logger.warning("Broker %s pas encore prêt (%d/%d) : %s",
                           bootstrap, attempt, _CONNECT_RETRIES, exc)
            time.sleep(_CONNECT_DELAY)
    return None


class GracefulStop(Exception):
    """Interruption demandée (SIGTERM / SIGINT)."""


def main() -> int:
    parser = argparse.ArgumentParser(description="Producteur Kafka : Open-Meteo vers topic météo")
    parser.add_argument("--once", action="store_true",
                        help="Un seul envoi (toutes les villes) puis arrêt")
    parser.add_argument("--max-runs", type=int, default=None,
                        help="Nombre maximal de cycles d'envoi (None = illimité)")
    parser.add_argument("--interval", type=int, default=None,
                        help="Intervalle entre deux envois en secondes (défaut : env POLL_INTERVAL_SECONDS)")
    parser.add_argument("--no-quota", action="store_true",
                        help="Désactive le contrôle du quota Bronze")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s [kafka_producer] %(message)s")

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.environ.get("METEO_TOPIC", "meteo-stream")
    interval = args.interval if args.interval is not None else int(
        os.environ.get("POLL_INTERVAL_SECONDS", "300"))
    quota_gb = float(os.environ.get("BRONZE_QUOTA_GB", "10.5"))
    cities = load_cities()

    def _stop_handler(_signum, _frame):  # pragma: no cover
        raise GracefulStop()

    signal.signal(signal.SIGTERM, _stop_handler)
    signal.signal(signal.SIGINT, _stop_handler)

    producer = connect_producer(bootstrap, topic)
    if producer is None:
        logger.error("Broker Kafka injoignable (%s) après %d tentatives.",
                     bootstrap, _CONNECT_RETRIES)
        return 1
    logger.info("Producteur démarré : topic=%s, intervalle=%ss, villes=%s, quota=%s Go",
                topic, interval, [c["city"] for c in cities], quota_gb)

    run_count = 0
    try:
        while True:
            run_count += 1
            sent = 0
            for city in cities:
                current = fetch_current(city)
                if current is None:
                    continue
                record = build_record(city, current)
                if not has_usable_measurement(record):
                    logger.warning("%s : aucune mesure exploitable, relevé ignoré.",
                                   city["city"])
                    continue
                future = producer.send(topic, value=record)
                try:
                    future.get(timeout=15)
                    sent += 1
                except Exception as exc:  # noqa: BLE001
                    logger.error("Échec d'envoi pour %s : %s", city["city"], exc)
            producer.flush()
            logger.info("Cycle %d : %d/%d relevés publiés (%s)",
                        run_count, sent, len(cities), datetime.now(timezone.utc).isoformat())

            if args.once:
                break
            if args.max_runs and run_count >= args.max_runs:
                logger.info("max-runs atteint (%d), arrêt.", args.max_runs)
                break
            # Contrôle du quota Bronze (le flux s'arrête si le quota est atteint)
            if not args.no_quota and run_count % _QUOTA_CHECK_EVERY == 0:
                if check_quota(quota_gb):
                    logger.warning("QUOTA BRONZE ATTEINT (>= %s Go) : le producteur s'arrête.", quota_gb)
                    break
            time.sleep(interval)
    except GracefulStop:
        logger.info("Arrêt demandé (signal) : arrêt propre du producteur.")
    except KeyboardInterrupt:
        logger.info("Arrêt par Ctrl-C.")
    finally:
        producer.flush()
        producer.close()
        logger.info("Producteur arrêté. Total cycles : %d", run_count)
    return 0


if __name__ == "__main__":
    sys.exit(main())
