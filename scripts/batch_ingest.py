# -*- coding: utf-8 -*-
"""
batch_ingest.py — Ingestion batch NOAA (GHCN-D) vers Bronze
=============================================================
Télécharge un sous-ensemble de fichiers CSV de stations NOAA
(Global Historical Climatology Network Daily) pour atteindre la cible
de volume (NOAA_TARGET_GB, défaut 6.6 Go) et les dépose en BRUT dans
HDFS :  /bronze/meteo/batch/source=noaa/year=YYYY/month=MM/

Convention de partitionnement : année/mois du lot d'ingestion (chaque
fichier est stocké une seule fois, aucun octet dupliqué). Un marqueur
_SUCCESS + un manifest.json sont déposés après ingestion complète d'un lot.

IDEMPOTENCE : la liste des stations déjà ingérées est conservée dans
/bronze/meteo/batch/source=noaa/_ingested.json ; relancer le script ne
retélécharge et ne réingère jamais deux fois la même station.

Mode --synthetic : génère des CSV au même schéma NOAA (utile si le réseau
scolaire bloque l'accès à NOAA ou pour une démo sans téléchargement).

Auteur : Youcef — Équipe DataLake Météo
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import json
import logging
import math
import os
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

import requests

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__))))
import hdfs_utils  # noqa: E402

logger = logging.getLogger("batch_ingest")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DEFAULT_BASE_URL = "https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/"
STATION_RE = re.compile(r'href="([A-Z0-9]{2,11}.csv)"')
HTTP_TIMEOUT = 60
DOWNLOAD_WORKERS = 4
MAX_PAGES = 40

NOAA_HEADER = ["STATION", "DATE", "LATITUDE", "LONGITUDE", "ELEVATION", "NAME",
               "PRCP", "TMAX", "TMIN", "TAVG", "SNOW", "SNWD", "AWND"]


# ---------------------------------------------------------------------------
# 1) Énumération des fichiers de stations disponibles sur le portail NOAA
# ---------------------------------------------------------------------------
def list_station_files(base_url: str, max_pages: int = MAX_PAGES) -> List[str]:
    """Récupère la liste des fichiers CSV de stations (URLs absolues)."""
    links: List[str] = []
    page = 1
    while page <= max_pages:
        url = base_url if page == 1 else f"{base_url}?page={page}"
        try:
            resp = requests.get(url, timeout=HTTP_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as exc:
            logger.warning("Page %d inaccessible (%s) — arrêt de l'énumération.", page, exc)
            break
        found = [f"{base_url}{m}" for m in STATION_RE.findall(resp.text)]
        if not found:
            break
        new = [u for u in found if u not in links]
        links.extend(new)
        logger.info("Page %d : %d fichiers, total %d", page, len(new), len(links))
        if len(new) == 0:
            break
        page += 1
    return links


# ---------------------------------------------------------------------------
# 2) Sélection déterministe des stations pour atteindre la cible
# ---------------------------------------------------------------------------
def station_sizes(urls: List[str], limit: int = 400) -> List[Tuple[str, int]]:
    """Interroge Content-Length (HEAD) pour trier par taille décroissante."""
    sizes: List[Tuple[str, int]] = []

    def _head(url: str) -> Optional[Tuple[str, int]]:
        try:
            resp = requests.head(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
            length = int(resp.headers.get("Content-Length") or 0)
            return (url, length)
        except requests.RequestException:
            return None

    with ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as pool:
        futures = {pool.submit(_head, u): u for u in urls[:limit]}
        for fut in as_completed(futures):
            result = fut.result()
            if result and result[1] > 0:
                sizes.append(result)
    sizes.sort(key=lambda x: (-x[1], x[0]))
    return sizes


def select_stations(urls: List[str], target_gb: float, limit_stations: int = 0) -> List[Tuple[str, int]]:
    """
    Sélectionne les stations (par taille décroissante) jusqu'à atteindre
    ~target_gb (marge de 5 %), dans la limite éventuelle de limit_stations.
    Retourne [(url, bytes), ...].
    """
    target_bytes = target_gb * (1024 ** 3)
    candidates = station_sizes(urls)
    selected: List[Tuple[str, int]] = []
    total = 0
    for url, size in candidates:
        if limit_stations and len(selected) >= limit_stations:
            break
        selected.append((url, size))
        total += size
        if total >= target_bytes * 0.95:
            break
    logger.info("Sélection : %d stations, %.2f Go (cible %.2f Go)",
                len(selected), total / (1024 ** 3), target_gb)
    return selected


# ---------------------------------------------------------------------------
# 3) Téléchargement (reprise possible : fichier partiel réutilisé)
# ---------------------------------------------------------------------------
def download_station(url: str, cache_dir: str, force: bool = False) -> Optional[str]:
    """Télécharge un CSV de station dans le cache local (reprise + retries)."""
    os.makedirs(cache_dir, exist_ok=True)
    name = url.rsplit("/", 1)[-1]
    local = os.path.join(cache_dir, name)
    if os.path.exists(local) and not force:
        return local
    tmp = local + ".part"
    for attempt in range(1, 4):
        try:
            with requests.get(url, stream=True, timeout=HTTP_TIMEOUT) as resp:
                resp.raise_for_status()
                with open(tmp, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=1024 * 1024):
                        fh.write(chunk)
            os.replace(tmp, local)
            logger.info("Téléchargé : %s (%.1f Mo)", name, os.path.getsize(local) / 1e6)
            return local
        except requests.RequestException as exc:
            logger.warning("Échec téléchargement %s (tentative %d/3) : %s", name, attempt, exc)
            time.sleep(3 * attempt)
    return None


def read_first_date(local: str) -> Optional[str]:
    """Lit la première ligne de données du CSV (date du plus ancien relevé)."""
    try:
        with open(local, "r", encoding="utf-8", errors="replace") as fh:
            next(fh)  # header
            line = next(fh, "")
            parts = line.strip().split(",")
            return parts[1] if len(parts) > 1 and parts[1].isdigit() else None
    except (StopIteration, OSError, IndexError):
        return None


# ---------------------------------------------------------------------------
# 4) Dépôt en Bronze (idempotent) + manifest
# ---------------------------------------------------------------------------
def ingested_set() -> set:
    """Stations déjà ingérées (depuis _ingested.json)."""
    path = "/bronze/meteo/batch/source=noaa/_ingested.json"
    try:
        data = hdfs_utils.hdfs_read_json(path)
        return set(data.get("stations", []))
    except IOError:
        return set()


def save_ingested(stations: set) -> None:
    path = "/bronze/meteo/batch/source=noaa/_ingested.json"
    hdfs_utils.hdfs_write_json(path, {"stations": sorted(stations),
                                      "updated_at": dt.datetime.now(dt.timezone.utc).isoformat()})


def upload_to_bronze(local: str, station: str, ingestion_date: dt.date) -> str:
    """
    Dépose le fichier CSV BRUT dans :
      /bronze/meteo/batch/source=noaa/year=YYYY/month=MM/
    (partition = année/mois du lot d'ingestion) puis dépose _SUCCESS
    et complète le manifest.json du lot.
    """
    year, month = ingestion_date.year, f"{ingestion_date.month:02d}"
    base = f"/bronze/meteo/batch/source=noaa/year={year}/month={month}"
    remote = f"{base}/{station}.csv"
    if hdfs_utils.hdfs_exists(remote):
        logger.info("Déjà présent en Bronze : %s — ignoré.", remote)
        return remote
    hdfs_utils.hdfs_upload(local, remote)
    # Manifest du lot (métadonnées d'ingestion, fichier toujours brut)
    manifest_path = f"{base}/manifest.json"
    manifest = []
    try:
        manifest = hdfs_utils.hdfs_read_json(manifest_path)
    except IOError:
        pass
    if not isinstance(manifest, list):
        manifest = []
    manifest.append({
        "station": station,
        "file": f"{station}.csv",
        "bytes": os.path.getsize(local),
        "first_date": read_first_date(local),
        "ingested_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "partition": f"year={year}/month={month}",
    })
    hdfs_utils.hdfs_write_json(manifest_path, manifest)
    hdfs_utils.write_success(base)
    logger.info("Ingéré en Bronze : %s", remote)
    return remote


# ---------------------------------------------------------------------------
# 5) Mode SYNTHETIC : génération locale de CSV au schéma NOAA
# ---------------------------------------------------------------------------
def synthetic_station_file(cache_dir: str, station_id: str, city: str, country: str,
                           start_year: int, end_year: int, seed: int) -> str:
    """
    Génère un CSV NOAA réaliste (valeurs en dixièmes, dates journalières,
    marche aléatoire saisonnière) pour une station. ~7 300 lignes/an.
    """
    rng = random.Random(seed)
    lat = round(rng.uniform(41.0, 51.0), 4)
    lon = round(rng.uniform(-5.0, 9.0), 4)
    elev = round(rng.uniform(10, 1200), 1)
    local = os.path.join(cache_dir, f"{station_id}.csv")
    if os.path.exists(local):
        return local
    tmp = local + ".part"
    with open(tmp, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(NOAA_HEADER)
        d = dt.date(start_year, 1, 1)
        end = dt.date(end_year, 12, 31)
        phase = rng.uniform(0, 2 * math.pi)
        base_temp = rng.uniform(80, 160)          # dixièmes de °C (8-16 °C)
        while d <= end:
            seasonal = 140 * math.sin(2 * math.pi * (d.timetuple().tm_yday - 80) / 365.25)
            noise = rng.gauss(0, 30)
            tmax = max(-300, int(base_temp + seasonal / 2 + noise))
            tmin = int(tmax - rng.uniform(30, 120))
            tavg = (tmax + tmin) // 2
            prcp = 0 if rng.random() > 0.35 else int(rng.expovariate(0.15) * 10)
            awnd = int(rng.uniform(5, 90))
            snow = 0 if prcp == 0 or tmax > 0 else int(prcp * rng.uniform(0.5, 1.5))
            writer.writerow([station_id, d.strftime("%Y%m%d"), f"{lat:.4f}", f"{lon:.4f}",
                             f"{elev:.1f}", f"{city}, {country}",
                             prcp, tmax, tmin, tavg, snow, 0, awnd])
            d += dt.timedelta(days=1)
    os.replace(tmp, local)
    logger.info("Généré (synthétique) : %s (%.1f Mo)",
                local, os.path.getsize(local) / 1e6)
    return local


# ---------------------------------------------------------------------------
# 6) Pipeline principal
# ---------------------------------------------------------------------------
def run_ingestion(args: argparse.Namespace) -> int:
    target_gb = float(args.target_gb or os.environ.get("NOAA_TARGET_GB", "6.6"))
    base_url = args.base_url or os.environ.get("NOAA_BASE_URL", DEFAULT_BASE_URL)
    cache_dir = args.cache_dir or os.environ.get("NOAA_CACHE_DIR",
                                                 os.path.join(os.getcwd(), "data", "noaa"))
    os.makedirs(cache_dir, exist_ok=True)
    ingestion_date = dt.date.today()
    already = ingested_set()

    if args.synthetic:
        stations: List[Tuple[str, int]] = []
        n = args.synthetic_stations or 200
        start_year = int(os.environ.get("NOAA_START_YEAR", "2022"))
        end_year = int(os.environ.get("NOAA_END_YEAR", "2025"))
        cities = ["PARIS", "LYON", "MARSEILLE", "BORDEAUX", "LILLE", "NANTES",
                  "TOULOUSE", "NICE", "STRASBOURG", "RENNES"]
        files: List[str] = []
        for i in range(n):
            sid = f"FRM{i:07d}"
            local = synthetic_station_file(
                cache_dir, sid, cities[i % len(cities)], "FR",
                start_year, end_year, seed=42 + i)
            files.append(local)
            stations.append((sid, os.path.getsize(local)))
        for sid, _size in stations:
            if sid in already:
                continue
            upload_to_bronze(os.path.join(cache_dir, f"{sid}.csv"), sid, ingestion_date)
            already.add(sid)
        save_ingested(already)
        return 0

    if args.dry_run:
        logger.info("DRY RUN : énumération des stations NOAA...")
        urls = list_station_files(base_url, args.max_pages)
        logger.info("%d stations disponibles sur le portail.", len(urls))
        if urls:
            sizes = station_sizes(urls, limit=300)
            total = sum(s for _, s in sizes)
            logger.info("Top stations par taille : %s", sizes[:5])
            logger.info("Volume total échantillonné : %.2f Go", total / (1024 ** 3))
        return 0

    # --- Mode réel ---
    logger.info("Énumération des fichiers NOAA (%s)...", base_url)
    urls = list_station_files(base_url, args.max_pages)
    if not urls:
        logger.error("Aucune station trouvée sur %s — vérifiez le réseau ou utilisez --synthetic.",
                     base_url)
        return 2
    selected = select_stations(urls, target_gb, args.limit_stations)
    if not selected:
        logger.error("Sélection vide.")
        return 2

    plan = [u for u, _ in selected if u.rsplit("/", 1)[-1][:-4] not in already]
    logger.info("Plan d'ingestion : %d nouvelles stations (déjà ingérées : %d).",
                len(plan), len(selected) - len(plan))

    ok, failed = 0, 0
    for url in plan:
        station = url.rsplit("/", 1)[-1][:-4]
        local = download_station(url, cache_dir, force=args.force)
        if not local:
            failed += 1
            logger.error("Téléchargement impossible : %s", station)
            continue
        upload_to_bronze(local, station, ingestion_date)
        already.add(station)
        ok += 1
        # Sauvegarde progressive de la liste ingérée (reprise après interruption)
        if ok % 10 == 0:
            save_ingested(already)
    save_ingested(already)
    logger.info("Terminé : %d ingérées, %d échecs.", ok, failed)
    hdfs_utils.write_success("/bronze/meteo/batch/source=noaa")
    return 0 if failed == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingestion batch NOAA (GHCN-D) vers Bronze")
    parser.add_argument("--target-gb", type=float, default=None, help="Cible de volume en Go")
    parser.add_argument("--limit-stations", type=int, default=0, help="Limite du nombre de stations")
    parser.add_argument("--max-pages", type=int, default=MAX_PAGES, help="Pages du portail à parcourir")
    parser.add_argument("--base-url", default=None, help="URL du portail NOAA")
    parser.add_argument("--cache-dir", default=None, help="Répertoire de cache local")
    parser.add_argument("--synthetic", action="store_true",
                        help="Génère des données synthétiques (même schéma NOAA)")
    parser.add_argument("--synthetic-stations", type=int, default=None,
                        help="Nombre de stations synthétiques")
    parser.add_argument("--force", action="store_true", help="Retélécharge même si déjà en cache")
    parser.add_argument("--dry-run", action="store_true", help="Simule sans télécharger")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s [batch_ingest] %(message)s")
    return run_ingestion(args)


if __name__ == "__main__":
    sys.exit(main())
