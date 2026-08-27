# -*- coding: utf-8 -*-
"""
meteofrance_ingest.py : source batch « archives » du DataLake Météo.
=====================================================================
Ingestion des **données climatologiques de base quotidiennes** publiées en
open data par Météo-France sur ``meteo.data.gouv.fr`` (portail exploité par
les notebooks https://github.com/loicduffar/meteo.data-Tools).

Jeu de données : « Données climatologiques de base - quotidiennes »,
fichiers ``RR-T-Vent`` (précipitations, températures, vent), un fichier CSV
gzippé **par département** et par période :

    {BASE}/Q_{DEP}_latest-2025-2026_RR-T-Vent.csv.gz
    {BASE}/Q_{DEP}_previous-1950-2024_RR-T-Vent.csv.gz

Les fichiers sont déposés **BRUTS** (gzip d'origine, aucune transformation)
dans le Bronze, selon la convention de partitionnement imposée par le sujet :

    /bronze/meteo/batch/source=meteofrance/year=YYYY/month=MM/

IDEMPOTENCE (critère central du TP) :
    - ``_ingested.json`` à la racine de la source : liste des lots déjà
      ingérés (clé ``DEP:période``) ; un lot n'est jamais retéléversé ;
    - ``manifest.json`` par partition : métadonnées du lot ;
    - ``_SUCCESS`` par partition : marqueur consommé par la couche Silver.

Mode ``--synthetic`` : génère des CSV au **même schéma** (séparateur ``;``,
valeurs manquantes vides) lorsque le réseau bloque data.gouv.fr, afin que la
chaîne Bronze -> Silver -> Gold reste démontrable de bout en bout.

Toute la logique « pure » (URL, partitions, parsing, génération) est
importable **sans réseau ni HDFS** : elle est couverte par les tests unitaires.

Auteur : équipe DataLake Météo (couche Medallion)
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import io
import logging
import math
import os
import random
import sys
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger("meteofrance_ingest")

# ---------------------------------------------------------------------------
# Contrat de la source (aucune dépendance : testable sans réseau)
# ---------------------------------------------------------------------------

#: Racine des fichiers QUOT publiés par Météo-France (open data, sans clé).
DEFAULT_BASE_URL = "https://meteofrance.s3.sbg.io.cloud.ovh.net/data/synchro_ftp/BASE/QUOT"

#: Nom logique de la source (utilisé dans les chemins Bronze et en Silver).
SOURCE_NAME = "meteofrance"

#: Racine Bronze de la source batch.
BRONZE_ROOT = f"/bronze/meteo/batch/source={SOURCE_NAME}"

#: Séparateur de colonnes des CSV Météo-France.
CSV_SEPARATOR = ";"

#: Colonnes du fichier RR-T-Vent réellement exploitées en Silver.
#: (le fichier officiel contient en plus les colonnes de qualité Qxxx et les
#: colonnes de rafales, que nous conservons telles quelles en Bronze).
MF_COLUMNS: List[str] = [
    "NUM_POSTE",   # identifiant du poste (8 caractères)
    "NOM_USUEL",   # nom usuel du poste
    "LAT",         # latitude (degrés décimaux)
    "LON",         # longitude (degrés décimaux)
    "ALTI",        # altitude (m)
    "AAAAMMJJ",    # date du relevé
    "RR",          # cumul de précipitations du jour (mm)
    "QRR",         # code qualité de RR
    "TN",          # température minimale (°C)
    "QTN",
    "TX",          # température maximale (°C)
    "QTX",
    "TM",          # température moyenne (°C)
    "QTM",
    "FFM",         # vitesse moyenne du vent à 10 m (m/s)
    "QFFM",
]

#: Périodes publiées par Météo-France pour les fichiers QUOT.
PERIOD_LATEST = "latest-2025-2026"
PERIOD_PREVIOUS = "previous-1950-2024"

#: Première année couverte par le fichier « latest ».
_LATEST_FIRST_YEAR = 2025

#: Départements métropolitains (la Corse utilise 2A / 2B) + outre-mer.
METROPOLITAN_DEPARTMENTS: List[str] = (
    [f"{n:02d}" for n in range(1, 20)]
    + ["2A", "2B"]
    + [f"{n:02d}" for n in range(21, 96)]
)
OVERSEAS_DEPARTMENTS: List[str] = ["971", "972", "973", "974", "975", "984", "985", "986", "987", "988"]

#: Départements des cinq villes suivies par le flux temps réel Open-Meteo
#: (Paris, Lyon, Marseille, Bordeaux, Lille) : sélection par défaut, afin que
#: les deux sources se recoupent dans les agrégats Gold.
DEFAULT_DEPARTMENTS: List[str] = ["75", "69", "13", "33", "59"]

#: Villes de référence par département (utilisé par le mode synthétique).
DEPARTMENT_CITIES: Dict[str, str] = {
    "75": "PARIS-MONTSOURIS",
    "69": "LYON-BRON",
    "13": "MARSEILLE-MARIGNANE",
    "33": "BORDEAUX-MERIGNAC",
    "59": "LILLE-LESQUIN",
}

#: Coordonnées approximatives par département (mode synthétique).
DEPARTMENT_COORDS: Dict[str, Tuple[float, float, float]] = {
    "75": (48.8219, 2.3372, 75.0),
    "69": (45.7269, 4.9447, 197.0),
    "13": (43.4378, 5.2158, 9.0),
    "33": (44.8306, -0.6914, 47.0),
    "59": (50.5700, 3.0975, 47.0),
}

HTTP_TIMEOUT = 120
DOWNLOAD_RETRIES = 3


# ---------------------------------------------------------------------------
# 1) Fonctions pures : URLs, périodes, partitions
# ---------------------------------------------------------------------------

def normalize_department(dep: str) -> str:
    """
    Normalise un code département vers la forme utilisée par Météo-France.

    Exemples :
        "1"   -> "01"      (métropole sur deux caractères)
        "2a"  -> "2A"      (Corse en majuscules)
        "971" -> "971"     (outre-mer sur trois caractères)
    """
    dep = str(dep).strip().upper()
    if not dep:
        raise ValueError("Code département vide.")
    if dep.isdigit() and len(dep) < 2:
        return dep.zfill(2)
    return dep


def periods_for_years(start_year: int, end_year: int) -> List[str]:
    """
    Retourne les périodes de fichiers à télécharger pour couvrir [start, end].

    Le fichier ``previous`` couvre 1950-2024, le fichier ``latest`` 2025-2026.
    """
    if end_year < start_year:
        raise ValueError(f"Fenêtre invalide : {start_year} > {end_year}")
    periods: List[str] = []
    if start_year < _LATEST_FIRST_YEAR:
        periods.append(PERIOD_PREVIOUS)
    if end_year >= _LATEST_FIRST_YEAR:
        periods.append(PERIOD_LATEST)
    return periods


def build_url(department: str, period: str, base_url: str = DEFAULT_BASE_URL) -> str:
    """Construit l'URL du fichier QUOT RR-T-Vent d'un département/période."""
    dep = normalize_department(department)
    return f"{base_url.rstrip('/')}/Q_{dep}_{period}_RR-T-Vent.csv.gz"


def batch_key(department: str, period: str) -> str:
    """Clé d'idempotence d'un lot (département + période)."""
    return f"{normalize_department(department)}:{period}"


def file_name(department: str, period: str) -> str:
    """Nom du fichier BRUT tel que déposé en Bronze."""
    return f"Q_{normalize_department(department)}_{period}_RR-T-Vent.csv.gz"


def bronze_partition(ingestion_date: dt.date) -> str:
    """
    Partition Bronze du lot : ``source=meteofrance/year=YYYY/month=MM``.

    Conformément au sujet, la partition correspond à l'année/mois **du lot
    d'ingestion** ; le contenu reste strictement brut et n'est stocké qu'une
    fois (la fenêtre temporelle des relevés est appliquée en Silver).
    """
    return f"{BRONZE_ROOT}/year={ingestion_date.year}/month={ingestion_date.month:02d}"


def bronze_target(department: str, period: str, ingestion_date: dt.date) -> str:
    """Chemin HDFS complet du fichier brut en Bronze."""
    return f"{bronze_partition(ingestion_date)}/{file_name(department, period)}"


def plan_batches(departments: Iterable[str], start_year: int, end_year: int,
                 already: Optional[Iterable[str]] = None) -> List[Tuple[str, str]]:
    """
    Construit le plan d'ingestion : liste de (département, période) à traiter,
    **privée des lots déjà ingérés** (idempotence).
    """
    done = set(already or ())
    periods = periods_for_years(start_year, end_year)
    plan: List[Tuple[str, str]] = []
    for dep in departments:
        dep = normalize_department(dep)
        for period in periods:
            if batch_key(dep, period) in done:
                continue
            plan.append((dep, period))
    return plan


# ---------------------------------------------------------------------------
# 2) Mode synthétique : CSV au schéma Météo-France exact
# ---------------------------------------------------------------------------

def _mf_float(value: Optional[float], digits: int = 1) -> str:
    """Formate une valeur pour le CSV Météo-France ('' si manquante)."""
    if value is None:
        return ""
    return f"{value:.{digits}f}"


def synthetic_rows(department: str, start_date: dt.date, end_date: dt.date,
                   seed: int = 42, missing_rate: float = 0.02) -> List[List[str]]:
    """
    Génère les lignes d'un fichier QUOT synthétique (schéma :data:`MF_COLUMNS`).

    Le signal est une sinusoïde saisonnière bruitée ; ``missing_rate`` des
    relevés ont des champs vides, comme dans les fichiers réels, afin que la
    validation et le nettoyage Silver soient réellement exercés.
    """
    dep = normalize_department(department)
    rng = random.Random(seed)
    lat, lon, alti = DEPARTMENT_COORDS.get(dep, (46.5, 2.5, 150.0))
    name = DEPARTMENT_CITIES.get(dep, f"POSTE-{dep}")
    num_poste = f"{dep.ljust(2, '0')[:2]}{rng.randint(0, 999999):06d}"

    rows: List[List[str]] = []
    day = start_date
    base_temp = rng.uniform(9.0, 15.0)
    while day <= end_date:
        seasonal = 9.0 * math.sin(2 * math.pi * (day.timetuple().tm_yday - 105) / 365.25)
        tm = base_temp + seasonal + rng.gauss(0, 2.0)
        amplitude = rng.uniform(4.0, 11.0)
        tn = round(tm - amplitude / 2.0, 1)
        tx = round(tm + amplitude / 2.0, 1)
        rr = 0.0 if rng.random() > 0.38 else round(rng.expovariate(0.18), 1)
        ffm = round(rng.uniform(0.8, 9.0), 1)

        # Quelques relevés incomplets, comme dans les données réelles.
        if rng.random() < missing_rate:
            tm_out: Optional[float] = None
        else:
            tm_out = round(tm, 1)
        if rng.random() < missing_rate:
            rr_out: Optional[float] = None
        else:
            rr_out = rr

        rows.append([
            num_poste, name, f"{lat:.6f}", f"{lon:.6f}", f"{alti:.0f}",
            day.strftime("%Y%m%d"),
            _mf_float(rr_out), "1",
            _mf_float(tn), "1",
            _mf_float(tx), "1",
            _mf_float(tm_out), "1",
            _mf_float(ffm), "1",
        ])
        day += dt.timedelta(days=1)
    return rows


def write_synthetic_file(path: str, department: str, start_date: dt.date,
                         end_date: dt.date, seed: int = 42) -> str:
    """Écrit un fichier ``.csv.gz`` synthétique au format Météo-France."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    rows = synthetic_rows(department, start_date, end_date, seed=seed)
    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter=CSV_SEPARATOR, lineterminator="\n")
    writer.writerow(MF_COLUMNS)
    writer.writerows(rows)
    tmp = path + ".part"
    with gzip.open(tmp, "wt", encoding="utf-8", newline="") as fh:
        fh.write(buffer.getvalue())
    os.replace(tmp, path)
    logger.info("Généré (synthétique) : %s (%d lignes, %.2f Mo)",
                path, len(rows), os.path.getsize(path) / 1e6)
    return path


# ---------------------------------------------------------------------------
# 3) Téléchargement (reprise + retries)
# ---------------------------------------------------------------------------

def download(url: str, local_path: str, force: bool = False) -> Optional[str]:
    """Télécharge un fichier QUOT dans le cache local (retours en arrière sûrs)."""
    import requests  # import local : le module reste importable sans requests

    os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
    if os.path.exists(local_path) and not force:
        logger.info("Déjà en cache : %s", local_path)
        return local_path
    tmp = local_path + ".part"
    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        try:
            with requests.get(url, stream=True, timeout=HTTP_TIMEOUT) as resp:
                resp.raise_for_status()
                with open(tmp, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=1024 * 1024):
                        fh.write(chunk)
            os.replace(tmp, local_path)
            logger.info("Téléchargé : %s (%.1f Mo)", url, os.path.getsize(local_path) / 1e6)
            return local_path
        except Exception as exc:  # noqa: BLE001 - réseau : on retente
            logger.warning("Échec téléchargement %s (tentative %d/%d) : %s",
                           url, attempt, DOWNLOAD_RETRIES, exc)
            time.sleep(3 * attempt)
    return None


# ---------------------------------------------------------------------------
# 4) Dépôt en Bronze (idempotent)
# ---------------------------------------------------------------------------

def load_ingested() -> set:
    """
    Lots deja ingeres (cles ``DEP:periode``), depuis le magasin de checkpoints.

    L'ancien ``_ingested.json`` est repris automatiquement s'il existe encore
    (voir ``checkpoint._import_legacy_bronze``) : aucun lot n'est retelecharge.
    """
    import checkpoint

    return set(checkpoint.load(checkpoint.STAGE_BRONZE).get("done", []))


def commit_batch(department: str, period: str) -> None:
    """
    Marque UN lot comme ingere, et committe immediatement.

    C'est le point cle de la reprise : l'ancienne version ne sauvegardait que
    tous les 5 lots, donc une interruption pouvait faire rejouer 4 lots deja
    deposes en Bronze. Ici, une interruption ne coute jamais plus que le lot
    en cours.
    """
    import checkpoint

    checkpoint.mark_done(checkpoint.STAGE_BRONZE, batch_key(department, period))


def manifest_entry(department: str, period: str, local_path: str,
                   ingestion_date: dt.date) -> Dict[str, Any]:
    """Construit l'entrée de manifeste d'un lot (métadonnées, fichier brut)."""
    return {
        "department": normalize_department(department),
        "period": period,
        "file": file_name(department, period),
        "bytes": os.path.getsize(local_path) if os.path.exists(local_path) else 0,
        "source_url": build_url(department, period),
        "partition": f"year={ingestion_date.year}/month={ingestion_date.month:02d}",
        "ingested_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }


def upload_batch(local_path: str, department: str, period: str,
                 ingestion_date: dt.date) -> str:
    """
    Dépose le fichier BRUT en Bronze puis met à jour ``manifest.json`` et
    ``_SUCCESS``. Ne fait rien si le fichier est déjà présent (idempotence).
    """
    import hdfs_utils

    partition = bronze_partition(ingestion_date)
    remote = bronze_target(department, period, ingestion_date)
    if hdfs_utils.hdfs_exists(remote):
        logger.info("Déjà présent en Bronze, ignoré : %s", remote)
        return remote

    hdfs_utils.hdfs_upload(local_path, remote)

    manifest_path = f"{partition}/manifest.json"
    try:
        manifest = hdfs_utils.hdfs_read_json(manifest_path)
    except IOError:
        manifest = []
    if not isinstance(manifest, list):
        manifest = []
    manifest.append(manifest_entry(department, period, local_path, ingestion_date))
    hdfs_utils.hdfs_write_json(manifest_path, manifest)
    hdfs_utils.write_success(partition)
    logger.info("Ingéré en Bronze : %s", remote)
    return remote


# ---------------------------------------------------------------------------
# 5) Pipeline
# ---------------------------------------------------------------------------

def run(args: argparse.Namespace) -> int:
    """Exécute l'ingestion batch Météo-France vers Bronze."""
    import hdfs_utils

    departments = [normalize_department(d) for d in (args.departments or DEFAULT_DEPARTMENTS)]
    start_year = int(args.start_year)
    end_year = int(args.end_year)
    cache_dir = args.cache_dir or os.environ.get(
        "MF_CACHE_DIR", os.path.join(os.getcwd(), "data", "meteofrance"))
    os.makedirs(cache_dir, exist_ok=True)
    ingestion_date = dt.date.today()

    # --dry-run reste 100 % hors ligne : ni HDFS, ni téléchargement.
    if args.dry_run:
        plan = plan_batches(departments, start_year, end_year)
        for dep, period in plan:
            logger.info("DRY RUN : %s -> %s",
                        build_url(dep, period, args.base_url),
                        bronze_target(dep, period, ingestion_date))
        logger.info("DRY RUN : %d lot(s) planifié(s).", len(plan))
        return 0

    quota_gb = float(os.environ.get("BRONZE_QUOTA_GB", "10.5"))
    if hdfs_utils.quota_reached("/bronze", quota_gb):
        logger.warning("Quota Bronze atteint (%.1f Go) : ingestion batch sautée.", quota_gb)
        return 0

    already = load_ingested()
    plan = plan_batches(departments, start_year, end_year, already)

    if not plan:
        logger.info("Rien à faire : les %d lot(s) demandés sont déjà en Bronze.", len(already))
        hdfs_utils.write_success(BRONZE_ROOT)
        return 0

    import checkpoint

    run = checkpoint.run_id("bronze")
    logger.info("Run %s : %d lot(s) a ingerer, %d deja fait(s).",
                run, len(plan), len(already))

    ok, failed = 0, 0
    for index, (dep, period) in enumerate(plan, start=1):
        key = batch_key(dep, period)
        local = os.path.join(cache_dir, file_name(dep, period))
        logger.info("[%d/%d] lot %s", index, len(plan), key)
        try:
            if args.synthetic:
                write_synthetic_file(
                    local, dep,
                    dt.date(start_year, 1, 1), dt.date(end_year, 12, 31),
                    seed=1000 + int("".join(c for c in dep if c.isdigit()) or 0),
                )
            else:
                url = build_url(dep, period, args.base_url)
                if download(url, local, force=args.force) is None:
                    failed += 1
                    logger.error("Lot indisponible : %s (%s)", key, url)
                    continue
            upload_batch(local, dep, period, ingestion_date)
        except Exception as exc:  # noqa: BLE001 - un lot rate n'arrete pas les autres
            failed += 1
            logger.error("Lot %s en echec : %s", key, exc)
            continue
        # COMMIT IMMEDIAT : le lot est en Bronze, il ne sera jamais rejoue.
        commit_batch(dep, period)
        already.add(key)
        ok += 1

    hdfs_utils.write_success(BRONZE_ROOT)
    checkpoint.record_run(checkpoint.STAGE_BRONZE, run,
                          "success" if failed == 0 else "partial",
                          batches_ingested=ok, batches_failed=failed,
                          batches_total=len(plan))
    logger.info("Ingestion Meteo-France terminee : %d lot(s) ingeres, %d echec(s).", ok, failed)
    return 0 if failed == 0 else 1


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingestion batch Météo-France (QUOT RR-T-Vent) vers Bronze")
    parser.add_argument("--departments", nargs="*", default=None,
                        help=f"Codes départements (défaut : {' '.join(DEFAULT_DEPARTMENTS)})")
    parser.add_argument("--start-year", type=int,
                        default=int(os.environ.get("MF_START_YEAR", "2022")))
    parser.add_argument("--end-year", type=int,
                        default=int(os.environ.get("MF_END_YEAR", "2026")))
    parser.add_argument("--base-url", default=os.environ.get("MF_BASE_URL", DEFAULT_BASE_URL))
    parser.add_argument("--cache-dir", default=None)
    parser.add_argument("--synthetic", action="store_true",
                        help="Génère des fichiers au schéma Météo-France (réseau filtré).")
    parser.add_argument("--force", action="store_true", help="Retélécharge même si en cache.")
    parser.add_argument("--dry-run", action="store_true", help="Affiche le plan sans ingérer.")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s [meteofrance_ingest] %(message)s")
    return run(parse_args(argv))


if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    sys.exit(main())
