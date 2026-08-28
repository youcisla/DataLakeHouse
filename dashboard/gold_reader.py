# -*- coding: utf-8 -*-
"""
gold_reader.py : lecture des tables Gold (Parquet sur HDFS) via WebHDFS REST.
==============================================================================

Le conteneur du dashboard ne dispose pas d'un client HDFS binaire : les tables
Gold sont donc lues à travers l'API REST WebHDFS
(http://{namenode}:{webhdfs_port}/webhdfs/v1/...) avec le module requests.

Fonctions principales :
    - load_config()      : lit dashboard/config.toml (source de vérité de la config).
    - namenode_base()    : URL de base WebHDFS.
    - webhdfs_list()     : liste les entrées d'un répertoire HDFS.
    - download_file()    : télécharge un fichier (op=OPEN, flux).
    - read_parquet_dir() : lit tous les *.parquet d'un répertoire (récursif).
    - read_gold_table()  : wrapper filtrant éventuellement par date (dt).

Auteur : Soufiane, équipe DataLake Météo
"""

from __future__ import annotations

import logging
import os
import re
import sys
import tempfile
import time
from typing import List, Optional

import pandas as pd
import requests

logger = logging.getLogger("gold_reader")

# ---------------------------------------------------------------------------
# Chemin d'import robuste : rend hdfs_utils disponible (Docker ou lancement local)
# ---------------------------------------------------------------------------
_CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
for _candidate in (
    "/opt/project/scripts",
    os.path.join(os.path.dirname(_CURRENT_DIR), "scripts"),
):
    if os.path.isdir(_candidate) and _candidate not in sys.path:
        sys.path.insert(0, _candidate)

try:
    import hdfs_utils  # noqa: F401  (utilitaires partagés, optionnels ici)
except ImportError:
    hdfs_utils = None  # type: ignore

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
_CONFIG_PATH = os.path.join(_CURRENT_DIR, "config.toml")
RETRIES = 3
REQUEST_TIMEOUT = 20
BACKOFF_SECONDS = 1.0
DEFAULT_USER = "root"


def load_config() -> dict:
    """
    Lit le fichier dashboard/config.toml (source de vérité de la configuration).

    Retourne
    --------
    dict
        Le contenu du fichier TOML, ou un dictionnaire vide si le fichier est
        absent / illisible, ou si tomllib n'est pas disponible (Python < 3.11).
    """
    config: dict = {}
    try:
        import tomllib  # Python 3.11+
    except ImportError:
        logger.warning("tomllib indisponible (Python < 3.11) : config.toml ignoré.")
        return config
    try:
        with open(_CONFIG_PATH, "rb") as fh:
            config = tomllib.load(fh)
    except FileNotFoundError:
        logger.warning("Fichier de config absent : %s", _CONFIG_PATH)
    except (OSError, ValueError) as exc:
        logger.warning("Impossible de lire %s : %s", _CONFIG_PATH, exc)
    return config


def namenode_base() -> str:
    """
    Retourne l'URL de base WebHDFS du namenode.

    Priorité : variables d'environnement HDFS_NAMENODE / HDFS_WEBHDFS_PORT,
    puis section [hdfs] de config.toml, puis valeurs par défaut.
    """
    config = load_config()
    hdfs_cfg = config.get("hdfs") or {}
    node = os.environ.get("HDFS_NAMENODE") or hdfs_cfg.get("namenode", "namenode")
    port = os.environ.get("HDFS_WEBHDFS_PORT") or str(hdfs_cfg.get("webhdfs_port", 9870))
    return f"http://{node}:{port}/webhdfs/v1"


def webhdfs_list(remote_dir: str) -> List[str]:
    """
    Liste les noms des entrées d'un répertoire HDFS (op=LISTSTATUS).

    Paramètres
    ----------
    remote_dir : str
        Chemin HDFS du répertoire (ex. /gold/meteo/daily_aggregates).

    Retourne
    --------
    list[str]
        Noms des entrées. Retourne [] si le répertoire n'existe pas (HTTP 404).
    """
    url = f"{namenode_base()}{remote_dir}"
    params = {"op": "LISTSTATUS", "user.name": DEFAULT_USER}
    last_exc: Optional[Exception] = None
    for attempt in range(1, RETRIES + 1):
        try:
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 404:
                return []
            resp.raise_for_status()
            data = resp.json()
            statuses = data.get("FileStatuses", {}).get("FileStatus", [])
            return [entry.get("pathSuffix", "") for entry in statuses]
        except requests.RequestException as exc:
            last_exc = exc
            logger.warning("LISTSTATUS %s : tentative %d/%d (%s)",
                           remote_dir, attempt, RETRIES, exc)
            if attempt < RETRIES:
                time.sleep(BACKOFF_SECONDS * attempt)
    raise requests.ConnectionError(
        f"WebHDFS LISTSTATUS {remote_dir} a échoué après {RETRIES} tentatives : {last_exc}"
    )


def download_file(remote_path: str, local_path: str) -> None:
    """
    Télécharge un fichier HDFS vers le disque local (op=OPEN, flux binaire).

    Paramètres
    ----------
    remote_path : str
        Chemin HDFS du fichier à télécharger.
    local_path : str
        Chemin local de destination.
    """
    url = f"{namenode_base()}{remote_path}"
    params = {"op": "OPEN", "user.name": DEFAULT_USER}
    parent = os.path.dirname(local_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with requests.get(url, params=params, timeout=REQUEST_TIMEOUT, stream=True) as resp:
        resp.raise_for_status()
        with open(local_path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    fh.write(chunk)


def _list_parquet_files(remote_dir: str, max_depth: int = 10) -> List[str]:
    """Parcours récursif de remote_dir pour collecter les chemins des *.parquet."""
    parquet_files: List[str] = []
    stack: List[tuple] = [(remote_dir, 0)]
    while stack:
        current, depth = stack.pop()
        if depth > max_depth:
            continue
        entries = webhdfs_list(current)
        for name in entries:
            if name.startswith("_") or name.startswith("."):
                continue
            full = f"{current.rstrip('/')}/{name}"
            if name.endswith(".parquet"):
                parquet_files.append(full)
            else:
                stack.append((full, depth + 1))
    return parquet_files


def read_parquet_dir(remote_dir: str, local_dir: Optional[str] = None,
                     max_files: int = 200,
                     columns: Optional[List[str]] = None,
                     partition_col: Optional[str] = None) -> pd.DataFrame:
    """
    Lit tous les fichiers *.parquet d'un répertoire HDFS (récursif) dans un DataFrame.

    Les fichiers sont téléchargés dans un répertoire temporaire puis lus avec
    pandas.read_parquet et concaténés. Si le namenode est injoignable
    (ConnectionError) ou qu'aucun fichier n'existe, un DataFrame vide est renvoyé.

    Paramètres
    ----------
    remote_dir : str
        Répertoire HDFS à parcourir (partitions dt= incluses).
    local_dir : Optional[str]
        Répertoire local de destination (sinon tempfile.mkdtemp).
    max_files : int
        Nombre maximal de fichiers Parquet à lire (200 par défaut).
    columns : Optional[List[str]]
        Sous-ensemble de colonnes à lire (projection mémoire), ex.
        ["city", "latitude", "longitude"] pour un catalogue de stations.
    partition_col : Optional[str]
        Nom d'une colonne de partition HDFS (ex. "year") à restituer : sa
        valeur est lue dans le chemin `year=2000/...` et ajoutée au DataFrame,
        car les colonnes de partition ne sont PAS stockées dans les Parquet.

    Retourne
    --------
    pd.DataFrame
        Concaténation des fichiers, ou DataFrame vide en cas d'absence de données.
    """
    try:
        files = _list_parquet_files(remote_dir)
    except requests.ConnectionError as exc:
        logger.warning("Namenode injoignable (%s) : aucune donnée Gold lue.", exc)
        return pd.DataFrame()
    except requests.RequestException as exc:
        logger.warning("Lecture WebHDFS impossible pour %s : %s", remote_dir, exc)
        return pd.DataFrame()

    if not files:
        return pd.DataFrame()

    files = files[:max_files]
    tmpdir = local_dir or tempfile.mkdtemp(prefix="gold_")
    frames: List[pd.DataFrame] = []
    for index, remote in enumerate(files):
        local_path = os.path.join(tmpdir, f"part_{index:04d}.parquet")
        try:
            download_file(remote, local_path)
            frame = pd.read_parquet(local_path, columns=columns)
            if partition_col:
                match = re.search(r"/" + re.escape(partition_col) + r"=([^/]+)/", remote + "/")
                if match:
                    value: object = match.group(1)
                    try:
                        value = int(value)
                    except ValueError:
                        pass
                    frame[partition_col] = value
            frames.append(frame)
        except Exception as exc:  # fichier corrompu / réseau : on continue
            logger.warning("Impossible de lire %s : %s", remote, exc)
            continue
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def read_gold_table(table: str, date_from: Optional[str] = None,
                    date_to: Optional[str] = None,
                    partition_col: Optional[str] = None) -> pd.DataFrame:
    """
    Wrapper de lecture d'une table Gold avec filtre optionnel sur la colonne dt.

    Paramètres
    ----------
    table : str
        Nom de la table Gold (ex. daily_aggregates).
    date_from / date_to : Optional[str]
        Bornes de date (format YYYY-MM-DD ou convertible par pandas).
    partition_col : Optional[str]
        Colonne de partition HDFS a restituer (ex. "dt" pour ml_predictions,
        "month" pour climate_profile) : sa valeur est lue dans le chemin.

    Retourne
    --------
    pd.DataFrame
        DataFrame de la table (filtré si dates fournies), vide si aucune donnée.
    """
    df = _cached_read_parquet_dir(f"/gold/meteo/{table}", partition_col=partition_col)
    if df.empty or "dt" not in df.columns:
        return df.copy()
    dt_series = pd.to_datetime(df["dt"], errors="coerce")
    mask = pd.Series(True, index=df.index)
    if date_from is not None:
        mask &= dt_series >= pd.to_datetime(date_from)
    if date_to is not None:
        mask &= dt_series <= pd.to_datetime(date_to)
    return df[mask].reset_index(drop=True).copy()


# ---------------------------------------------------------------------------
# Cache Streamlit (TTL 60 s) si disponible, sinon simple fonction
# ---------------------------------------------------------------------------
try:
    import streamlit as st

    _cached_read_parquet_dir = st.cache_data(ttl=60, show_spinner=False)(read_parquet_dir)
except Exception:
    _cached_read_parquet_dir = read_parquet_dir
