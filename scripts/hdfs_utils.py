# -*- coding: utf-8 -*-
"""
hdfs_utils.py : utilitaires HDFS (client WebHDFS) pour le projet DataLake Météo.
=================================================================================
Ce module expose des fonctions simples basées sur l'API REST WebHDFS
(http://namenode:9870/webhdfs/v1/...) afin de fonctionner depuis n'importe
quel conteneur (Airflow, Kafka, Jupyter, Spark) sans binaire HDFS local.

Fonctions principales :
    - hdfs_mkdirs / hdfs_exists / hdfs_list / hdfs_delete
    - hdfs_upload / hdfs_download / hdfs_read_text / hdfs_write_text
    - hdfs_size (taille récursive d'un répertoire)
    - has_success / write_success  (marqueurs d'idempotence "_SUCCESS")
    - quota_reached (vérification du quota Bronze)

Auteur : Youcef, équipe DataLake Météo
"""

from __future__ import annotations

import io
import json
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger("hdfs_utils")

# ---------------------------------------------------------------------------
# Configuration (via variables d'environnement, avec valeurs par défaut)
# ---------------------------------------------------------------------------
HDFS_NAMENODE = os.environ.get("HDFS_NAMENODE", "namenode")
HDFS_WEBHDFS_PORT = os.environ.get("HDFS_WEBHDFS_PORT", "9870")
WEBHDFS_BASE = f"http://{HDFS_NAMENODE}:{HDFS_WEBHDFS_PORT}/webhdfs/v1"

RETRIES = 5
BACKOFF_SECONDS = 3
REQUEST_TIMEOUT = 60


def namenode_url() -> str:
    """URL de base du Namenode (WebHDFS)."""
    return WEBHDFS_BASE


def _request(method: str, path: str, params: Optional[Dict[str, Any]] = None,
             **kwargs: Any) -> requests.Response:
    """Envoie une requête WebHDFS avec retries + backoff."""
    url = f"{WEBHDFS_BASE}{path}"
    params = dict(params or {})
    params.setdefault("user.name", os.environ.get("HDFS_USER", "root"))
    last_exc: Optional[Exception] = None
    for attempt in range(1, RETRIES + 1):
        try:
            resp = requests.request(method, url, params=params, timeout=REQUEST_TIMEOUT, **kwargs)
            # WebHDFS CREATE/APPEND renvoient un 307 avec l'URL du datanode
            if resp.status_code in (301, 307) and "Location" in resp.headers:
                location = resp.headers["Location"]
                logger.debug("Redirection WebHDFS vers %s", location)
                resp = requests.request(method, location, timeout=REQUEST_TIMEOUT, **kwargs)
            if resp.status_code == 404:
                # Absent = réponse NORMALE pour exists()/read sur un fichier
                # inexistant : on ne retente pas (gain ~30 s par vérification).
                raise IOError(f"WebHDFS {method} {path} -> HTTP 404 (absent)")
            if resp.status_code < 400:
                return resp
            last_exc = IOError(f"WebHDFS {method} {path} -> HTTP {resp.status_code}: {resp.text[:300]}")
        except requests.RequestException as exc:  # réseau / timeouts
            last_exc = exc
        logger.warning("Tentative %d/%d échouée pour %s %s (%s)",
                       attempt, RETRIES, method, path, last_exc)
        if attempt < RETRIES:
            time.sleep(BACKOFF_SECONDS * attempt)
    raise IOError(f"WebHDFS {method} {path} a échoué après {RETRIES} tentatives : {last_exc}")


def _json(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    resp = _request("GET", path, params)
    return resp.json()


# ---------------------------------------------------------------------------
# Opérations sur les fichiers / répertoires
# ---------------------------------------------------------------------------
def hdfs_exists(path: str) -> bool:
    """Retourne True si le chemin existe (fichier ou répertoire)."""
    try:
        _json(path, {"op": "GETFILESTATUS"})
        return True
    except IOError:
        return False


def hdfs_mkdirs(path: str) -> bool:
    """Crée un répertoire (et ses parents) s'il n'existe pas déjà."""
    if hdfs_exists(path):
        return True
    _request("PUT", path, {"op": "MKDIRS"})
    return True


def hdfs_list(path: str) -> List[str]:
    """Liste les noms des entrées d'un répertoire HDFS."""
    data = _json(path, {"op": "LISTSTATUS"})
    return [entry["pathSuffix"] for entry in data["FileStatuses"]["FileStatus"]]


def hdfs_list_full(path: str) -> List[Dict[str, Any]]:
    """Liste les entrées d'un répertoire avec leurs métadonnées (taille, type...)."""
    data = _json(path, {"op": "LISTSTATUS"})
    return data["FileStatuses"]["FileStatus"]


def hdfs_delete(path: str, recursive: bool = True) -> bool:
    """Supprime un fichier ou répertoire (récursif par défaut)."""
    _request("DELETE", path, {"op": "DELETE", "recursive": str(recursive).lower()})
    return True


def hdfs_size(path: str) -> int:
    """Taille totale (octets) d'un fichier ou répertoire, récursivement."""
    data = _json(path, {"op": "GETCONTENTSUMMARY"})
    return int(data["ContentSummary"]["length"])


def hdfs_size_gb(path: str) -> float:
    """Taille en Go (base 1024)."""
    return hdfs_size(path) / (1024 ** 3)


# ---------------------------------------------------------------------------
# Transferts de contenu
# ---------------------------------------------------------------------------
def hdfs_upload(local_path: str, remote_path: str, overwrite: bool = True) -> None:
    """Upload un fichier local vers HDFS (flux binaire, création + redirect 307)."""
    hdfs_mkdirs(os.path.dirname(remote_path))
    with open(local_path, "rb") as fh:
        resp = _request("PUT", remote_path,
                        {"op": "CREATE", "overwrite": str(overwrite).lower()},
                        data=fh)
        if resp.status_code >= 400:
            raise IOError(f"Échec upload {local_path} -> {remote_path} : HTTP {resp.status_code}")
    logger.info("Upload OK : %s -> %s", local_path, remote_path)


def hdfs_download(remote_path: str, local_path: str) -> None:
    """Télécharge un fichier HDFS vers le système de fichiers local."""
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    resp = _request("GET", remote_path, {"op": "OPEN"})
    with open(local_path, "wb") as fh:
        fh.write(resp.content)
    logger.info("Download OK : %s -> %s", remote_path, local_path)


def hdfs_read_text(remote_path: str) -> str:
    """Lit un fichier texte HDFS et retourne son contenu."""
    resp = _request("GET", remote_path, {"op": "OPEN"})
    return resp.text


def hdfs_write_text(remote_path: str, text: str) -> None:
    """Écrit un texte dans un fichier HDFS (écrasement)."""
    hdfs_mkdirs(os.path.dirname(remote_path))
    data = io.BytesIO(text.encode("utf-8"))
    _request("PUT", remote_path, {"op": "CREATE", "overwrite": "true"}, data=data)


def hdfs_write_json(remote_path: str, payload: Any) -> None:
    """Écrit un objet JSON dans un fichier HDFS."""
    hdfs_write_text(remote_path, json.dumps(payload, ensure_ascii=False, indent=2))


def hdfs_read_json(remote_path: str) -> Any:
    """Lit un fichier JSON HDFS."""
    return json.loads(hdfs_read_text(remote_path))


def hdfs_move(src: str, dst: str) -> None:
    """Déplace (renomme) un fichier/répertoire HDFS."""
    _request("PUT", src, {"op": "RENAME", "destination": dst})


# ---------------------------------------------------------------------------
# Idempotence : marqueurs "_SUCCESS"
# ---------------------------------------------------------------------------
def write_success(path: str, content: str = "") -> None:
    """Dépose le marqueur _SUCCESS dans un répertoire (idempotent)."""
    if hdfs_exists(f"{path}/_SUCCESS"):
        logger.info("Marqueur _SUCCESS déjà présent : %s", path)
        return
    hdfs_mkdirs(path)
    hdfs_write_text(f"{path}/_SUCCESS", content)
    logger.info("Marqueur _SUCCESS déposé : %s", path)


def has_success(path: str) -> bool:
    """True si le marqueur _SUCCESS existe dans le répertoire."""
    return hdfs_exists(f"{path}/_SUCCESS")


def success_paths(base: str, max_depth: int = 3) -> List[str]:
    """
    Parcourt récursivement base (jusqu'à max_depth niveaux) et retourne la
    liste des répertoires contenant un marqueur _SUCCESS.
    Utile pour ne traiter que les partitions Bronze complètement ingérées.
    """
    found: List[str] = []

    def walk(path: str, depth: int) -> None:
        if not hdfs_exists(path):
            return
        entries = hdfs_list_full(path)
        for entry in entries:
            full = f"{path}/{entry['pathSuffix']}"
            if entry["type"] == "DIRECTORY":
                if has_success(full):
                    found.append(full)
                elif depth < max_depth:
                    walk(full, depth + 1)

    walk(base, 0)
    return found


def quota_reached(path: str, quota_gb: float) -> bool:
    """True si la taille du répertoire dépasse le quota (en Go)."""
    if not hdfs_exists(path):
        return False
    size = hdfs_size(path) / (1024 ** 3)
    reached = size >= quota_gb
    if reached:
        logger.warning("QUOTA ATTEINT : %s = %.2f Go >= %.2f Go", path, size, quota_gb)
    return reached


# ---------------------------------------------------------------------------
# CLI de test (utile dans les conteneurs) : python hdfs_utils.py ls /
# ---------------------------------------------------------------------------
def _cli() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if len(sys.argv) < 2:
        print("Usage: python hdfs_utils.py {ls|mkdir|size|exists|put|get} <args...>")
        sys.exit(1)
    cmd = sys.argv[1]
    if cmd == "ls" and len(sys.argv) >= 3:
        for name in hdfs_list(sys.argv[2]):
            print(name)
    elif cmd == "mkdir" and len(sys.argv) >= 3:
        hdfs_mkdirs(sys.argv[2])
        print(f"OK mkdir {sys.argv[2]}")
    elif cmd == "size" and len(sys.argv) >= 3:
        print(f"{hdfs_size_gb(sys.argv[2]):.3f} Go ({hdfs_size(sys.argv[2])} octets)")
    elif cmd == "exists" and len(sys.argv) >= 3:
        print(hdfs_exists(sys.argv[2]))
    elif cmd == "put" and len(sys.argv) >= 4:
        hdfs_upload(sys.argv[2], sys.argv[3])
        print(f"OK put {sys.argv[2]} -> {sys.argv[3]}")
    elif cmd == "get" and len(sys.argv) >= 4:
        hdfs_download(sys.argv[2], sys.argv[3])
        print(f"OK get {sys.argv[2]} -> {sys.argv[3]}")
    else:
        print("Commande inconnue.")
        sys.exit(2)


if __name__ == "__main__":
    _cli()
