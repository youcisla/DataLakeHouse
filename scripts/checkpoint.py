# -*- coding: utf-8 -*-
"""
checkpoint.py : reprise fine des traitements du DataLake Météo.
================================================================
Le sujet impose qu'« un DAG interrompu puisse être relancé sans dupliquer les
données déjà traitées ». Les marqueurs ``_SUCCESS`` couvrent la granularité
**partition** ; ce module ajoute la granularité **unité de travail** :

    bronze_meteofrance   une cle par lot (departement x periode)
    silver               une cle par partition dt
    gold                 une cle par table et par dt

Chaque unite terminee est **committee immediatement** : une interruption ne
coute jamais plus que l'unite en cours. Un journal borne des derniers runs
(``runs``) accompagne l'etat, ce qui permet de repondre a « ou en est-on ? »
sans lire les donnees.

Stockage : JSON dans HDFS sous ``/checkpoints/medallion/<etape>.json``, ecrit
via ``hdfs_utils`` (WebHDFS), donc disponible dans **toutes** les images du
projet, sans dependance supplementaire. Un backend fichier local existe pour
les tests et l'usage hors cluster :

    METEO_CHECKPOINT_BACKEND=file
    METEO_CHECKPOINT_DIR=/chemin/local

Toute la logique d'etat est exposee en **fonctions pures** (aucune E/S), ce
qui la rend testable sans HDFS.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger("checkpoint")

#: Racine HDFS des checkpoints.
CHECKPOINT_ROOT = "/checkpoints/medallion"

#: Version du schema d'etat (permet une migration future explicite).
STATE_VERSION = 1

#: Nombre de runs conserves dans le journal (borne la taille du fichier).
MAX_RUNS = 20

#: Etapes connues de la couche Medallion.
STAGE_BRONZE = "bronze_meteofrance"
STAGE_SILVER = "silver"
STAGE_GOLD = "gold"
#: Etapes de `make all` elles-memes : une cle par etape terminee, ce qui rend
#: le workflow complet reprenable et pas seulement les traitements de donnees.
STAGE_WORKFLOW = "workflow"
KNOWN_STAGES: List[str] = [STAGE_WORKFLOW, STAGE_BRONZE, STAGE_SILVER, STAGE_GOLD]

#: Ancien emplacement de l'etat Bronze, importe automatiquement s'il existe.
LEGACY_BRONZE_PATH = "/bronze/meteo/batch/source=meteofrance/_ingested.json"


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# Fonctions pures : le schema d'etat (testables sans HDFS)
# ---------------------------------------------------------------------------

def new_state(stage: str) -> Dict[str, Any]:
    """Etat vide d'une etape."""
    return {
        "stage": stage,
        "version": STATE_VERSION,
        "done": [],
        "runs": [],
        "updated_at": _now(),
    }


def normalize_state(stage: str, raw: Any) -> Dict[str, Any]:
    """
    Rend exploitable un etat lu depuis le stockage, quel que soit son etat.

    Tolere : None, un dict partiel, un fichier corrompu, une version inconnue.
    Ne perd jamais les cles ``done`` deja presentes (c'est ce qui evite de
    retraiter du travail deja fait).
    """
    state = new_state(stage)
    if not isinstance(raw, dict):
        return state
    done = raw.get("done")
    if isinstance(done, list):
        state["done"] = sorted({str(k) for k in done if str(k).strip()})
    runs = raw.get("runs")
    if isinstance(runs, list):
        state["runs"] = [r for r in runs if isinstance(r, dict)][-MAX_RUNS:]
    if raw.get("updated_at"):
        state["updated_at"] = str(raw["updated_at"])
    return state


def is_done_in(state: Dict[str, Any], key: str) -> bool:
    """L'unite de travail ``key`` est-elle deja terminee ?"""
    return str(key) in set(state.get("done", []))


def mark_done_in(state: Dict[str, Any], key: str) -> Dict[str, Any]:
    """Retourne un NOUVEL etat ou ``key`` est marquee terminee (idempotent)."""
    done = set(state.get("done", []))
    done.add(str(key))
    updated = dict(state)
    updated["done"] = sorted(done)
    updated["updated_at"] = _now()
    return updated


def mark_many_in(state: Dict[str, Any], keys: List[str]) -> Dict[str, Any]:
    """
    Marque PLUSIEURS unites d'un coup (fonction pure).

    Indispensable des que les unites se comptent en centaines : marquer une a
    une relit et rereecrit tout l'etat a chaque fois : un cout quadratique en
    octets transferes, pour des milliers d'aller-retours HTTP.
    """
    done = set(state.get("done", []))
    done.update(str(key) for key in keys if str(key).strip())
    updated = dict(state)
    updated["done"] = sorted(done)
    updated["updated_at"] = _now()
    return updated


def forget_in(state: Dict[str, Any], key: str) -> Dict[str, Any]:
    """Retourne un NOUVEL etat ou ``key`` n'est plus marquee (pour rejouer)."""
    done = set(state.get("done", []))
    done.discard(str(key))
    updated = dict(state)
    updated["done"] = sorted(done)
    updated["updated_at"] = _now()
    return updated


def pending(state: Dict[str, Any], keys: List[str]) -> List[str]:
    """Parmi ``keys``, celles qui restent a traiter, dans l'ordre fourni."""
    done = set(state.get("done", []))
    seen, result = set(), []
    for key in keys:
        key = str(key)
        if key in done or key in seen:
            continue
        seen.add(key)
        result.append(key)
    return result


def append_run(state: Dict[str, Any], run: Dict[str, Any]) -> Dict[str, Any]:
    """Ajoute un run au journal, borne a :data:`MAX_RUNS` entrees."""
    runs = list(state.get("runs", []))
    runs.append(run)
    updated = dict(state)
    updated["runs"] = runs[-MAX_RUNS:]
    updated["updated_at"] = _now()
    return updated


def summarize_state(state: Dict[str, Any]) -> Dict[str, Any]:
    """Resume lisible d'une etape : combien d'unites, dernier run."""
    runs = state.get("runs", [])
    last = runs[-1] if runs else None
    return {
        "stage": state.get("stage", "?"),
        "done": len(state.get("done", [])),
        "updated_at": state.get("updated_at", "-"),
        "last_run": (last or {}).get("run_id", "-"),
        "last_status": (last or {}).get("status", "-"),
        "runs": len(runs),
    }


def render_summary(summaries: List[Dict[str, Any]]) -> str:
    """Tableau texte aligne des etapes."""
    lines = [
        f"{'ETAPE':<20} {'UNITES':>7} {'DERNIER RUN':<22} {'STATUT':<10} MAJ",
        "-" * 88,
    ]
    for item in summaries:
        lines.append(
            f"{item['stage']:<20} {item['done']:>7} {str(item['last_run']):<22} "
            f"{str(item['last_status']):<10} {item['updated_at']}"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Stockage : HDFS (defaut) ou fichier local (tests / hors cluster)
# ---------------------------------------------------------------------------

def backend() -> str:
    """Backend actif : ``hdfs`` (defaut) ou ``file``."""
    return os.environ.get("METEO_CHECKPOINT_BACKEND", "hdfs").strip().lower()


def local_root() -> Path:
    """Racine du backend fichier."""
    return Path(os.environ.get("METEO_CHECKPOINT_DIR", "/tmp/meteo-checkpoints"))


def state_path(stage: str) -> str:
    """Chemin HDFS de l'etat d'une etape."""
    return f"{CHECKPOINT_ROOT}/{stage}.json"


def _read_raw(stage: str) -> Any:
    """Lit l'etat brut ; retourne None si absent ou illisible."""
    if backend() == "file":
        path = local_root() / f"{stage}.json"
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            logger.warning("Checkpoint %s illisible (%s) : on repart a vide.", path, exc)
            return None

    import hdfs_utils
    try:
        return hdfs_utils.hdfs_read_json(state_path(stage))
    except (IOError, ValueError) as exc:
        logger.debug("Checkpoint %s absent (%s).", state_path(stage), exc)
        return None


def _write_raw(stage: str, state: Dict[str, Any]) -> bool:
    """Ecrit l'etat. Retourne False en cas d'echec (jamais d'exception)."""
    try:
        if backend() == "file":
            root = local_root()
            root.mkdir(parents=True, exist_ok=True)
            (root / f"{stage}.json").write_text(
                json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
            return True
        import hdfs_utils
        hdfs_utils.hdfs_write_json(state_path(stage), state)
        return True
    except (IOError, OSError, ValueError) as exc:
        # Un checkpoint est une optimisation de reprise : son echec ne doit
        # jamais faire echouer le traitement lui-meme.
        logger.warning("Ecriture du checkpoint %s impossible : %s", stage, exc)
        return False


def _import_legacy_bronze(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Reprend l'ancien ``_ingested.json`` s'il existe et que l'etat est vide.

    Evite de retelecharger des lots deja ingeres par une version anterieure.
    """
    if state.get("done") or backend() == "file":
        return state
    import hdfs_utils
    try:
        legacy = hdfs_utils.hdfs_read_json(LEGACY_BRONZE_PATH)
    except (IOError, ValueError):
        return state
    batches = legacy.get("batches") if isinstance(legacy, dict) else None
    if not isinstance(batches, list) or not batches:
        return state
    logger.info("Reprise de %d lot(s) depuis l'ancien %s.", len(batches), LEGACY_BRONZE_PATH)
    updated = dict(state)
    updated["done"] = sorted({str(b) for b in batches})
    return updated


def load(stage: str) -> Dict[str, Any]:
    """Charge l'etat d'une etape (toujours exploitable, jamais d'exception)."""
    state = normalize_state(stage, _read_raw(stage))
    if stage == STAGE_BRONZE:
        state = _import_legacy_bronze(state)
    return state


def save(stage: str, state: Dict[str, Any]) -> bool:
    """Persiste l'etat d'une etape."""
    return _write_raw(stage, state)


# ---------------------------------------------------------------------------
# API de haut niveau
# ---------------------------------------------------------------------------

def is_done(stage: str, key: str) -> bool:
    """L'unite ``key`` de ``stage`` est-elle deja terminee ?"""
    return is_done_in(load(stage), key)


def mark_done(stage: str, key: str) -> bool:
    """
    Marque une unite terminee et **committe immediatement**.

    C'est le coeur de la reprise fine : une interruption ne coute jamais plus
    que l'unite en cours, jamais un lot de N unites.
    """
    return save(stage, mark_done_in(load(stage), key))


def mark_many(stage: str, keys: List[str]) -> bool:
    """
    Marque un lot d'unites en UNE lecture et UNE ecriture.

    A utiliser pour Silver et Gold, dont les unites (partitions dt) se comptent
    en milliers. Bronze conserve ``mark_done`` unite par unite : ses lots sont
    peu nombreux et longs, le commit immediat y est le bon compromis.
    """
    if not keys:
        return True
    return save(stage, mark_many_in(load(stage), keys))


def forget(stage: str, key: str) -> bool:
    """Oublie une unite pour la rejouer au prochain passage."""
    return save(stage, forget_in(load(stage), key))


def reset(stage: str) -> bool:
    """Vide l'etat d'une etape (tout sera rejoue)."""
    return save(stage, new_state(stage))


def pending_keys(stage: str, keys: List[str]) -> List[str]:
    """Parmi ``keys``, celles qui restent a traiter."""
    return pending(load(stage), keys)


def record_run(stage: str, run_id: str, status: str, **counters: Any) -> bool:
    """Ajoute une entree au journal des runs d'une etape."""
    run = {"run_id": run_id, "status": status, "at": _now()}
    run.update({k: v for k, v in counters.items() if v is not None})
    return save(stage, append_run(load(stage), run))


def summary(stages: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Resume de chaque etape connue."""
    return [summarize_state(load(stage)) for stage in (stages or KNOWN_STAGES)]


def run_id(prefix: str = "run") -> str:
    """Identifiant de run lisible, base sur l'horodatage UTC."""
    return f"{prefix}-{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
