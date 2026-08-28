# -*- coding: utf-8 -*-
"""
pipeline_ctl.py : pilote multiplateforme du workflow DataLake Météo.
=====================================================================
Toute la logique d'orchestration de ``make all`` vit ici, en **Python
standard uniquement** (aucune dépendance externe), afin que le Makefile
reste une simple liste de commandes d'une ligne exécutables **à
l'identique sous Windows (cmd.exe / PowerShell), macOS et Linux**.

Sans ce module, le Makefile devrait embarquer des boucles ``for``/``while``
bash, des ``echo -e`` et des ``2>/dev/null`` : autant de constructions que
``cmd.exe`` ne comprend pas.

Commandes :
    doctor    vérifie Python, Docker et le plugin docker compose
    up        démarre le cluster puis attend HDFS et Airflow
    init      crée les répertoires HDFS, le topic Kafka, vérifie Airflow
    unpause   active les DAGs
    trigger   déclenche un DAG
    pipeline  déclenche Bronze puis attend Bronze -> Silver -> Gold
    wait      attend la fin de la chaîne (sans déclencher)
    verify    contrôle les trois couches sur HDFS
    urls      rappelle les interfaces
    clean     supprime les caches Python locaux
    deps      installe pytest/pandas (utile hors conteneur)

Usage :
    python scripts/pipeline_ctl.py <commande> [options]
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set

# Sous Windows la console est en cp1252/cp850 : on force l'UTF-8 pour que les
# accents n'interrompent pas l'affichage.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    except Exception:  # pragma: no cover
        pass

ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = ROOT / "docker" / ".env"
COMPOSE_FILE = ROOT / "docker" / "docker-compose.yml"

#: Vrai lorsque ce script s'execute DANS un conteneur du cluster : la CLI
#: ``airflow`` n'existe que la, et les services se joignent par leur nom de
#: service Docker plutot que par localhost.
IN_CONTAINER = shutil.which("airflow") is not None or bool(os.environ.get("HDFS_NAMENODE"))


def namenode_url() -> str:
    """
    URL WebHDFS, valide des deux cotes de la frontiere Docker.

    Dans un conteneur, ``HDFS_NAMENODE`` vaut ``namenode`` (reseau Docker) ;
    depuis l'hote, le Namenode n'est joignable que sur ``localhost:9870``.
    """
    explicit = os.environ.get("NAMENODE_URL")
    if explicit:
        return explicit.rstrip("/")
    host = os.environ.get("HDFS_NAMENODE", "localhost")
    port = os.environ.get("HDFS_WEBHDFS_PORT", "9870")
    return f"http://{host}:{port}"


HDFS_USER = os.environ.get("HDFS_USER", "root")

#: URL de sante du webserver Airflow, vue DEPUIS LE RESEAU DOCKER : le port
#: interne reste 8080 quel que soit le port publie sur l'hote.
AIRFLOW_HEALTH_URL = os.environ.get(
    "AIRFLOW_HEALTH_URL", "http://airflow-webserver:8080/health")

#: Port HOTE de l'UI Airflow (8080 est volontairement laisse libre).
AIRFLOW_HOST_PORT = os.environ.get("AIRFLOW_WEB_PORT", "8082")

#: Répertoires racines créés à l'initialisation du datalake.
HDFS_DIRS: List[str] = [
    "/bronze", "/silver", "/gold", "/models", "/checkpoints",
    "/bronze/meteo/batch/source=meteofrance",
    "/bronze/meteo/stream/source=openmeteo",
]

#: Les cinq DAGs du projet (streaming découplé du batch).
ALL_DAGS: List[str] = [
    "dag_bronze_ingest", "dag_stream_ingest", "dag_silver_transform",
    "dag_gold_aggregate", "dag_ml_retrain",
]

#: La chaîne Medallion, dans l'ordre de dépendance.
PIPELINE_DAGS: List[str] = [
    "dag_bronze_ingest", "dag_silver_transform", "dag_gold_aggregate",
]

KAFKA_TOPIC = os.environ.get("METEO_TOPIC", "meteo-stream")


def say(message: str) -> None:
    print(f"[make] {message}", flush=True)


def fail(message: str) -> int:
    print(f"[make] ERREUR : {message}", file=sys.stderr, flush=True)
    return 1


# ---------------------------------------------------------------------------
# Docker
# ---------------------------------------------------------------------------

def compose_command(profile: str = "") -> List[str]:
    """Préfixe ``docker compose`` complet (fichier d'env + compose + profil)."""
    command = ["docker", "compose", "--env-file", str(ENV_FILE), "-f", str(COMPOSE_FILE)]
    if profile:
        command += ["--profile", profile]
    return command


def run(command: Sequence[str], check: bool = True, capture: bool = False,
        timeout: Optional[int] = None) -> subprocess.CompletedProcess:
    """Exécute une commande (liste d'arguments : aucun shell, donc portable)."""
    return subprocess.run(
        list(command), check=check, timeout=timeout,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
        text=True, encoding="utf-8", errors="replace",
    )


def quiet(command: Sequence[str], timeout: Optional[int] = None) -> bool:
    """True si la commande réussit ; n'affiche rien et ne lève jamais."""
    try:
        return run(command, check=False, capture=True, timeout=timeout).returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


def airflow(args: Sequence[str], capture: bool = False,
            timeout: Optional[int] = None) -> subprocess.CompletedProcess:
    """
    Exécute une commande ``airflow``.

    Dans le conteneur Airflow, la CLI est appelée directement (aucun client
    Docker n'y est installé) ; depuis l'hôte, elle est encapsulée dans
    ``docker compose exec``. Les appelants n'ont pas à connaître le contexte.
    """
    if IN_CONTAINER:
        command = ["airflow"] + list(args)
    else:
        command = compose_command() + ["exec", "-T", "airflow-webserver", "airflow"] + list(args)
    try:
        return run(command, check=False, capture=capture, timeout=timeout)
    except (OSError, subprocess.SubprocessError):
        return subprocess.CompletedProcess(command, 1, "", "")


# ---------------------------------------------------------------------------
# WebHDFS (urllib : aucune dépendance externe requise côté hôte)
# ---------------------------------------------------------------------------

def webhdfs(path: str, op: str, method: str = "GET", timeout: int = 10) -> bool:
    """Appelle WebHDFS ; True si la requête aboutit."""
    url = f"{namenode_url()}/webhdfs/v1{path}?op={op}&user.name={HDFS_USER}"
    request = urllib.request.Request(url, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return 200 <= response.status < 400
    except (urllib.error.URLError, OSError, ValueError):
        return False


def wait_for(label: str, probe, attempts: int = 60, delay: int = 5) -> bool:
    """Sonde ``probe`` jusqu'à succès."""
    say(f"Attente de {label} ...")
    for attempt in range(1, attempts + 1):
        if probe():
            say(f"  {label} : pret.")
            return True
        time.sleep(delay)
    print(f"[make] {label} : toujours indisponible apres {attempts * delay}s.",
          file=sys.stderr)
    return False


# ---------------------------------------------------------------------------
# Commandes
# ---------------------------------------------------------------------------

def _checkpoint():
    """Module de checkpoints (importable seulement dans un conteneur)."""
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import checkpoint

    return checkpoint


def step_done(step: str) -> bool:
    """
    L'etape ``step`` de `make all` est-elle deja terminee ?

    Hors conteneur, HDFS n'est pas joignable : on repond False (l'etape est
    rejouee, ce qui est sans danger car chaque etape est idempotente).
    """
    if not IN_CONTAINER:
        return False
    try:
        return _checkpoint().is_done("workflow", step)
    except Exception:  # noqa: BLE001 - un checkpoint illisible ne bloque rien
        return False


def complete_step(step: str) -> None:
    """Marque une etape de `make all` comme terminee."""
    if not IN_CONTAINER:
        return
    try:
        _checkpoint().mark_done("workflow", step)
    except Exception:  # noqa: BLE001
        logger_warn = f"checkpoint de l'etape {step} non enregistre"
        print(f"[make] {logger_warn}", file=sys.stderr)


def skip_if_done(step: str, force: bool) -> bool:
    """True si l'etape peut etre sautee ; affiche pourquoi."""
    if force:
        return False
    if step_done(step):
        say(f"Etape '{step}' deja terminee - on continue (--force pour la rejouer).")
        return True
    return False


def cmd_doctor(args: argparse.Namespace) -> int:
    """Vérifie que l'environnement peut exécuter le workflow."""
    say(f"Python : {sys.version.split()[0]} ({sys.executable})")
    if args.python_only:
        return 0
    if shutil.which("docker") is None:
        return fail("Docker est introuvable dans le PATH. Installez Docker Desktop, "
                    "ouvrez-le, puis relancez 'make all'.")
    if not quiet(["docker", "compose", "version"], timeout=60):
        return fail("'docker compose' (plugin v2) est indisponible. "
                    "Mettez Docker Desktop a jour.")
    if not quiet(["docker", "info"], timeout=60):
        return fail("Le demon Docker ne repond pas. Demarrez Docker Desktop, "
                    "attendez qu'il soit pret, puis relancez.")
    say("Docker est operationnel.")
    return 0


def cmd_up(args: argparse.Namespace) -> int:
    """Démarre le cluster puis attend que HDFS et Airflow répondent."""
    say("Demarrage du cluster (build inclus) - la premiere fois, comptez 5 a 15 min...")
    if run(compose_command(args.profile) + ["up", "-d", "--build"], check=False).returncode != 0:
        return fail("'docker compose up' a echoue (voir la sortie ci-dessus).")
    if not wait_for("le Namenode HDFS", lambda: webhdfs("/", "GETFILESTATUS"),
                    attempts=args.attempts):
        return fail("HDFS n'a pas demarre. Diagnostic : make logs SVC=namenode")
    if not wait_for("le webserver Airflow",
                    lambda: airflow(["version"], capture=True, timeout=60).returncode == 0,
                    attempts=args.attempts):
        return fail("Airflow n'a pas demarre. Diagnostic : make logs SVC=airflow-webserver")
    return 0


def http_ok(url: str, timeout: int = 5) -> bool:
    """True si l'URL répond avec un code < 400."""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return 200 <= response.status < 400
    except (urllib.error.URLError, OSError, ValueError):
        return False


def cmd_wait_services(args: argparse.Namespace) -> int:
    """
    Attend que HDFS et Airflow répondent.

    Exécuté DANS un conteneur du réseau Docker : c'est ce qui permet à l'hôte
    de n'avoir besoin d'aucun interpréteur Python.
    """
    if not wait_for("le Namenode HDFS", lambda: webhdfs("/", "GETFILESTATUS"),
                    attempts=args.attempts):
        return fail("HDFS n'a pas demarre. Diagnostic : make logs SVC=namenode")
    if not wait_for("le webserver Airflow", lambda: http_ok(AIRFLOW_HEALTH_URL),
                    attempts=args.attempts):
        return fail("Airflow n'a pas demarre. Diagnostic : make logs SVC=airflow-webserver")
    return 0


def cmd_init(args: argparse.Namespace) -> int:
    """Crée les répertoires racines du datalake sur HDFS (idempotent)."""
    if skip_if_done("init", args.force):
        return 0
    say("Creation des repertoires HDFS...")
    failures = []
    for directory in HDFS_DIRS:
        if webhdfs(directory, "MKDIRS", method="PUT"):
            print(f"  OK   {directory}")
        else:
            print(f"  KO   {directory}")
            failures.append(directory)
    if failures:
        return fail(f"{len(failures)} repertoire(s) HDFS non crees : {', '.join(failures)}. "
                    f"Namenode interroge : {namenode_url()}")
    complete_step("init")
    return 0


def cmd_unpause(args: argparse.Namespace) -> int:
    """Active les DAGs (le scheduler peut mettre un moment à les parser)."""
    if skip_if_done("unpause", args.force):
        return 0
    say("Activation des DAGs...")
    pending = list(ALL_DAGS)
    for attempt in range(1, args.attempts + 1):
        still_pending = []
        for dag in pending:
            if airflow(["dags", "unpause", dag], capture=True, timeout=120).returncode == 0:
                print(f"  OK   {dag}")
            else:
                still_pending.append(dag)
        pending = still_pending
        if not pending:
            complete_step("unpause")
            return 0
        if attempt < args.attempts:
            say(f"  {len(pending)} DAG(s) pas encore charge(s) par le scheduler, "
                f"nouvel essai dans {args.delay}s...")
            time.sleep(args.delay)
    return fail("DAG(s) introuvable(s) apres attente : " + ", ".join(pending)
                + ". Diagnostic : make logs SVC=airflow-scheduler")


def list_runs(dag: str) -> List[Dict[str, Any]]:
    """Exécutions connues d'un DAG (via ``airflow dags list-runs -o json``)."""
    result = airflow(["dags", "list-runs", "-d", dag, "-o", "json"],
                     capture=True, timeout=120)
    text = (result.stdout or "").strip()
    if result.returncode != 0 or not text:
        return []
    start = text.find("[")
    if start == -1:
        return []
    try:
        data = json.loads(text[start:])
    except (ValueError, TypeError):
        return []
    return [row for row in data if isinstance(row, dict)]


def run_ids(dag: str) -> Set[str]:
    """Photo des exécutions existantes, avant déclenchement."""
    return {str(row.get("run_id", "")) for row in list_runs(dag)}


def wait_for_dags(dags: List[str], baselines: Dict[str, Set[str]],
                  timeout: int, poll: int) -> int:
    """
    Attend qu'une **nouvelle** exécution de chaque DAG de la chaîne réussisse.

    Comparer aux exécutions présentes avant le déclenchement évite qu'un succès
    d'une session précédente ne soit pris pour le succès du jour.
    """
    say(f"Attente de {', '.join(dags)} (timeout {timeout}s)...")
    deadline = time.time() + timeout
    for dag in dags:
        print(f"  -> {dag}")
        known = baselines.get(dag, set())
        while True:
            fresh = [row for row in list_runs(dag)
                     if str(row.get("run_id", "")) not in known]
            states = {str(row.get("state", "")).lower() for row in fresh}
            if "success" in states:
                print("     succes")
                break
            if fresh and states and states <= {"failed"}:
                return fail(f"{dag} a echoue. Diagnostic : make logs SVC=airflow-scheduler")
            if time.time() >= deadline:
                current = ", ".join(sorted(s for s in states if s)) or "aucune execution"
                return fail(f"{dag} : timeout apres {timeout}s (etat : {current}). "
                            f"Diagnostic : make logs SVC=airflow-scheduler")
            time.sleep(poll)
    return 0


def cmd_trigger(args: argparse.Namespace) -> int:
    say(f"Declenchement de {args.dag}...")
    if airflow(["dags", "trigger", args.dag], timeout=180).returncode != 0:
        return fail(f"Impossible de declencher {args.dag}.")
    return 0


def trigger_and_wait(dags: List[str], first: str, timeout: int, poll: int) -> int:
    """Photographie les runs, declenche ``first``, attend un NOUVEAU succes."""
    baselines = {dag: run_ids(dag) for dag in dags}
    say(f"Declenchement de {first}...")
    if airflow(["dags", "trigger", first], timeout=180).returncode != 0:
        return fail(f"Impossible de declencher {first}.")
    return wait_for_dags(dags, baselines, timeout, poll)


def cmd_pipeline(args: argparse.Namespace) -> int:
    """Bronze -> Silver -> Gold, puis attente du succes de la chaine."""
    if skip_if_done("pipeline", args.force):
        return 0
    code = trigger_and_wait(PIPELINE_DAGS, "dag_bronze_ingest", args.timeout, args.poll)
    if code == 0:
        complete_step("pipeline")
    return code


def cmd_wait(args: argparse.Namespace) -> int:
    """Attend la chaîne sans rien déclencher."""
    return wait_for_dags(PIPELINE_DAGS, {dag: set() for dag in PIPELINE_DAGS},
                         args.timeout, args.poll)


def cmd_verify(args: argparse.Namespace) -> int:
    """
    Contrôle les trois couches Medallion sur HDFS.

    Dans un conteneur, ``verify_medallion`` est importé et exécuté directement ;
    depuis l'hôte, on délègue au conteneur spark-master.
    """
    say("Verification de la couche Medallion sur HDFS...")
    if IN_CONTAINER:
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        import verify_medallion

        argv = []
        if args.allow_empty_stream:
            argv.append("--allow-empty-stream")
        if args.with_ml:
            argv.append("--with-ml")
        return verify_medallion.main(argv)

    command = compose_command() + [
        "exec", "-T", "spark-master",
        "python", "/opt/project/scripts/verify_medallion.py",
    ]
    if args.allow_empty_stream:
        command.append("--allow-empty-stream")
    if args.with_ml:
        command.append("--with-ml")
    return run(command, check=False, timeout=300).returncode


def cmd_checkpoints(args: argparse.Namespace) -> int:
    """
    Affiche l'etat de reprise de chaque etape Bronze -> Silver -> Gold.

    Dans un conteneur : lecture directe des checkpoints HDFS. Depuis l'hote :
    on delegue au conteneur spark-master, qui lui a acces a HDFS.
    """
    if not IN_CONTAINER:
        command = compose_command() + [
            "exec", "-T", "spark-master",
            "python3", "/opt/project/scripts/pipeline_ctl.py", "checkpoints",
        ]
        if args.reset:
            command.append("--reset")
        return run(command, check=False, timeout=180).returncode

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import checkpoint

    if args.reset:
        for stage in checkpoint.KNOWN_STAGES:
            checkpoint.reset(stage)
        say("Checkpoints remis a zero : tout sera rejoue au prochain run.")
        return 0

    print(checkpoint.render_summary(checkpoint.summary()))
    return 0


def cmd_urls(args: argparse.Namespace) -> int:
    print("")
    print("  Interfaces du datalake")
    print(f"  Airflow     http://localhost:{AIRFLOW_HOST_PORT}     (admin / admin)")
    print("  Spark       http://localhost:8081")
    print("  HDFS        http://localhost:9870")
    print("  Jupyter     http://localhost:8888     (token : meteo)")
    print("  Dashboard   http://localhost:8501")
    print("  Kafka       localhost:9092            (topic : meteo-stream)")
    print("")
    return 0


def cmd_clean(args: argparse.Namespace) -> int:
    """Supprime les caches Python locaux (portable, sans 'find')."""
    removed = 0
    for name in ("__pycache__", ".pytest_cache"):
        for path in ROOT.rglob(name):
            if ".git" in path.parts or not path.is_dir():
                continue
            shutil.rmtree(path, ignore_errors=True)
            removed += 1
    say(f"Caches Python nettoyes ({removed} repertoire(s)).")
    return 0


#: Paquets requis par la suite de tests (installés à la demande).
TEST_REQUIREMENTS: List[str] = ["pytest", "pandas"]


def missing_requirements(packages: Sequence[str]) -> List[str]:
    """Retourne les paquets non importables parmi ``packages``."""
    import importlib.util

    return [name for name in packages if importlib.util.find_spec(name) is None]


def cmd_deps(args: argparse.Namespace) -> int:
    """
    Installe les dépendances de test manquantes, dans l'interpréteur courant.

    Utilise ``sys.executable -m pip`` : sous Windows, cela garantit que pytest
    est installé pour le Python que le Makefile utilisera réellement.
    """
    missing = missing_requirements(TEST_REQUIREMENTS)
    if not missing:
        return 0
    say(f"Installation des dependances de test manquantes : {', '.join(missing)}")
    command = [sys.executable, "-m", "pip", "install", "--disable-pip-version-check"] + missing
    if run(command, check=False).returncode != 0:
        return fail("Installation impossible. Installez-les manuellement : "
                    f"{sys.executable} -m pip install " + " ".join(missing))
    still_missing = missing_requirements(TEST_REQUIREMENTS)
    if still_missing:
        return fail("Toujours introuvable(s) apres installation : " + ", ".join(still_missing))
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Pilote du workflow DataLake Meteo")
    sub = parser.add_subparsers(dest="command", required=True)

    doctor = sub.add_parser("doctor", help="Verifie Python, Docker, docker compose")
    doctor.add_argument("--python-only", action="store_true")
    doctor.set_defaults(func=cmd_doctor)

    up = sub.add_parser("up", help="Demarre le cluster et attend HDFS + Airflow")
    up.add_argument("--profile", default=os.environ.get("PROFILE", ""))
    up.add_argument("--attempts", type=int, default=90)
    up.set_defaults(func=cmd_up)

    init = sub.add_parser("init", help="Cree les repertoires HDFS")
    init.add_argument("--force", action="store_true", help="Rejouer meme si deja fait.")
    init.set_defaults(func=cmd_init)

    services = sub.add_parser("wait-services", help="Attend HDFS et Airflow")
    services.add_argument("--attempts", type=int, default=90)
    services.set_defaults(func=cmd_wait_services)

    unpause = sub.add_parser("unpause", help="Active les DAGs")
    unpause.add_argument("--attempts", type=int, default=12)
    unpause.add_argument("--delay", type=int, default=10)
    unpause.add_argument("--force", action="store_true", help="Rejouer meme si deja fait.")
    unpause.set_defaults(func=cmd_unpause)

    trigger = sub.add_parser("trigger", help="Declenche un DAG")
    trigger.add_argument("dag")
    trigger.set_defaults(func=cmd_trigger)

    step = sub.add_parser("pipeline", help="Bronze -> Silver -> Gold, puis attente")
    step.add_argument("--timeout", type=int,
                      default=int(os.environ.get("PIPELINE_TIMEOUT", "2400")))
    step.add_argument("--poll", type=int, default=10)
    step.add_argument("--force", action="store_true", help="Rejouer meme si deja fait.")
    step.set_defaults(func=cmd_pipeline)

    wait = sub.add_parser("wait", help="Attend la chaine sans declencher")
    wait.add_argument("--timeout", type=int,
                      default=int(os.environ.get("PIPELINE_TIMEOUT", "2400")))
    wait.add_argument("--poll", type=int, default=10)
    wait.set_defaults(func=cmd_wait)

    verify = sub.add_parser("verify", help="Controle les trois couches sur HDFS")
    verify.add_argument("--allow-empty-stream", action="store_true")
    verify.add_argument("--with-ml", action="store_true",
                        help="Exiger aussi ml_predictions et ai_insights.")
    verify.set_defaults(func=cmd_verify)

    checkpoints = sub.add_parser("checkpoints", help="Etat de reprise des trois etapes")
    checkpoints.add_argument("--reset", action="store_true",
                             help="Vide les checkpoints (tout sera rejoue).")
    checkpoints.set_defaults(func=cmd_checkpoints)

    sub.add_parser("deps", help="Installe pytest/pandas si absents").set_defaults(func=cmd_deps)
    sub.add_parser("urls", help="Rappelle les interfaces").set_defaults(func=cmd_urls)
    sub.add_parser("clean", help="Nettoie les caches Python").set_defaults(func=cmd_clean)
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.func(args))
    except KeyboardInterrupt:
        return fail("Interrompu par l'utilisateur.")
    except subprocess.TimeoutExpired as exc:
        return fail(f"Commande expiree : {exc}")


if __name__ == "__main__":
    sys.exit(main())
