#!/usr/bin/env bash
# ============================================================================
#  deploy.sh : script de déploiement du cluster DataLake Météo
#  ----------------------------------------------------------------------------
#  Usage :
#    ./deploy.sh up          Démarre tout le cluster (build des images inclus)
#    ./deploy.sh up genai    Démarre aussi Ollama (bulletins IA génératifs)
#    ./deploy.sh status      État des conteneurs
#    ./deploy.sh logs <svc>  Logs d'un service (ex: ./deploy.sh logs kafka-producer)
#    ./deploy.sh stop        Arrête les conteneurs (sans supprimer les volumes)
#    ./deploy.sh down        Arrête et supprime conteneurs + réseaux
#    ./deploy.sh reset       down + suppression des volumes (données HDFS/Kafka/Postgres)
#    ./deploy.sh init        (ré)initialise HDFS, le topic Kafka et Airflow
#    ./deploy.sh trigger     Déclenche manuellement le DAG d'ingestion Bronze
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE="docker compose --env-file ${SCRIPT_DIR}/docker/.env -f ${SCRIPT_DIR}/docker/docker-compose.yml"
NAMENODE_URL="http://namenode:9870"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[deploy]${NC} $*"; }
warn()  { echo -e "${YELLOW}[deploy]${NC} $*"; }
error() { echo -e "${RED}[deploy]${NC} $*"; }

# ---------------------------------------------------------------------------
# Attente d'un service HTTP (curl retry)
# ---------------------------------------------------------------------------
wait_http() {
  local url="$1" name="$2" attempts="${3:-60}"
  info "Attente de ${name} (${url}) ..."
  for i in $(seq 1 "${attempts}"); do
    if curl -sf --max-time 5 "${url}" >/dev/null 2>&1; then
      info "${name} est prêt."
      return 0
    fi
    sleep 5
  done
  error "${name} n'a pas démarré après ${attempts} tentatives."
  return 1
}

# ---------------------------------------------------------------------------
# Création des répertoires racines HDFS + topic Kafka (idempotent)
# ---------------------------------------------------------------------------
hdfs_init() {
  info "Création des répertoires HDFS (Bronze / Silver / Gold / modèles / checkpoints)..."
  for dir in /bronze /silver /gold /models /checkpoints \
             /bronze/meteo/batch/source=noaa \
             /bronze/meteo/stream/source=openmeteo; do
    curl -sf -X PUT "${NAMENODE_URL}/webhdfs/v1${dir}?op=MKDIRS&user.name=root" >/dev/null \
      && info "  OK  ${dir}" || warn "  Échec mkdir ${dir}"
  done
}

kafka_init() {
  info "Création du topic Kafka '${METEO_TOPIC:-meteo-stream}' (si absent)..."
  ${COMPOSE} exec -T kafka kafka-topics --bootstrap-server kafka:9092 \
    --create --if-not-exists \
    --topic "${METEO_TOPIC:-meteo-stream}" --partitions 3 --replication-factor 1 \
    >/dev/null && info "  Topic OK"
}

airflow_init() {
  info "Attente de l'initialisation Airflow (migration DB + user admin)..."
  ${COMPOSE} ps airflow-init >/dev/null 2>&1 || true
  # airflow-init s'exécute une fois ; on attend la fin de son conteneur
  local status
  for i in $(seq 1 60); do
    status="$(${COMPOSE} ps -a --format '{{.State}}' airflow-init 2>/dev/null || true)"
    if [ "${status}" = "exited" ]; then
      info "Airflow initialisé (admin / admin)."
      return 0
    fi
    sleep 5
  done
  warn "Le service airflow-init n'est pas terminé ; vérifiez ses logs : ./deploy.sh logs airflow-init"
  return 0
}

# ---------------------------------------------------------------------------
# Commandes principales
# ---------------------------------------------------------------------------
case "${1:-up}" in
  up|start)
    info "Démarrage du cluster DataLake Météo..."
    EXTRA_PROFILE=""
    if [ "${2:-}" = "genai" ]; then
      EXTRA_PROFILE="--profile genai"
      warn "Profil GenAI activé : Ollama sera démarré (pensez à : docker exec ollama ollama pull llama3.2:3b)"
    fi
    ${COMPOSE} ${EXTRA_PROFILE} up -d --build
    wait_http "http://localhost:9870/webhdfs/v1/?op=GETFILESTATUS&user.name=root" "Namenode (HDFS)"
    hdfs_init
    kafka_init
    airflow_init
    info "======================================================"
    info " Cluster prêt :"
    info "  - HDFS UI     : http://localhost:9870"
    info "  - Spark UI    : http://localhost:8080"
    info "  - Airflow     : http://localhost:8080/  (admin / admin)"
    info "  - Jupyter     : http://localhost:8888/  (token: meteo)"
    info "  - Dashboard   : http://localhost:8501"
    info "  - Kafka       : localhost:9092 (topic: meteo-stream)"
    info "  - Ollama (IA) : http://localhost:11434 (si profil genai)"
    info "======================================================"
    info "Prochaine étape : ./deploy.sh trigger  (ou via l'UI Airflow)"
    ;;

  status)
    ${COMPOSE} ps
    ;;

  logs)
    ${COMPOSE} logs -f --tail=100 "${2:-airflow-scheduler}"
    ;;

  stop)
    info "Arrêt des conteneurs (les volumes sont conservés)..."
    ${COMPOSE} stop
    ;;

  down)
    info "Arrêt et suppression des conteneurs + réseaux (volumes conservés)..."
    ${COMPOSE} down
    ;;

  reset)
    warn "Suppression de TOUT (conteneurs, réseaux, volumes : HDFS, Kafka, Postgres...)"
    read -r -p "Confirmer ? [y/N] " answer
    if [ "${answer}" = "y" ] || [ "${answer}" = "Y" ]; then
      ${COMPOSE} down -v
      info "Cluster réinitialisé. Relancez : ./deploy.sh up"
    else
      info "Annulé."
    fi
    ;;

  init)
    hdfs_init
    kafka_init
    airflow_init
    ;;

  trigger)
    info "Déclenchement du DAG dag_bronze_ingest..."
    ${COMPOSE} exec -T airflow-webserver airflow dags trigger dag_bronze_ingest
    info "DAG déclenché. Suivez-le sur http://localhost:8080"
    ;;

  *)
    error "Commande inconnue : $1"
    grep -E "^#    \./deploy.sh" "${BASH_SOURCE[0]}" | head -10
    exit 1
    ;;
esac
