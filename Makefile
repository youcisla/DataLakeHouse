# =============================================================================
#  DataLake Météo — TP Big Data (DataLake / DataLakehouse)
#  Automatisation complète du workflow Bronze -> Silver -> Gold.
#
#      make all      : de zéro au Gold vérifié, SANS aucune étape manuelle
#      make help     : liste toutes les cibles
#
#  Sources : archives Météo-France (meteo.data.gouv.fr) + Open-Meteo (temps réel).
# =============================================================================

SHELL := /bin/bash
.DEFAULT_GOAL := help
# Les étapes du workflow sont séquentielles par nature (le Gold dépend du
# Silver, qui dépend du Bronze) : on interdit l'exécution parallèle.
.NOTPARALLEL:

# --- Configuration (surchargeable : make all PROFILE=genai) -------------------
PY               ?= python3
ENV_FILE         := docker/.env
COMPOSE_FILE     := docker/docker-compose.yml
COMPOSE          := docker compose --env-file $(ENV_FILE) -f $(COMPOSE_FILE)
PROFILE          ?=
COMPOSE_PROFILE  := $(if $(PROFILE),--profile $(PROFILE),)

AIRFLOW          := $(COMPOSE) exec -T airflow-webserver airflow
SPARK_PY         := $(COMPOSE) exec -T spark-master python /opt/project/scripts

DAGS             := dag_bronze_ingest dag_silver_transform dag_gold_aggregate dag_ml_retrain
PIPELINE_DAGS    := dag_bronze_ingest dag_silver_transform dag_gold_aggregate
PIPELINE_TIMEOUT ?= 2400
POLL_SECONDS     ?= 10

# Départements Météo-France ingérés (villes du flux temps réel Open-Meteo).
MF_DEPARTMENTS   ?= 75 69 13 33 59

C_OK   := \033[0;32m
C_WARN := \033[1;33m
C_ERR  := \033[0;31m
C_OFF  := \033[0m
.PHONY: help all test lint check-docker build up init unpause trigger \
        wait-pipeline verify urls bronze silver gold ml genai dry-run \
        status logs ps stop down reset clean re

# -----------------------------------------------------------------------------
help:  ## Affiche cette aide
	@echo ""
	@echo -e "$(C_OK)DataLake Météo — cibles disponibles$(C_OFF)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | sort \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo -e "  Workflow complet : $(C_OK)make all$(C_OFF)   (options : PROFILE=genai, MF_DEPARTMENTS=\"75 69\")"
	@echo ""

# =============================================================================
#  LA CIBLE PRINCIPALE : tout le TP, de bout en bout, sans intervention
# =============================================================================
all: test check-docker up init unpause trigger wait-pipeline verify urls  ## Workflow complet : tests -> cluster -> Bronze -> Silver -> Gold -> vérification
	@echo -e "$(C_OK)[make]$(C_OFF) Workflow terminé : la couche Medallion est en place et vérifiée."

re: reset all  ## Repart de zéro (supprime les volumes) puis rejoue tout

# =============================================================================
#  1. Qualité : tests unitaires (sans Spark ni HDFS, exécutables hors Docker)
# =============================================================================
test:  ## Lance les tests unitaires (installe pytest/pandas si besoin)
	@echo -e "$(C_OK)[make]$(C_OFF) Tests unitaires (sans Spark)..."
	@$(PY) -c "import pytest, pandas" 2>/dev/null \
	  || $(PY) -m pip install --quiet --disable-pip-version-check pytest pandas
	@$(PY) -m pytest tests -q

lint:  ## Vérifie la syntaxe de tous les scripts Python
	@echo -e "$(C_OK)[make]$(C_OFF) Compilation des scripts, DAGs et tests..."
	@$(PY) -m compileall -q scripts airflow/dags tests ml dashboard
	@$(PY) -m pyflakes scripts/meteofrance_ingest.py scripts/verify_medallion.py tests/*.py \
	  2>/dev/null || echo "  (pyflakes absent : compilation seule)"

# =============================================================================
#  2. Cluster : build et démarrage
# =============================================================================
check-docker:  ## Vérifie que Docker et docker compose sont disponibles
	@command -v docker >/dev/null 2>&1 || { \
	  echo -e "$(C_ERR)[make] Docker est introuvable. Installez Docker Desktop puis relancez.$(C_OFF)"; exit 1; }
	@docker compose version >/dev/null 2>&1 || { \
	  echo -e "$(C_ERR)[make] 'docker compose' est indisponible (plugin v2 requis).$(C_OFF)"; exit 1; }
	@docker info >/dev/null 2>&1 || { \
	  echo -e "$(C_ERR)[make] Le démon Docker ne répond pas. Démarrez Docker puis relancez.$(C_OFF)"; exit 1; }
	@echo -e "$(C_OK)[make]$(C_OFF) Docker est opérationnel."

build: check-docker  ## Construit les images (spark, airflow, base, jupyter)
	@echo -e "$(C_OK)[make]$(C_OFF) Construction des images Docker..."
	@$(COMPOSE) $(COMPOSE_PROFILE) build

up: check-docker  ## Démarre le cluster complet (HDFS, Kafka, Spark, Airflow, Streamlit)
	@echo -e "$(C_OK)[make]$(C_OFF) Démarrage du cluster (build inclus)..."
	@$(COMPOSE) $(COMPOSE_PROFILE) up -d --build
	@echo -e "$(C_OK)[make]$(C_OFF) Attente du Namenode HDFS..."
	@for i in $$(seq 1 60); do \
	  curl -sf --max-time 5 "http://localhost:9870/webhdfs/v1/?op=GETFILESTATUS&user.name=root" >/dev/null 2>&1 && break; \
	  sleep 5; \
	done
	@echo -e "$(C_OK)[make]$(C_OFF) Attente du webserver Airflow..."
	@for i in $$(seq 1 90); do \
	  $(COMPOSE) exec -T airflow-webserver airflow version >/dev/null 2>&1 && break; \
	  sleep 5; \
	done

init:  ## Crée les répertoires HDFS, le topic Kafka et initialise Airflow
	@echo -e "$(C_OK)[make]$(C_OFF) Initialisation HDFS / Kafka / Airflow..."
	@bash ./deploy.sh init

unpause:  ## Dé-planifie (unpause) les quatre DAGs
	@echo -e "$(C_OK)[make]$(C_OFF) Activation des DAGs..."
	@for dag in $(DAGS); do \
	  $(AIRFLOW) dags unpause $$dag >/dev/null 2>&1 \
	    && echo "  OK  $$dag" \
	    || echo -e "  $(C_WARN)!$(C_OFF)   $$dag (pas encore chargé par le scheduler)"; \
	done

# =============================================================================
#  3. Pipeline : Bronze -> Silver -> Gold (déclenché puis attendu)
# =============================================================================
trigger: bronze  ## Alias de 'bronze' : déclenche la chaîne complète

bronze:  ## Déclenche dag_bronze_ingest (enchaîne Silver puis Gold)
	@echo -e "$(C_OK)[make]$(C_OFF) Déclenchement de dag_bronze_ingest (enchaîne Silver puis Gold)..."
	@$(AIRFLOW) dags trigger dag_bronze_ingest

silver:  ## Déclenche uniquement dag_silver_transform (idempotent)
	@echo -e "$(C_OK)[make]$(C_OFF) Déclenchement de dag_silver_transform..."
	@$(AIRFLOW) dags trigger dag_silver_transform

gold:  ## Déclenche uniquement dag_gold_aggregate (idempotent)
	@echo -e "$(C_OK)[make]$(C_OFF) Déclenchement de dag_gold_aggregate..."
	@$(AIRFLOW) dags trigger dag_gold_aggregate

ml:  ## Déclenche dag_ml_retrain (bonus : XGBoost + prédictions J+1)
	@echo -e "$(C_OK)[make]$(C_OFF) Déclenchement de dag_ml_retrain..."
	@$(AIRFLOW) dags trigger dag_ml_retrain

genai:  ## Démarre Ollama et télécharge le modèle des bulletins IA
	@echo -e "$(C_OK)[make]$(C_OFF) Démarrage d'Ollama (profil genai)..."
	@$(COMPOSE) --profile genai up -d ollama
	@docker exec ollama ollama pull llama3.2:3b

wait-pipeline:  ## Attend la fin de la chaîne Bronze -> Silver -> Gold
	@echo -e "$(C_OK)[make]$(C_OFF) Attente de la chaîne Bronze -> Silver -> Gold (timeout $(PIPELINE_TIMEOUT)s)..."
	@deadline=$$(( $$(date +%s) + $(PIPELINE_TIMEOUT) )); \
	for dag in $(PIPELINE_DAGS); do \
	  echo "  → $$dag"; \
	  while true; do \
	    state=$$($(AIRFLOW) dags list-runs -d $$dag -o plain 2>/dev/null | awk 'NR==2 {print $$3}'); \
	    case "$$state" in \
	      success) echo -e "    $(C_OK)succès$(C_OFF)"; break ;; \
	      failed)  echo -e "    $(C_ERR)ÉCHEC$(C_OFF) — inspectez : make logs SVC=airflow-scheduler"; exit 1 ;; \
	      *)       : ;; \
	    esac; \
	    if [ $$(date +%s) -ge $$deadline ]; then \
	      echo -e "    $(C_ERR)timeout$(C_OFF) (dernier état : $${state:-aucun run})"; exit 1; \
	    fi; \
	    sleep $(POLL_SECONDS); \
	  done; \
	done

# =============================================================================
#  4. Vérification automatique de la couche Medallion
# =============================================================================
verify:  ## Vérifie sur HDFS que Bronze, Silver et Gold sont complets et marqués
	@echo -e "$(C_OK)[make]$(C_OFF) Vérification de la couche Medallion sur HDFS..."
	@$(SPARK_PY)/verify_medallion.py --allow-empty-stream

dry-run:  ## Affiche le plan d'ingestion Météo-France (hors ligne, sans HDFS)
	@$(PY) scripts/meteofrance_ingest.py --dry-run --departments $(MF_DEPARTMENTS)

# =============================================================================
#  5. Exploitation
# =============================================================================
urls:  ## Rappelle les URLs des interfaces
	@echo ""
	@echo -e "$(C_OK)  Interfaces du datalake$(C_OFF)"
	@echo "  Airflow     http://localhost:8080     (admin / admin)"
	@echo "  HDFS        http://localhost:9870"
	@echo "  Jupyter     http://localhost:8888     (token : meteo)"
	@echo "  Dashboard   http://localhost:8501"
	@echo "  Kafka       localhost:9092            (topic : meteo-stream)"
	@echo ""

status:  ## État des conteneurs
	@$(COMPOSE) ps

ps: status  ## Alias de 'status'

SVC ?= airflow-scheduler
logs:  ## Logs d'un service (make logs SVC=kafka-producer)
	@$(COMPOSE) logs -f --tail=100 $(SVC)

stop:  ## Arrête les conteneurs (volumes conservés)
	@$(COMPOSE) stop

down:  ## Arrête et supprime conteneurs + réseaux (volumes conservés)
	@$(COMPOSE) down

reset:  ## Supprime TOUT, volumes compris (non interactif)
	@echo -e "$(C_WARN)[make] Suppression des conteneurs, réseaux ET volumes (HDFS, Kafka, Postgres).$(C_OFF)"
	@$(COMPOSE) down -v --remove-orphans

clean:  ## Nettoie les caches Python locaux
	@find . -type d -name __pycache__ -not -path './.git/*' -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name .pytest_cache -not -path './.git/*' -exec rm -rf {} + 2>/dev/null || true
	@echo -e "$(C_OK)[make]$(C_OFF) Caches Python nettoyés."
