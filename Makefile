# =============================================================================
#  DataLake Meteo - TP Big Data (DataLake / DataLakehouse)
#  Automatisation complete du workflow Bronze -> Silver -> Gold.
#
#      make all    : de zero au Gold verifie, SANS aucune etape manuelle
#      make help   : liste toutes les cibles
#
#  PREREQUIS SUR LA MACHINE : Docker Desktop et make. RIEN D AUTRE.
#  Aucun Python, aucun Java, aucun Spark a installer : tout tourne dans les
#  conteneurs. Les tests eux-memes sont executes dans l image du projet.
#
#  PORTABILITE : chaque recette est UNE commande docker simple, donc identique
#  sous Windows cmd.exe / PowerShell, macOS et Linux. Aucun echo -e, aucune
#  boucle bash, aucun 2>/dev/null : toute la logique d attente et de controle
#  vit dans scripts/pipeline_ctl.py, execute DANS un conteneur.
# =============================================================================

ENV_FILE         := docker/.env
COMPOSE_FILE     := docker/docker-compose.yml
DC               := docker compose --env-file $(ENV_FILE) -f $(COMPOSE_FILE)

# exec  : dans un conteneur deja demarre
# run   : conteneur jetable, sans demarrer ses dependances
DCX              := $(DC) exec -T
DCRUN            := $(DC) run --rm --no-deps -T

# Les conteneurs qui portent la logique :
#   kafka-producer   image meteo-base : python + pandas + pytest + requests
#   airflow-webserver  CLI airflow + projet monte
#   spark-master     lecture HDFS pour la verification finale
CTL_AIRFLOW      := $(DCX) airflow-webserver python3 /opt/project/scripts/pipeline_ctl.py
CTL_BASE         := $(DCRUN) kafka-producer python3 /opt/project/scripts/pipeline_ctl.py

PROFILE          ?=
MF_DEPARTMENTS   ?= 75 69 13 33 59
PIPELINE_TIMEOUT ?= 2400
# Profondeur d historique exportee vers le site.
WEB_WINDOW_DAYS   ?= 180
METEO_TOPIC      ?= meteo-stream
SVC              ?= airflow-scheduler
# FORCE=1 rejoue les etapes deja marquees terminees dans les checkpoints.
FORCE            ?=
FORCE_FLAG       := $(if $(FORCE),--force,)
# GenAI n'est inclus dans `make all` que si PROFILE=genai (le modele LLM pese ~2 Go).
GENAI_STEP       := $(if $(filter genai,$(PROFILE)),genai,)

# Python de l hote : UNIQUEMENT pour la cible optionnelle test-local.
PY               ?= python3

.DEFAULT_GOAL := help
# Gold depend de Silver, qui depend de Bronze : pas d execution parallele.
.NOTPARALLEL:

.PHONY: help all re doctor build test test-local lint dry-run up wait-services \
        init topic unpause pipeline ml predict wait trigger bronze silver gold genai \
        export-web web-install web-dev web-build \
        checkpoints checkpoints-reset verify urls status ps logs stop down reset clean

help:  ## Affiche l aide - instantane, aucun conteneur demarre
	@echo DataLake Meteo - automatisation du TP, Bronze puis Silver puis Gold
	@echo Prerequis : Docker Desktop et make. Aucun Python a installer.
	@echo   make all - Workflow complet : Docker, tests, cluster,
	@echo                     Bronze, Silver, Gold, verification finale
	@echo   make re - Reset complet, volumes compris, puis rejoue tout
	@echo ---
	@echo   Qualite
	@echo   make test - Tests unitaires dans un conteneur
	@echo   make test-local - Tests avec le Python de l hote, si vous en avez un
	@echo   make lint - Compile scripts, DAGs et tests
	@echo   make dry-run - Plan d ingestion Meteo-France, hors ligne
	@echo ---
	@echo   Cluster
	@echo   make doctor - Verifie Docker et docker compose
	@echo   make build - Construit les images
	@echo   make up - Demarre le cluster
	@echo   make init - Cree les repertoires HDFS
	@echo   make topic - Cree le topic Kafka
	@echo   make unpause - Active les quatre DAGs
	@echo   make status - Etat des conteneurs
	@echo   make logs - Logs d un service : make logs SVC=namenode
	@echo   make stop - Arrete les conteneurs
	@echo   make down - Supprime conteneurs et reseaux
	@echo   make reset - Supprime TOUT, volumes compris
	@echo   make clean - Nettoie les caches Python du depot
	@echo ---
	@echo   Pipeline
	@echo   make pipeline - Declenche Bronze et attend toute la chaine
	@echo   make bronze - Rejoue une couche isolement, idempotent
	@echo   make silver - Rejoue Silver isolement, idempotent
	@echo   make gold - Rejoue Gold isolement, idempotent
	@echo   make genai - Bonus : Ollama et bulletins IA
	@echo   make checkpoints - Etat de reprise des trois etapes
	@echo   make checkpoints-reset - Vide les checkpoints
	@echo   make export-web - Exporte les tables Gold en JSON pour le site
	@echo   make web-dev - Site en local sur http://localhost:3000
	@echo   make verify - Controle les trois couches sur HDFS
	@echo   make urls - Rappelle les interfaces
	@echo ---
	@echo   Options
	@echo   make all PROFILE=genai - inclut aussi Ollama et les bulletins LLM
	@echo   make all FORCE=1 - rejoue les etapes deja terminees
	@echo     make all MF_DEPARTMENTS=75 69      restreint les departements
	@echo   make all PIPELINE_TIMEOUT=3600 - allonge l attente de la chaine

# =============================================================================
#  LA CIBLE PRINCIPALE : tout le TP, de bout en bout, sans intervention
# =============================================================================
all: doctor build test up wait-services init topic unpause pipeline ml predict $(GENAI_STEP) verify export-web urls  ## TOUT : cluster, Bronze, Silver, Gold, ML, predictions, verification, vitrine en ligne. Relancable : reprend ou il s est arrete.
	@echo [make] Workflow termine : Medallion, ML et predictions en place et verifies.

re: reset all  ## Reset complet, volumes compris, puis rejoue tout

# =============================================================================
#  1. Pre-vol et images
# =============================================================================
doctor:  ## Verifie que Docker et docker compose repondent
	@echo [make] Verification de Docker...
	@docker version
	@docker compose version

build:  ## Construit les images du projet
	@echo [make] Construction des images - la premiere fois, comptez 5 a 15 min...
	@$(DC) build

# =============================================================================
#  2. Qualite : tests unitaires, executes DANS l image du projet
# =============================================================================
test:  ## Lance les tests unitaires dans un conteneur - aucun Python requis sur l hote
	@echo [make] Tests unitaires dans un conteneur - sans Spark, sans HDFS...
	@$(DCRUN) kafka-producer python3 -m pytest tests -q

test-local:  ## Variante rapide : tests avec le Python de l hote, si vous en avez un
	@$(PY) -m pytest tests -q

lint:  ## Compile tous les scripts, DAGs et tests
	@$(DCRUN) kafka-producer python3 -m compileall -q scripts airflow/dags tests ml dashboard

dry-run:  ## Plan d ingestion Meteo-France, hors ligne
	@$(DCRUN) kafka-producer python3 /opt/project/scripts/meteofrance_ingest.py --dry-run --departments $(MF_DEPARTMENTS)

# =============================================================================
#  3. Cluster
# =============================================================================
up:  ## Demarre le cluster complet
	@echo [make] Demarrage du cluster...
	@$(DC) up -d --build

wait-services:  ## Attend que HDFS et Airflow repondent
	@$(CTL_BASE) wait-services

init:  ## Cree les repertoires HDFS du datalake
	@$(CTL_AIRFLOW) init $(FORCE_FLAG)

topic:  ## Cree le topic Kafka du flux temps reel
	@echo [make] Creation du topic Kafka $(METEO_TOPIC)...
	@$(DCX) kafka kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic $(METEO_TOPIC) --partitions 3 --replication-factor 1

unpause:  ## Active les quatre DAGs
	@$(CTL_AIRFLOW) unpause $(FORCE_FLAG)

# =============================================================================
#  4. Pipeline : Bronze -> Silver -> Gold
# =============================================================================
pipeline:  ## Bronze -> Silver -> Gold, puis attend la fin de la chaine
	@$(CTL_AIRFLOW) pipeline --timeout $(PIPELINE_TIMEOUT) $(FORCE_FLAG)

wait:  ## Attend la chaine sans rien declencher
	@$(CTL_AIRFLOW) wait --timeout $(PIPELINE_TIMEOUT)

trigger: bronze  ## Alias de bronze

bronze:  ## Declenche dag_bronze_ingest, qui enchaine Silver puis Gold
	@$(CTL_AIRFLOW) trigger dag_bronze_ingest

silver:  ## Declenche dag_silver_transform, idempotent
	@$(CTL_AIRFLOW) trigger dag_silver_transform

gold:  ## Declenche dag_gold_aggregate, idempotent
	@$(CTL_AIRFLOW) trigger dag_gold_aggregate

ml:  ## Entraine le modele XGBoost depuis le Silver, et attend la fin
	@$(CTL_AIRFLOW) ml --timeout $(PIPELINE_TIMEOUT) $(FORCE_FLAG)

predict:  ## Rejoue Gold une fois le modele pret : predictions J+1 et bulletin IA
	@$(CTL_AIRFLOW) predict --timeout $(PIPELINE_TIMEOUT) $(FORCE_FLAG)

genai:  ## Bonus : demarre Ollama et telecharge le modele des bulletins IA
	@$(DC) --profile genai up -d ollama
	@docker exec ollama ollama pull llama3.2:3b

# =============================================================================
#  5. Verification et exploitation
# =============================================================================
export-web:  ## Exporte les tables Gold en JSON pour le site Next.js
	@$(DCX) spark-master python3 /opt/project/scripts/export_web.py --days $(WEB_WINDOW_DAYS)

web-install:  ## Installe les dependances du site - dans un conteneur node
	@$(DC) --profile web run --rm web npm install

web-dev:  ## Lance le site sur http://localhost:3000 - dans un conteneur node
	@$(DC) --profile web run --rm --service-ports web npm run dev

web-build:  ## Build de production du site - verifie avant de deployer
	@$(DC) --profile web run --rm web npm run build

checkpoints:  ## Affiche l etat de reprise des trois etapes
	@$(DCX) spark-master python3 /opt/project/scripts/pipeline_ctl.py checkpoints

checkpoints-reset:  ## Vide les checkpoints : tout sera rejoue au prochain run
	@$(DCX) spark-master python3 /opt/project/scripts/pipeline_ctl.py checkpoints --reset

verify:  ## Controle sur HDFS que Bronze, Silver, Gold, ML et bulletins sont complets
	@$(DCX) spark-master python3 /opt/project/scripts/verify_medallion.py --allow-empty-stream --with-ml

urls:  ## Rappelle les URLs des interfaces
	@echo [make] Interfaces du datalake :
	@echo [make]   Airflow     http://localhost:8082    admin / admin
	@echo [make]   Spark       http://localhost:8081
	@echo [make]   HDFS        http://localhost:9870
	@echo [make]   Jupyter     http://localhost:8888    token : meteo
	@echo [make]   Dashboard   http://localhost:8501
	@echo [make]   Kafka       localhost:9092           topic : $(METEO_TOPIC)

status:  ## Etat des conteneurs
	@$(DC) ps

ps: status  ## Alias de status

logs:  ## Logs d un service : make logs SVC=kafka-producer
	@$(DC) logs -f --tail=100 $(SVC)

stop:  ## Arrete les conteneurs, volumes conserves
	@$(DC) stop

down:  ## Supprime conteneurs et reseaux, volumes conserves
	@$(DC) down

reset:  ## Supprime TOUT, volumes compris, non interactif
	@$(DC) down -v --remove-orphans

clean:  ## Nettoie les caches Python du depot
	@$(CTL_BASE) clean
