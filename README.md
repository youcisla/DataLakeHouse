# 🌤️ Projet DataLake Météo, TP Big Data (DataLake / DataLakehouse)

> **Thème : Météo & analyse climatique**. Un datalake **Bronze → Silver → Gold**
> persistant sur **HDFS**, ingérant deux sources hétérogènes (une **batch** : archives
> quotidiennes **Météo-France** de `meteo.data.gouv.fr`, une **temps réel** : **Open-Meteo**
> via Kafka + Spark Structured Streaming),
> orchestré par **Airflow**, avec **ML (XGBoost)**, **IA générative (Ollama)**
> et **dashboard Streamlit**.

**Équipe :** Youcef (architecture/backend) · Sarah (couche Medallion) · Sara (ML & GenAI) · Soufiane (dashboard)

---

## 1. Objectif du TP

Concevoir et implémenter un datalake en architecture Bronze → Silver → Gold qui :

1. **Ingère** au moins deux sources hétérogènes, dont une **en temps réel** (API → topic
   Kafka → consommé en continu par **Spark Structured Streaming** → écrit en **Bronze**),
   les archives batch étant déposées en Bronze avec un **marqueur d'idempotence `_SUCCESS`** ;
2. **Persiste et transforme** chaque couche sur HDFS (Silver = validation, déduplication,
   normalisation, Parquet Zstd ; Gold = agrégations/KPIs métier), **sans aucune étape manuelle**
   (`make all` va des tests au Gold vérifié en une commande) ;
3. **Orchestre** le tout avec **Airflow** : un DAG par couche, dépendances explicites,
   et un DAG interrompu **relançable sans dupliquer** les données ;
4. **Restitue** via un dashboard Streamlit **et** un notebook Jupyter (lecture directe
   des tables Gold) ;
5. **Bonus** : prédiction de température à 24h (XGBoost, RMSE cible < 2 °C), bulletins
   météo générés par LLM local (Ollama), réentraînement hebdomadaire automatique.

---

## 2. Architecture

```
                    ┌───────────────────────────────┐
                    │        SOURCES HÉTÉROGÈNES      │
                    └──────────────┬────────────────┘
          BATCH (Météo-France QUOT)│        TEMPS RÉEL (Open-Meteo API)
        ┌──────────────────────────┘        ┌───────────────────────────┐
        │                                   │ kafka_producer.py          │
        │                                   │ (toutes les 5 min, 5 villes)│
        │                                   └────────────┬──────────────┘
        ▼                                                ▼
┌───────────────────────┐                    ┌───────────────────────┐
│ /bronze/meteo/batch   │                    │   Kafka topic         │
│ source=meteofrance/   │                    │   "meteo-stream"      │
│   year=YYYY/month=MM  │                    └───────────┬───────────┘
│ (.csv.gz bruts+_SUCCESS)                                │
└───────────┬───────────┘                                │
            │                          ┌─────────────────▼──────────────┐
            │                          │ streaming_ingest.py            │
            │                          │ Spark Structured Streaming     │
            │                          │ (checkpoint HDFS, durée bornée)│
            │                          └─────────────────┬──────────────┘
            ▼                                            ▼
┌──────────────────────────────────────────────────────────────────────┐
│            BRONZE  /bronze/meteo/stream/source=openmeteo/...         │
│        (JSON bruts, partitionnés par heure + _SUCCESS)               │
└──────────────────────────────────┬───────────────────────────────────┘
                                   ▼  dag_silver_transform (Airflow)
┌──────────────────────────────────────────────────────────────────────┐
│  SILVER  /silver/meteo/dt=YYYY-MM-DD/  (Parquet Zstd niveau 22)      │
│  validation de schéma · dédup (station_id+timestamp) · normalisation │
│  indicateurs : moyennes mobiles 3j/7j, écart-type 7j, anomalie        │
└──────────────────────────────────┬───────────────────────────────────┘
                                   ▼  dag_gold_aggregate (Airflow)
┌──────────────────────────────────────────────────────────────────────┐
│  GOLD  /gold/meteo/                                                  │
│  daily_aggregates · weekly_trends · extreme_events · climate_profile │
│  ml_predictions · ai_insights (bulletins LLM)                        │
└──────────┬───────────────────────────────────────────────────────────┘
           ▼
   Dashboard Streamlit (8501) · Jupyter (8888) · notebooks/
```

### Services Docker

| Service | Image | Port(s) | Rôle |
|---|---|---|---|
| `namenode` | apache/hadoop:3.3.6 | 9870 (UI/WebHDFS), 9000 (RPC) | Namenode HDFS |
| `datanode` | apache/hadoop:3.3.6 | aucun | Datanode HDFS |
| `zookeeper` | confluentinc/cp-zookeeper:7.5.3 | 2181 | Coordination Kafka |
| `kafka` | confluentinc/cp-kafka:7.5.3 | 9092 | Broker (topic `meteo-stream`) |
| `spark-master` | meteo-spark:3.5.1 (build) | 8080 (UI), 7077, 4040 | Master Spark + driver des jobs |
| `spark-worker` | meteo-spark:3.5.1 (build) | aucun | Workers Spark |
| `postgres` | postgres:15-alpine | 5432 | Méta-données Airflow |
| `airflow-init` | meteo-airflow:2.9.3 (build) | aucun | Migration DB + user admin (one-shot) |
| `airflow-webserver` | meteo-airflow:2.9.3 (build) | 8080 | UI Airflow |
| `airflow-scheduler` | meteo-airflow:2.9.3 (build) | aucun | Scheduler Airflow |
| `kafka-producer` | meteo-base (build) | aucun | Producteur Open-Meteo → Kafka (5 min) |
| `jupyter` | meteo-jupyter (build) | 8888 | Notebooks (token `meteo`) |
| `streamlit` | meteo-base (build) | 8501 | Dashboard Gold |
| `ollama` *(profil genai)* | ollama/ollama | 11434 | LLM local pour les bulletins IA |

---

## 3. Démarrage rapide

### Tout, en une commande

```bash
make all
```

`make all` enchaîne **sans aucune étape manuelle** :

| # | Étape | Cible | Ce qu'elle fait |
|---|---|---|---|
| 1 | Qualité | `test` | 36 tests unitaires (sans Spark ni HDFS) ; installe `pytest`/`pandas` si absents |
| 2 | Pré-vol | `check-docker` | Vérifie Docker + `docker compose` v2 + démon actif, avec message explicite |
| 3 | Cluster | `up` | `docker compose up -d --build`, puis attend le Namenode HDFS **et** le webserver Airflow |
| 4 | Init | `init` | Crée `/bronze` `/silver` `/gold` `/models` `/checkpoints`, le topic Kafka, l'admin Airflow |
| 5 | DAGs | `unpause` | Active les quatre DAGs |
| 6 | Pipeline | `trigger` | Déclenche `dag_bronze_ingest`, qui enchaîne Silver puis Gold |
| 7 | Attente | `wait-pipeline` | Sonde l'état des trois DAGs jusqu'au succès (échoue vite si un DAG casse) |
| 8 | Contrôle | `verify` | `verify_medallion.py` : les trois couches existent, sont marquées `_SUCCESS` et non vides |
| 9 | Sortie | `urls` | Rappelle les interfaces |

Toute étape en échec arrête `make all` avec un code de sortie non nul :
un « succès » silencieux sur un datalake vide est impossible.

```bash
make help                       # liste toutes les cibles
make all PROFILE=genai          # + Ollama (bulletins IA)
make all MF_DEPARTMENTS="75 69" # restreint les départements ingérés
make re                         # reset complet (volumes compris) puis rejoue tout
```

### Cibles utiles

```bash
make test          # tests unitaires seuls (aucun Docker requis)
make lint          # compilation + pyflakes
make dry-run       # plan d'ingestion Météo-France, hors ligne
make bronze        # rejoue une couche isolément (idempotent)
make silver
make gold
make ml            # bonus : réentraînement XGBoost
make verify        # re-contrôle l'état des trois couches sur HDFS
make status        # état des conteneurs
make logs SVC=kafka-producer
make stop | down | reset | clean
```

### Ou manuellement, étape par étape

```bash
# 1) Lancer le cluster complet (build des images, initialisation HDFS/Kafka/Airflow)
./deploy.sh up

#    (optionnel) avec l'IA générative :
./deploy.sh up genai
docker exec ollama ollama pull llama3.2:3b   # télécharge le modèle LLM

# 2) Déclencher le pipeline (ou via l'UI Airflow → DAGs → bouton ▶)
./deploy.sh trigger      # lance dag_bronze_ingest → enchaîne silver → gold

# 3) Ouvrir :
#    Airflow    http://localhost:8080   (admin / admin)
#    HDFS UI    http://localhost:9870
#    Spark UI   http://localhost:8080   (ne pas confondre avec Airflow : 8080 est redirigé)
#    Jupyter    http://localhost:8888   (token : meteo)
#    Dashboard  http://localhost:8501
```

> ⚠️ **Ports 8080** : Airflow et Spark Master utilisent tous deux 8080 en interne ;
> dans ce projet **8080 est exposé pour Airflow**, l'UI Spark reste accessible
> `docker compose exec spark-master curl http://localhost:8080` ou en changeant
> le mapping de port dans `docker/docker-compose.yml`.

Autres commandes : `./deploy.sh status` · `./deploy.sh logs kafka-producer` ·
`./deploy.sh stop` · `./deploy.sh down` · `./deploy.sh reset` (⚠️ supprime les volumes).

---

## 4. Sources de données

### 4.1 Batch : archives Météo-France (meteo.data.gouv.fr)

- **Portail** : https://meteo.data.gouv.fr — jeu **« Données climatologiques de base -
  quotidiennes »**, exploré par les notebooks
  [loicduffar/meteo.data-Tools](https://github.com/loicduffar/meteo.data-Tools).
- **Fichiers** : un CSV gzippé **par département** et par période, variables
  **RR-T-Vent** (précipitations, températures, vent) :

```
{MF_BASE_URL}/Q_{DEP}_previous-1950-2024_RR-T-Vent.csv.gz    # historique
{MF_BASE_URL}/Q_{DEP}_latest-2025-2026_RR-T-Vent.csv.gz      # période courante
MF_BASE_URL = https://meteofrance.s3.sbg.io.cloud.ovh.net/data/synchro_ftp/BASE/QUOT
```

- **Schéma CSV** (séparateur `;`) : `NUM_POSTE ; NOM_USUEL ; LAT ; LON ; ALTI ;
  AAAAMMJJ ; RR ; QRR ; TN ; QTN ; TX ; QTX ; TM ; QTM ; FFM ; QFFM` (les colonnes
  `Qxxx` sont les codes qualité, conservées telles quelles en Bronze).
- ⚠️ **Valeurs manquantes** : ce sont des **champs vides**, pas des sentinelles
  numériques ; les unités sont déjà en °C / mm / m·s⁻¹ (aucune division par 10).
  La conversion « vide → NULL » est faite en Silver (`mf_parse_number`).
- **Départements ingérés** (`MF_DEPARTMENTS`) : `75 69 13 33 59`, soit ceux des cinq
  villes suivies en temps réel par Open-Meteo — les deux sources se recoupent donc
  dans les agrégats Gold. La fenêtre `MF_START_YEAR`/`MF_END_YEAR` sélectionne les
  périodes de fichiers à télécharger ; le filtrage fin des dates a lieu en Silver.
- **Ingestion** : `scripts/meteofrance_ingest.py` construit le plan (département ×
  période), télécharge avec reprise et dépose les fichiers **bruts** en Bronze.
- **Mode `--synthetic`** : si `meteo.data.gouv.fr` est inaccessible (réseau filtré),
  le script génère des `.csv.gz` au **schéma identique** (sinusoïde saisonnière
  bruitée, ~2 % de relevés incomplets), pour une démo de bout en bout.
- **Source historique facultative** : `scripts/batch_ingest.py` (NOAA GHCN-D) reste
  disponible et alimente `source=noaa/` ; la couche Silver sait lire les deux.

### 4.2 Temps réel : Open-Meteo API

- **URL** : https://open-meteo.com (sans clé API)
- **Fréquence** : toutes les 5 minutes (configurable, `POLL_INTERVAL_SECONDS`)
- **Villes** : Paris, Lyon, Marseille, Bordeaux, Lille
- **Flux** : `kafka_producer.py` interroge l'API et publie au format JSON sur le
  topic Kafka `meteo-stream` :

```json
{"city":"Paris","latitude":48.8534,"longitude":2.3488,"timestamp":"2026-08-27T14:00",
 "temperature":22.5,"windspeed":12.3,"winddirection":240,"weathercode":1,
 "precipitation":0.0,"source":"OPENMETEO"}
```

---

## 5. Les trois couches

### 🥉 Bronze (format brut, aucune transformation)

| Flux | Chemin HDFS | Marqueur |
|---|---|---|
| Batch Météo-France | `/bronze/meteo/batch/source=meteofrance/year=YYYY/month=MM/` | `_SUCCESS` + `manifest.json` + `_ingested.json` |
| Stream Open-Meteo | `/bronze/meteo/stream/source=openmeteo/year=YYYY/month=MM/day=DD/hour=HH/` | `_SUCCESS` par heure |

- Convention de partitionnement **`source=X/year=YYYY/month=MM`** (exigence du sujet).
  Pour le batch, `YYYY-MM` est **l'année/mois du lot d'ingestion** : chaque fichier
  départemental est stocké **une seule fois** (aucune duplication d'octets) et la liste
  des lots déjà ingérés (clés `DEP:période`) est conservée dans `_ingested.json`.
- **Quota** : le script de monitoring `hdfs_utils.quota_reached('/bronze', quota)`
  arrête automatiquement le producteur Kafka lorsque le Bronze atteint
  **`BRONZE_QUOTA_GB` (10,5 Go par défaut)** ; le DAG Bronze saute alors le streaming.
- **Budget HDFS** : Bronze ≈ 6,6 Go + Silver ≈ 3,3 Go + Gold ≈ 1,1 Go ≈ **11 Go ≤ 11 Go** ✔
  (à ajuster via `NOAA_TARGET_GB`, le streaming étant négligeable : ~10 Mo/jour).

### 🥈 Silver (validation · dédup · normalisation · indicateurs)

`scripts/silver_transform.py` (DAG `dag_silver_transform`) :

1. **Lecture** des partitions Bronze marquées `_SUCCESS` (CSV `;` gzippés
   Météo-France, CSV NOAA facultatifs, JSON Open-Meteo) ;
2. **Validation du schéma** (colonnes obligatoires, sinon échec explicite :
   `METEOFRANCE_REQUIRED_COLUMNS`, `OPENMETEO_REQUIRED_COLUMNS`) ;
3. **Déduplication** sur `(station_id, timestamp)` ;
4. **Normalisation** vers le schéma unifié (`station_id = MF_<NUM_POSTE>`,
   `city` déduite de `NOM_USUEL` pour recouper les villes Open-Meteo,
   `temperature = TM` sinon `(TN + TX) / 2`) :

```
station_id, station_name, city, country, latitude, longitude, elevation,
timestamp, temperature, precipitation, wind_speed, snow, source, dt
```

5. **Indicateurs** : moyennes mobiles 3j/7j (`temp_ma3`, `temp_ma7`), écart-type
   mobile 7j (`temp_std7`), anomalie (`temperature - temp_ma7`) ;
6. **Écriture** : Parquet **Zstd niveau 22**, partitionné `dt=YYYY-MM-DD`,
   en **overwrite dynamique** (idempotent) + `_SUCCESS` par partition.

### 🥇 Gold (agrégations / KPIs métier)

`scripts/gold_transform.py` (DAG `dag_gold_aggregate`) produit les tables Parquet :

| Table | Contenu | Partitionnement |
|---|---|---|
| `daily_aggregates` | T° moy/min/max, précip, vent, neige, écart-type par ville et jour | `dt=` |
| `weekly_trends` | Moyennes hebdo, pente de tendance, écart vs semaine précédente | `year=,week=` |
| `extreme_events` | Canicule, fortes pluies, vents violents, vague de froid (seuils configurables) | `dt=` |
| `climate_profile` | **Profil météo** mensuel par ville : normales de T°, amplitude thermique, cumul de pluie moyen, jours de pluie, vent, saison | `month=` |
| `ml_predictions` | Prédictions J+1 vs réalité, erreur, confiance, version du modèle | `dt=` |
| `ai_insights` | Bulletins météo générés par LLM | `dt=` |

---

## 6. Idempotence (critère central du TP)

Un DAG interrompu **peut être relancé sans dupliquer** :

| Mécanisme | Où | Pourquoi c'est sûr |
|---|---|---|
| Marqueur `_SUCCESS` | chaque répertoire Bronze (batch + heure stream) | l'ingestion ne re-traite jamais un lot déjà complet |
| `_ingested.json` | `/bronze/.../source=meteofrance/` | un lot (département × période) n'est jamais téléversé deux fois |
| **Checkpoint Kafka** | `/checkpoints/kafka_to_bronze` | le streaming reprend aux offsets exacts (exactly-once côté lecture) |
| **Overwrite dynamique** des partitions | Silver + Gold | relancer réécrit seulement les partitions présentes dans l'input |
| `--only-new` | DAG Silver | saute les `dt` déjà marquées `_SUCCESS` |
| Dédup `(station_id, timestamp)` | Silver | filet de sécurité même si un doublon arrive quand même |
| `_SUCCESS` par partition dt | Silver/Gold | les couches aval savent exactement quoi traiter |

---

## 7. Orchestration Airflow

| DAG | Fréquence | Déclenchement | Tâches |
|---|---|---|---|
| `dag_bronze_ingest` | quotidienne | planifiée ou manuelle | vérif quota → `meteofrance_ingest.py` + `streaming_ingest.py` → déclenche Silver |
| `dag_silver_transform` | sur déclenchement | automatique (après Bronze) ou manuelle | `silver_transform.py` → déclenche Gold |
| `dag_gold_aggregate` | sur déclenchement | automatique (après Silver) ou manuelle | `gold_transform.py` → `inference.py` → `genai_summary.py` |
| `dag_ml_retrain` | hebdomadaire (lun. 03h00) | planifiée ou manuelle | `feature_engineering.py` → `train_model.py` |

Les dépendances entre couches sont **explicites** (`TriggerDagRunOperator`) et chaque
DAG reste **relançable isolément** (idempotence). Le scheduler lance `spark-submit`
(SparkSubmitOperator, connexion `spark_default` = `spark://spark-master:7077`).

---

## 8. Machine Learning (bonus)

- **Objectif** : prédire la température à **J+1** par ville/station (régression).
- **Features** (`ml/feature_engineering.py`) : lags J-1/J-2/J-7, moyennes mobiles 3j/7j,
  écart-type 7j, précipitations cumulées 3j, vent moyen 7j, mois, jour de l'année,
  jour de la semaine, saison, encodage de la ville.
- **Modèle** (`ml/train_model.py`) : **XGBoost Regressor** + GridSearchCV,
  split **temporel** 70/15/15, métriques RMSE/MAE/R², sauvegarde versionnée
  `/models/temperature_predictor_v{N}/` (joblib + metrics.json).
- **Inférence** (`ml/inference.py`) : charge le dernier modèle, prédit J+1 pour
  toutes les locations et écrit `/gold/meteo/ml_predictions/`.
- **Cible** : RMSE < 2 °C sur le jeu de test.

## 9. IA générative (bonus)

`ml/genai_summary.py` construit un **prompt structuré en français** à partir des
tables Gold (températures, précipitations, événements extrêmes, prédictions) et
appelle **Ollama** (Llama 3.2 local, sans clé API). Si Ollama est indisponible,
un **bulletin fallback** (règles) est généré pour ne jamais casser le pipeline.
Résultat : `/gold/meteo/ai_insights/dt=.../bulletin.json`.

---

## 10. Dashboard & notebooks

- **`dashboard/app.py`** (Streamlit, auto-refresh 30 s) :
  - Vue d'ensemble : KPIs par ville, **carte de France**, évolution 30 jours, événements ;
  - Panneau **ML** (`ml_panel.py`) : prévisions vs réalité, erreurs, confiance ;
  - Panneau **IA** (`genai_panel.py`) : bulletin météo du jour.
  - Lecture HDFS **sans client HDFS** : WebHDFS REST (`dashboard/gold_reader.py`).
- **`notebooks/dashboard.ipynb`** : version Pandas/Matplotlib/Seaborn des insights.
- **`notebooks/eda_ml.ipynb`** : EDA de Silver + justification du choix XGBoost.

---

## 11. Tests

```bash
make test                      # ou : python -m pytest tests -q
```

**36 tests, sans Spark ni HDFS** — ils tournent hors Docker, en une seconde.

- `tests/test_transform.py` — fonctions pures historiques : parsing ville/pays NOAA,
  détection des valeurs manquantes, conversion dixièmes → °C, validation de schéma,
  **déduplication**, classification des événements extrêmes, pente de tendance.
- `tests/test_medallion.py` — la couche Medallion de bout en bout, avec pandas :
  - **Bronze** : convention `source=X/year=YYYY/month=MM`, construction des URLs
    Météo-France, périodes `previous`/`latest`, **idempotence du plan d'ingestion**
    (reprise après interruption), génération et relecture d'un lot `.csv.gz` au
    schéma officiel, contenu du `manifest.json` ;
  - **Silver** : `mf_parse_number` (champs vides → NULL), `mf_parse_date`,
    `mf_station_id`, `mf_city_name`, `mf_mean_temperature`, validation de schéma,
    puis normalisation d'un Bronze synthétique (2 villes × 365 jours), dédup
    `(station_id, timestamp)` et indicateurs de fenêtre ;
  - **Gold** : agrégats quotidiens, `season_of_month`, `rain_day_ratio`,
    `climate_profile` (12 mois × 2 villes, saisonnalité vérifiée) et détection
    d'événements extrêmes sur le Silver produit ;
  - **Contrôle** : le contrat de `make verify` — couverture des trois couches,
    tolérance d'un flux temps réel encore vide, règles de verdict
    (`OK` / `ABSENT` / `SANS _SUCCESS` / `VIDE`) et rendu du rapport.

---

## 12. Réponses préparées aux questions du professeur

**Q : Comment construisez-vous un profil client sans historique d'achat ?**
→ Nous adaptons la question au domaine météo : nous construisons un **profil météo**
par station (moyennes climatiques, amplitude thermique, jours de pluie, saisonnalité)
à partir de l'historique NOAA. Ces indicateurs servent de features au modèle de
prédiction, l'équivalent météo d'un profil client.

**Q : Où est l'historique d'achat dans la météo ?**
→ L'analogue de l'historique d'achat est **l'historique climatique** : les archives
NOAA fournissent des décennies d'enregistrements quotidiens. Les séries temporelles
permettent d'identifier patterns saisonniers, tendances de réchauffement et
événements extrêmes récurrents.

**Q : Comment prédire sans profil ?**
→ Le modèle s'appuie sur des **features temporelles** (mois, jour de l'année, saison)
et **climatiques** (lags J-1/J-2/J-7, moyennes mobiles, écart-types) : exactement
comme un prévisionniste, on utilise l'historique pour anticiper le futur.

---

## 13. Arborescence du projet

```
projet-meteo/
├── docker/
│   ├── docker-compose.yml          # cluster complet (HDFS, Spark, Kafka, Airflow, ...)
│   ├── .env                        # toutes les variables de configuration
│   └── images/                     # Dockerfiles (spark, airflow, base, jupyter)
├── scripts/
│   ├── kafka_producer.py           # producteur Open-Meteo → Kafka (5 min, quota)
│   ├── meteofrance_ingest.py       # ingestion batch Météo-France → Bronze (idempotent)
│   ├── batch_ingest.py             # ingestion batch NOAA → Bronze (facultative)
│   ├── streaming_ingest.py         # Spark Structured Streaming Kafka → Bronze
│   ├── hdfs_utils.py               # client WebHDFS (quotas, _SUCCESS, uploads)
│   ├── silver_transform.py         # Bronze → Silver (validation, dédup, normalisation)
│   ├── gold_transform.py           # Silver → Gold (KPIs, tendances, extrêmes, profil)
│   ├── verify_medallion.py         # contrôle automatique des 3 couches (make verify)
│   └── compress_silver.py          # ré-écriture Silver en Zstd niveau 22
├── ml/
│   ├── feature_engineering.py      # lags, moyennes mobiles, encodages, target J+1
│   ├── train_model.py              # XGBoost + GridSearchCV, split 70/15/15
│   ├── inference.py                # prédictions J+1 → Gold/ml_predictions
│   └── genai_summary.py            # bulletins météo LLM (Ollama, fallback)
├── dashboard/
│   ├── app.py                      # Streamlit (3 vues, auto-refresh 30 s)
│   ├── gold_reader.py              # lecture tables Gold via WebHDFS
│   ├── ml_panel.py                 # widget prévisions vs réalité
│   ├── genai_panel.py              # widget bulletin IA
│   └── config.toml                 # thème + refresh + HDFS
├── airflow/dags/
│   ├── dag_bronze_ingest.py        # DAG 1 : ingestion Bronze
│   ├── dag_silver_transform.py     # DAG 2 : Bronze → Silver
│   ├── dag_gold_aggregate.py       # DAG 3 : Silver → Gold + ML + GenAI
│   └── dag_ml_retrain.py           # DAG 4 : réentraînement hebdo
├── notebooks/
│   ├── eda_ml.ipynb                # EDA + justification du modèle
│   └── dashboard.ipynb             # visualisations Pandas/Matplotlib
├── tests/
│   ├── conftest.py                 # rend scripts/ importable
│   ├── test_transform.py           # fonctions pures (sans Spark)
│   └── test_medallion.py           # Bronze → Silver → Gold (sans Spark)
├── configs/
│   ├── spark-defaults.conf         # Zstd 22, overwrite dynamique, mémoire
│   └── requirements.txt            # dépendances Python
├── Makefile                        # automatisation complète (make all)
├── deploy.sh                       # déploiement en 1 commande
└── README.md                       # ce document
```

---

## 14. Commandes utiles

```bash
# Workflow complet (recommandé)
make all                      # tests -> cluster -> Bronze -> Silver -> Gold -> vérification
make help                     # toutes les cibles

# Cluster (équivalents bas niveau)
./deploy.sh up                # démarre tout (build + init HDFS/Kafka/Airflow)
./deploy.sh up genai          # + Ollama
./deploy.sh status | logs <svc> | stop | down | reset
./deploy.sh trigger           # déclenche dag_bronze_ingest

# Producteur Kafka (test manuel)
docker compose --env-file docker/.env -f docker/docker-compose.yml   exec -T kafka-producer python /opt/project/scripts/kafka_producer.py --once

# Taille HDFS
docker compose --env-file docker/.env -f docker/docker-compose.yml   exec -T spark-master python /opt/project/scripts/hdfs_utils.py size /bronze

# Ingestion batch Météo-France (test manuel : plan sans téléchargement)
python scripts/meteofrance_ingest.py --dry-run --departments 75 69 13 33 59

# Contrôle de la couche Medallion sur HDFS
make verify

# Tests unitaires
make test

# Dashboard local (hors Docker)
streamlit run dashboard/app.py --server.port 8501
```

---

## 15. Notes & limites

- **Météo-France** : les valeurs manquantes sont des **champs vides** (et non des
  sentinelles `-9999` comme chez NOAA) ; les unités sont déjà en °C / mm / m·s⁻¹.
  Si `meteo.data.gouv.fr` est filtré par le réseau, `--synthetic` produit des lots
  au schéma identique et la chaîne complète reste démontrable.
- **NOAA** (source facultative conservée) : valeurs en dixièmes d'unité (÷10 en
  Silver), sentinelles `-9999` / `9999` ; ~6,6 Go de téléchargement.
- Le **partitionnement batch** est fait par lot d'ingestion (année/mois) : c'est une
  convention de dépôt (le sujet n'impose pas le découpage du contenu) ; chaque
  fichier reste brut et n'est stocké qu'une fois. La fenêtre temporelle est appliquée
  en Silver (`SILVER_START_DATE` / `SILVER_END_DATE`).
- **Ollama** est optionnel (profil `genai`) : sans lui, le bulletin fallback
  (règles) est généré et le DAG ne casse pas.
- Le connecteur **spark-sql-kafka** (`--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1`)
  est téléchargé depuis Maven Central au premier lancement du job streaming
  (le driver Spark a besoin d'accéder à internet une fois).
- En cas de perte du checkpoint Kafka, des doublons peuvent transiter par Bronze ;
  la dédup Silver (`station_id + timestamp`) les élimine.
