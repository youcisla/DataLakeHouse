# 🌤️ Projet DataLake Météo — TP Big Data (DataLake / DataLakehouse)

> **Thème : Météo & analyse climatique** — un datalake **Bronze → Silver → Gold**
> persistant sur **HDFS**, ingérant deux sources hétérogènes (une **batch** : NOAA GHCN-D,
> une **temps réel** : Open-Meteo via Kafka + Spark Structured Streaming),
> orchestré de bout en bout par **Airflow**, avec **ML (XGBoost)**, **IA générative (Ollama)**
> et **dashboard Streamlit**.

**Équipe :** Youcef (architecture/backend) · Sarah (couche Medallion) · Sara (ML & GenAI) · Soufiane (dashboard)

---

## 1. Objectif du TP

Concevoir et implémenter un datalake en architecture Bronze → Silver → Gold qui :

1. **Ingère** au moins deux sources hétérogènes, dont une **en temps réel** (API → topic
   Kafka → consommé en continu par **Spark Structured Streaming** → écrit en **Bronze**),
   les archives batch étant déposées en Bronze avec un **marqueur d'idempotence `_SUCCESS`** ;
2. **Persiste et transforme** chaque couche sur HDFS (Silver = validation, déduplication,
   normalisation, Parquet Zstd ; Gold = agrégations/KPIs métier) — **aucune étape manuelle** ;
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
                BATCH (NOAA GHCN-D)│        TEMPS RÉEL (Open-Meteo API)
        ┌──────────────────────────┘        ┌───────────────────────────┐
        │                                   │ kafka_producer.py          │
        │                                   │ (toutes les 5 min, 5 villes)│
        │                                   └────────────┬──────────────┘
        ▼                                                ▼
┌───────────────────────┐                    ┌───────────────────────┐
│ /bronze/meteo/batch   │                    │   Kafka topic         │
│ source=noaa/year=..   │                    │   "meteo-stream"      │
│ (CSV bruts + _SUCCESS)│                    └───────────┬───────────┘
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
│  daily_aggregates · weekly_trends · extreme_events · ml_predictions  │
│  ai_insights (bulletins LLM)                                         │
└──────────┬───────────────────────────────────────────────────────────┘
           ▼
   Dashboard Streamlit (8501) · Jupyter (8888) · notebooks/
```

### Services Docker

| Service | Image | Port(s) | Rôle |
|---|---|---|---|
| `namenode` | apache/hadoop:3.3.6 | 9870 (UI/WebHDFS), 9000 (RPC) | Namenode HDFS |
| `datanode` | apache/hadoop:3.3.6 | — | Datanode HDFS |
| `zookeeper` | confluentinc/cp-zookeeper:7.5.3 | 2181 | Coordination Kafka |
| `kafka` | confluentinc/cp-kafka:7.5.3 | 9092 | Broker (topic `meteo-stream`) |
| `spark-master` | meteo-spark:3.5.1 (build) | 8080 (UI), 7077, 4040 | Master Spark + driver des jobs |
| `spark-worker` | meteo-spark:3.5.1 (build) | — | Workers Spark |
| `postgres` | postgres:15-alpine | 5432 | Méta-données Airflow |
| `airflow-init` | meteo-airflow:2.9.3 (build) | — | Migration DB + user admin (one-shot) |
| `airflow-webserver` | meteo-airflow:2.9.3 (build) | 8080 | UI Airflow |
| `airflow-scheduler` | meteo-airflow:2.9.3 (build) | — | Scheduler Airflow |
| `kafka-producer` | meteo-base (build) | — | Producteur Open-Meteo → Kafka (5 min) |
| `jupyter` | meteo-jupyter (build) | 8888 | Notebooks (token `meteo`) |
| `streamlit` | meteo-base (build) | 8501 | Dashboard Gold |
| `ollama` *(profil genai)* | ollama/ollama | 11434 | LLM local pour les bulletins IA |

---

## 3. Démarrage rapide

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

### 4.1 Batch — NOAA GHCN-D (~6,6 Go visés)

- **URL** : https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/
- **Format** : CSV par station (100 000+ stations mondiales), valeurs journalières
  (températures, précipitations, neige, vent).
- **Période ciblée** : 2022–2025 (les fichiers de stations contiennent l'historique
  complet ; la fenêtre temporelle est appliquée en **Silver** lors de la normalisation).
- **Schéma CSV** : `STATION, DATE, LATITUDE, LONGITUDE, ELEVATION, NAME, PRCP, TMAX,
  TMIN, TAVG, SNOW, SNWD, AWND`.
- ⚠️ **Unités NOAA** : les valeurs sont en **dixièmes** d'unité (TMAX=220 → 22,0 °C)
  et les valeurs manquantes valent **-9999** (9999 pour SNOW/SNWD). La conversion
  (÷10) est faite en Silver.
- **Ingestion** : `scripts/batch_ingest.py` énumère le portail, sélectionne les
  stations par taille décroissante jusqu'à atteindre `NOAA_TARGET_GB` (6,6 Go),
  télécharge avec reprise, et dépose les fichiers **bruts** en Bronze.
- **Mode `--synthetic`** : si NOAA est inaccessible (réseau filtré), le script
  génère des CSV au même schéma (marche aléatoire saisonnière), parfait pour la démo.

### 4.2 Temps réel — Open-Meteo API

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
| Batch NOAA | `/bronze/meteo/batch/source=noaa/year=YYYY/month=MM/` | `_SUCCESS` + `manifest.json` |
| Stream Open-Meteo | `/bronze/meteo/stream/source=openmeteo/year=YYYY/month=MM/day=DD/hour=HH/` | `_SUCCESS` par heure |

- Convention de partitionnement **`source=X/year=YYYY/month=MM`** (exigence du sujet).
  Pour le batch, `YYYY-MM` est **l'année/mois du lot d'ingestion** : chaque fichier
  de station est stocké **une seule fois** (aucune duplication d'octets) et la liste
  des stations déjà ingérées est conservée dans `_ingested.json`.
- **Quota** : le script de monitoring `hdfs_utils.quota_reached('/bronze', quota)`
  arrête automatiquement le producteur Kafka lorsque le Bronze atteint
  **`BRONZE_QUOTA_GB` (10,5 Go par défaut)** ; le DAG Bronze saute alors le streaming.
- **Budget HDFS** : Bronze ≈ 6,6 Go + Silver ≈ 3,3 Go + Gold ≈ 1,1 Go ≈ **11 Go ≤ 11 Go** ✔
  (à ajuster via `NOAA_TARGET_GB`, le streaming étant négligeable : ~10 Mo/jour).

### 🥈 Silver (validation · dédup · normalisation · indicateurs)

`scripts/silver_transform.py` (DAG `dag_silver_transform`) :

1. **Lecture** des partitions Bronze marquées `_SUCCESS` (CSV NOAA + JSON Open-Meteo) ;
2. **Validation du schéma** (colonnes obligatoires, sinon échec explicite) ;
3. **Déduplication** sur `(station_id, timestamp)` ;
4. **Normalisation** vers le schéma unifié :

```
station_id, station_name, city, country, latitude, longitude, elevation,
timestamp, temperature, precipitation, wind_speed, snow, source, dt
```

5. **Indicateurs** : moyennes mobiles 3j/7j (`temp_ma3`, `temp_ma7`), écart-type
   mobile 7j (`temp_std7`), anomalie (`temperature - temp_ma7`) ;
6. **Écriture** : Parquet **Zstd niveau 22**, partitionné `dt=YYYY-MM-DD`,
   en **overwrite dynamique** (idempotent) + `_SUCCESS` par partition.

### 🥇 Gold (agrégations / KPIs métier)

`scripts/gold_transform.py` (DAG `dag_gold_aggregate`) — tables Parquet :

| Table | Contenu | Partitionnement |
|---|---|---|
| `daily_aggregates` | T° moy/min/max, précip, vent, neige, écart-type par ville et jour | `dt=` |
| `weekly_trends` | Moyennes hebdo, pente de tendance, écart vs semaine précédente | `year=,week=` |
| `extreme_events` | Canicule, fortes pluies, vents violents, vague de froid (seuils configurables) | `dt=` |
| `ml_predictions` | Prédictions J+1 vs réalité, erreur, confiance, version du modèle | `dt=` |
| `ai_insights` | Bulletins météo générés par LLM | `dt=` |

---

## 6. Idempotence (critère central du TP)

Un DAG interrompu **peut être relancé sans dupliquer** :

| Mécanisme | Où | Pourquoi c'est sûr |
|---|---|---|
| Marqueur `_SUCCESS` | chaque répertoire Bronze (batch + heure stream) | l'ingestion ne re-traite jamais un lot déjà complet |
| `_ingested.json` | `/bronze/.../source=noaa/` | une station n'est jamais téléversée deux fois |
| **Checkpoint Kafka** | `/checkpoints/kafka_to_bronze` | le streaming reprend aux offsets exacts (exactly-once côté lecture) |
| **Overwrite dynamique** des partitions | Silver + Gold | relancer réécrit seulement les partitions présentes dans l'input |
| `--only-new` | DAG Silver | saute les `dt` déjà marquées `_SUCCESS` |
| Dédup `(station_id, timestamp)` | Silver | filet de sécurité même si un doublon arrive quand même |
| `_SUCCESS` par partition dt | Silver/Gold | les couches aval savent exactement quoi traiter |

---

## 7. Orchestration Airflow

| DAG | Fréquence | Déclenchement | Tâches |
|---|---|---|---|
| `dag_bronze_ingest` | quotidienne | planifiée ou manuelle | vérif quota → `batch_ingest.py` + `streaming_ingest.py` → déclenche Silver |
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
python -m pytest tests/test_transform.py -q
```

Couvre : parsing ville/pays NOAA, détection des valeurs manquantes, conversion
dixièmes → °C, validation de schéma, **déduplication**, classification des
événements extrêmes, calcul de pente de tendance.

---

## 12. Réponses préparées aux questions du professeur

**Q : Comment construisez-vous un profil client sans historique d'achat ?**
→ Nous adaptons la question au domaine météo : nous construisons un **profil météo**
par station (moyennes climatiques, amplitude thermique, jours de pluie, saisonnalité)
à partir de l'historique NOAA. Ces indicateurs servent de features au modèle de
prédiction — l'équivalent du profil client en météorologie.

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
│   ├── batch_ingest.py             # ingestion batch NOAA → Bronze (idempotent)
│   ├── streaming_ingest.py         # Spark Structured Streaming Kafka → Bronze
│   ├── hdfs_utils.py               # client WebHDFS (quotas, _SUCCESS, uploads)
│   ├── silver_transform.py         # Bronze → Silver (validation, dédup, normalisation)
│   ├── gold_transform.py           # Silver → Gold (KPIs, tendances, extrêmes)
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
│   └── test_transform.py           # tests unitaires (sans Spark)
├── configs/
│   ├── spark-defaults.conf         # Zstd 22, overwrite dynamique, mémoire
│   └── requirements.txt            # dépendances Python
├── deploy.sh                       # déploiement en 1 commande
└── README.md                       # ce document
```

---

## 14. Commandes utiles

```bash
# Cluster
./deploy.sh up                # démarre tout (build + init HDFS/Kafka/Airflow)
./deploy.sh up genai          # + Ollama
./deploy.sh status | logs <svc> | stop | down | reset
./deploy.sh trigger           # déclenche dag_bronze_ingest

# Producteur Kafka (test manuel)
docker compose --env-file docker/.env -f docker/docker-compose.yml   exec -T kafka-producer python /opt/project/scripts/kafka_producer.py --once

# Taille HDFS
docker compose --env-file docker/.env -f docker/docker-compose.yml   exec -T spark-master python /opt/project/scripts/hdfs_utils.py size /bronze

# Tests unitaires
python -m pytest tests/test_transform.py -q

# Dashboard local (hors Docker)
streamlit run dashboard/app.py --server.port 8501
```

---

## 15. Notes & limites

- **NOAA** : les valeurs sont en dixièmes d'unité (conversion ÷10 en Silver) ; le
  téléchargement réel de ~6,6 Go peut prendre 1–3 h selon le réseau → mode
  `--synthetic` ou `--target-gb 0.5` pour une démo rapide.
- Le **partitionnement batch** est fait par lot d'ingestion (année/mois) : c'est une
  convention de dépôt (le sujet n'impose pas le découpage du contenu) ; chaque
  fichier reste brut et n'est stocké qu'une fois. La fenêtre 2022–2025 est appliquée
  en Silver.
- **Ollama** est optionnel (profil `genai`) : sans lui, le bulletin fallback
  (règles) est généré et le DAG ne casse pas.
- Le connecteur **spark-sql-kafka** (`--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1`)
  est téléchargé depuis Maven Central au premier lancement du job streaming
  (le driver Spark a besoin d'accéder à internet une fois).
- En cas de perte du checkpoint Kafka, des doublons peuvent transiter par Bronze ;
  la dédup Silver (`station_id + timestamp`) les élimine.
