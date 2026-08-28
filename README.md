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
4. **Restitue** via un dashboard Streamlit lisant directement les tables Gold
   (un service Jupyter reste à disposition pour l'exploration) ;
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
   Dashboard Streamlit (8501) · Jupyter (8888)
```

### Services Docker

| Service | Image | Port(s) | Rôle |
|---|---|---|---|
| `namenode` | apache/hadoop:3.3.6 | 9870 (UI/WebHDFS), 9000 (RPC) | Namenode HDFS |
| `datanode` | apache/hadoop:3.3.6 | aucun | Datanode HDFS |
| `zookeeper` | confluentinc/cp-zookeeper:7.5.3 | 2181 | Coordination Kafka |
| `kafka` | confluentinc/cp-kafka:7.5.3 | 9092 | Broker (topic `meteo-stream`) |
| `spark-master` | meteo-spark:3.5.1 (build) | 8081 (UI), 7077, 4040 | Master Spark + driver des jobs |
| `spark-worker` | meteo-spark:3.5.1 (build) | aucun | Workers Spark |
| `postgres` | postgres:15-alpine | 5432 | Méta-données Airflow |
| `airflow-init` | meteo-airflow:2.9.3 (build) | aucun | Migration DB + user admin (one-shot) |
| `airflow-webserver` | meteo-airflow:2.9.3 (build) | **8082** (hôte) → 8080 | UI Airflow |
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
| 1 | Pré-vol | `doctor` | Vérifie que Docker et `docker compose` v2 répondent |
| 2 | Images | `build` | Construit les images du projet |
| 3 | Qualité | `test` | 62 tests unitaires, **exécutés dans un conteneur** |
| 4 | Cluster | `up` | `docker compose up -d --build` |
| 5 | Attente | `wait-services` | Sonde WebHDFS et la santé d'Airflow depuis le réseau Docker |
| 6 | Init | `init` + `topic` | Répertoires `/bronze` `/silver` `/gold` `/models` `/checkpoints`, puis topic Kafka |
| 7 | DAGs | `unpause` | Active les quatre DAGs |
| 8 | Pipeline | `pipeline` | Déclenche Bronze, attend Bronze → Silver → Gold → entraînement → inférence → bulletin |
| 9 | IA *(si `PROFILE=genai`)* | `genai` | Démarre Ollama et télécharge le modèle LLM |
| 10 | Contrôle | `verify` | Les trois couches **plus** `ml_predictions` et `ai_insights` |
| 11 | Vitrine | `export-web` | Exporte les tables Gold en JSON pour le site Next.js |
| 12 | Sortie | `urls` | Rappelle les interfaces |

**L'entraînement et l'inférence vivent dans le DAG Gold.** `dag_gold_aggregate`
calcule les tables Gold, puis entraîne XGBoost si `/models` est vide, puis prédit
J+1 et génère le bulletin. Un seul déclenchement (`pipeline`) produit donc tout,
ML compris. Le réentraînement périodique reste confié à `dag_ml_retrain`
(hebdomadaire) et s'exécute à part, sans rien casser.

Toute étape en échec arrête `make all` avec un code de sortie non nul :
un « succès » silencieux sur un datalake vide est impossible.

> ### `make all` est relançable en boucle
>
> **Chaque étape est checkpointée** (`workflow` dans `/checkpoints/medallion/`).
> Une étape terminée est sautée au passage suivant : `make all` reprend là où il
> s'est arrêté, quelle qu'ait été la cause de l'arrêt (coupure réseau, `Ctrl-C`,
> machine éteinte. Relancé après un succès complet, il ne refait rien et se
> contente d'être vert.
>
> ```bash
> make all              # reprend où il en était
> make all FORCE=1      # rejoue tout, checkpoints ignorés
> make checkpoints      # où en est chaque étape ?
> make checkpoints-reset
> ```
>
> Les premières étapes (`doctor`, `build`, `test`, `up`, `wait-services`) tournent
> toujours : elles précèdent HDFS (donc les checkpoints) et sont de toute façon
> quasi instantanées (images en cache, `up` idempotent). Les tests notamment
> tournent à **chaque** passage : sauter une suite parce qu'elle était verte la
> fois d'avant est exactement ce qui laisse passer une régression.

> ### Prérequis : Docker Desktop et `make`. Rien d'autre.
>
> **Aucun Python, aucun Java, aucun Spark à installer sur la machine.** Les
> tests eux-mêmes tournent dans l'image du projet
> (`docker compose run --rm kafka-producer python -m pytest`), et toute la
> logique d'orchestration (boucles d'attente, sondes WebHDFS, lecture de
> l'état des DAGs) s'exécute **dans les conteneurs**, via
> `scripts/pipeline_ctl.py` (Python standard, zéro dépendance).
>
> Côté hôte, le `Makefile` n'invoque donc que deux binaires : **`docker`** et
> **`echo`**. Chaque recette est **une seule commande simple** : ni `echo -e`,
> ni `2>/dev/null`, ni boucle `bash`, ni `curl`/`grep`/`awk`. Rien que
> `cmd.exe` ne sache exécuter. Le workflow est identique sous **Windows
> (PowerShell / cmd), macOS et Linux**.
>
> `scripts/pipeline_ctl.py` détecte de quel côté de la frontière Docker il
> tourne : dans un conteneur il appelle la CLI `airflow` directement et joint
> `namenode:9870` ; depuis l'hôte il passe par `docker compose exec` et
> `localhost:9870`.
>
> Si vous *avez* Python sur votre machine, `make test-local` lance la même
> suite en une seconde, sans conteneur.

```bash
make help                       # aide instantanée, sans démarrer de conteneur
make all PROFILE=genai          # + Ollama (bulletins IA)
make all MF_DEPARTMENTS="75 69" # restreint les départements ingérés
make all PIPELINE_TIMEOUT=3600  # allonge l'attente de la chaîne
make re                         # reset complet (volumes compris) puis rejoue tout
```

### Cibles utiles

```bash
make doctor        # Docker et docker compose repondent-ils ?
make test          # tests unitaires, dans un conteneur
make test-local    # idem avec le Python de l'hote, si vous en avez un
make lint          # compilation de tous les scripts et DAGs
make dry-run       # plan d'ingestion Météo-France, hors ligne
make bronze        # rejoue une couche isolément (idempotent)
make silver
make gold
make checkpoints   # où en est chaque étape (reprise)
make verify        # re-contrôle l'état des trois couches sur HDFS
make export-web    # exporte les tables Gold en JSON pour le site Next.js
make web-dev       # site web en local sur http://localhost:3000
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
#    Airflow    http://localhost:8082   (admin / admin)
#    HDFS UI    http://localhost:9870
#    Spark UI   http://localhost:8081
#    Jupyter    http://localhost:8888   (token : meteo)
#    Dashboard  http://localhost:8501
```

> ⚠️ **Ports** : Airflow et Spark Master écoutent tous deux sur 8080 **à
> l'intérieur** de leur conteneur. Côté hôte, **8080 est volontairement laissé
> libre** (il est très souvent déjà pris) : Airflow est publié sur **8082**
> (`AIRFLOW_WEB_PORT`) et l'UI Spark sur
> 8081 (http://localhost:8081).

Autres commandes : `./deploy.sh status` · `./deploy.sh logs kafka-producer` ·
`./deploy.sh stop` · `./deploy.sh down` · `./deploy.sh reset` (⚠️ supprime les volumes).

---

## 4. Sources de données

### 4.1 Batch : archives Météo-France (meteo.data.gouv.fr)

- **Portail** : https://meteo.data.gouv.fr, jeu **« Données climatologiques de base -
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
  villes suivies en temps réel par Open-Meteo, les deux sources se recoupent donc
  dans les agrégats Gold. La fenêtre `MF_START_YEAR`/`MF_END_YEAR` sélectionne les
  périodes de fichiers à télécharger ; le filtrage fin des dates a lieu en Silver.
- **Ingestion** : `scripts/meteofrance_ingest.py` construit le plan (département ×
  période), télécharge avec reprise et dépose les fichiers **bruts** en Bronze.
- **Mode `--synthetic`** : si `meteo.data.gouv.fr` est inaccessible (réseau filtré),
  le script génère des `.csv.gz` au **schéma identique** (sinusoïde saisonnière
  bruitée, ~2 % de relevés incomplets), pour une démo de bout en bout.

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
- **Budget HDFS** : les archives Météo-France (5 départements) pèsent quelques Mo
  et le streaming ajoute ~10 Mo/jour. Le quota `BRONZE_QUOTA_GB` (10,5 Go) borne
  l'ensemble et arrête le producteur si besoin.

### 🥈 Silver (validation · dédup · normalisation · indicateurs)

`scripts/silver_transform.py` (DAG `dag_silver_transform`) :

1. **Lecture** des partitions Bronze marquées `_SUCCESS` (CSV `;` gzippés
   Météo-France et JSON Open-Meteo) ;
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
| **Checkpoints par unité de travail** | `/checkpoints/medallion/<étape>.json` | commit **après chaque** lot / partition : une interruption ne coûte jamais plus que l'unité en cours |
| **Checkpoint Kafka** | `/checkpoints/kafka_to_bronze` | le streaming reprend aux offsets exacts (exactly-once côté lecture) |
| **Overwrite dynamique** des partitions | Silver + Gold | relancer réécrit seulement les partitions présentes dans l'input |
| `--only-new` | DAG Silver **et Gold** | saute les `dt` déjà marquées `_SUCCESS` **et** déjà présentes dans les checkpoints (une seule lecture native `globStatus`) |
| Dédup `(station_id, timestamp)` | Silver | filet de sécurité même si un doublon arrive quand même |
| `_SUCCESS` par partition dt | Silver/Gold | les couches aval savent exactement quoi traiter |

---

### Accès HDFS : natif, jamais REST, dans les jobs Spark

Un job Spark **parle déjà HDFS nativement** : le faire passer par WebHDFS ajoute
un aller-retour HTTP là où la JVM répond gratuitement. Sur quatre ans de données
(~1460 partitions `dt`), l'ancienne version de `write_silver` coûtait :

| Opération | Avant | Après |
|---|---|---|
| Calcul du pipeline Silver | **2×** (pas de cache avant `collect()`) | 1× (`persist`) |
| Lecture des partitions existantes | ~1460 appels WebHDFS | **1** `globStatus` natif |
| Dépôt des `_SUCCESS` | ~4400 appels (exists + mkdirs + create) | JVM locale, 0 HTTP |
| Checkpoints | ~2900 appels — chacun relisant/réécrivant **tout** l'état | **1** lecture + **1** écriture (`mark_many`) |

Soit environ **9000 aller-retours HTTP** remplacés par une poignée d'appels JVM,
et la moitié du travail Spark supprimée. Les trois jobs (`silver_transform`,
`gold_transform`, `streaming_ingest`) n'importent plus `hdfs_utils` du tout —
**un test l'interdit**. `hdfs_utils` reste le bon outil là où il n'y a pas de JVM
Spark : ingestion batch, dashboard Streamlit, `pipeline_ctl`.

Le job de streaming souffrait du même mal en pire : sa boucle filtrait le
micro-batch **une fois par heure**, recalculant le parsing JSON `N+2` fois toutes
les 30 secondes. Un `partitionBy("year","month","day","hour")` produit exactement
la même arborescence Bronze en **une seule écriture**.

### Checkpoints : reprise fine (`scripts/checkpoint.py`)

Les marqueurs `_SUCCESS` donnent la granularité **partition**. Les checkpoints
ajoutent la granularité **unité de travail**, et surtout : le commit a lieu
**après chaque unité**, jamais par paquets.

| Étape | Une unité = | Clé |
|---|---|---|
| `bronze_meteofrance` | un lot Météo-France | `75:latest-2025-2026` |
| `silver` | une partition | `2025-01-15` |
| `gold` | une table × une partition | `daily_aggregates:2025-01-15` |

Concrètement : 10 lots à ingérer, coupure après le 7ᵉ → la reprise ne traite
que les 3 restants. Avant, l'état n'était sauvegardé que tous les 5 lots, donc
jusqu'à 4 lots déjà déposés en Bronze étaient rejoués.

```bash
make checkpoints         # où en est chaque étape ?
make checkpoints-reset   # tout oublier : le prochain run recalcule tout
```

```
ETAPE                 UNITES DERNIER RUN            STATUT     MAJ
----------------------------------------------------------------------------
bronze_meteofrance        10 bronze-20260827T151202Z success    2026-08-27T15:12:02+00:00
silver                   365 silver-20260827T151530Z success    2026-08-27T15:15:30+00:00
gold                     365 gold-20260827T151812Z   success    2026-08-27T15:18:12+00:00
```

Choix de conception :

- **Stockage dans HDFS** (`/checkpoints/medallion/`), via `hdfs_utils` (WebHDFS) :
  présent dans **toutes** les images du projet, donc aucune dépendance ajoutée et
  aucune image à reconstruire. Un backend fichier (`METEO_CHECKPOINT_BACKEND=file`)
  sert aux tests et à l'usage hors cluster.
- **Pas de second journal en base.** Le Postgres d'Airflow enregistre déjà l'état,
  les reprises et les durées de chaque tâche, visibles dans son UI : un ledger SQL
  parallèle ferait doublon. Les checkpoints couvrent ce qu'Airflow ignore : la
  progression *à l'intérieur* d'une tâche.
- **Jamais bloquant.** Un checkpoint est une optimisation de reprise : si son
  écriture échoue (HDFS indisponible, disque plein), le traitement continue et
  seul un avertissement est journalisé. Un état corrompu repart à vide plutôt
  que de faire échouer le run.
- **Journal borné** aux 20 derniers runs par étape : le fichier ne grossit pas
  indéfiniment.
- **Migration automatique** depuis l'ancien `_ingested.json` : les lots déjà
  ingérés par une version antérieure ne sont pas retéléchargés.

## 7. Orchestration Airflow

| DAG | Fréquence | Déclenchement | Tâches |
|---|---|---|---|
| `dag_bronze_ingest` | quotidienne | planifiée ou manuelle | `meteofrance_ingest.py` (batch) → déclenche Silver |
| `dag_stream_ingest` | toutes les 10 min | planifiée | `streaming_ingest.py` (Kafka → Bronze) → déclenche Silver |
| `dag_silver_transform` | sur déclenchement | automatique (après Bronze) ou manuelle | `silver_transform.py` → déclenche Gold |
| `dag_gold_aggregate` | sur déclenchement | automatique (après Silver) ou manuelle | `gold_transform.py` → entraînement (si besoin) → `inference.py` → `genai_summary.py` |
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

## 10. Dashboard & restitution

- **`dashboard/app.py`** (Streamlit, auto-refresh 30 s) :
  - Vue d'ensemble : KPIs par ville, **carte de France**, évolution 30 jours, événements ;
  - Panneau **ML** (`ml_panel.py`) : prévisions vs réalité, erreurs, confiance ;
  - Panneau **IA** (`genai_panel.py`) : bulletin météo du jour.
  - Lecture HDFS **sans client HDFS** : WebHDFS REST (`dashboard/gold_reader.py`).
- **`ML(SARA)/`** : module ML & GenAI autonome de Sara (`train_model.py`, `predict.py`,
  `genai_bulletin.py`) avec son guide `README_ML_GENAI.md` et le compte-rendu
  `TP_DataLake_DataLakehouse.docx`.

---

## 11. Tests

```bash
make test          # dans un conteneur : aucun Python requis sur la machine
make test-local    # avec le Python de l'hote, si vous en avez un
```

**62 tests, sans Spark ni HDFS**. Ils s'exécutent en une à deux secondes.

- `tests/test_transform.py` : fonctions pures : validation de schéma,
  **déduplication**, classification des événements extrêmes, pente de tendance.
- `tests/test_medallion.py` : la couche Medallion de bout en bout, avec pandas :
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
  - **Contrôle** : le contrat de `make verify` : couverture des trois couches,
    tolérance d'un flux temps réel encore vide, règles de verdict
    (`OK` / `ABSENT` / `SANS _SUCCESS` / `VIDE`) et rendu du rapport ;
  - **Checkpoints** : schéma d'état résilient (fichier corrompu, valeurs
    aberrantes), pureté et idempotence des fonctions de marquage, journal borné,
    aller-retour sur disque, échec d'écriture non bloquant, et surtout le
    **scénario de reprise** : 10 lots, coupure après le 7ᵉ, seuls 3 rejoués ;
  - **Workflow** : `make all` complet et relançable : coupure pendant `pipeline`,
    reprise exacte à cette étape ; `FORCE=1` qui rejoue une étape pourtant terminée ;
    garde inerte hors conteneur ; inférence qui se retire proprement sans modèle ;
  - **Orchestration** : l'ordre de la chaîne, les répertoires HDFS créés, et
    surtout le **franchissement de la frontière Docker** : `namenode_url()` et
    la construction de la commande `airflow` doivent être correctes *depuis
    l'hôte comme depuis un conteneur*.

---

## 12. Réponses préparées aux questions du professeur

**Q : Comment construisez-vous un profil client sans historique d'achat ?**
→ Nous adaptons la question au domaine météo : la table Gold `climate_profile`
construit un **profil météo** par ville (normales de température, amplitude thermique,
jours de pluie, saison) à partir de l'historique Météo-France. Ces indicateurs
servent de features au modèle de prédiction,
l'équivalent météo d'un profil client.

**Q : Où est l'historique d'achat dans la météo ?**
→ L'analogue de l'historique d'achat est **l'historique climatique** : les archives
Météo-France (1950-2026) fournissent des décennies d'enregistrements quotidiens.
Les séries temporelles permettent d'identifier patterns saisonniers, tendances de
réchauffement et
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
│   ├── streaming_ingest.py         # Spark Structured Streaming Kafka → Bronze
│   ├── hdfs_utils.py               # client WebHDFS (quotas, _SUCCESS, uploads)
│   ├── silver_transform.py         # Bronze → Silver (validation, dédup, normalisation)
│   ├── gold_transform.py           # Silver → Gold (KPIs, tendances, extrêmes, profil)
│   ├── checkpoint.py               # reprise fine, unité de travail par unité
│   ├── verify_medallion.py         # contrôle automatique des 3 couches (make verify)
│   ├── pipeline_ctl.py             # pilote de make all, exécuté dans les conteneurs
│   ├── export_web.py               # exporte les tables Gold en JSON pour le site Next.js
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
│   ├── dag_bronze_ingest.py        # DAG 1 : ingestion batch → Bronze
│   ├── dag_stream_ingest.py        # DAG 2 : streaming Kafka → Bronze (découplé)
│   ├── dag_silver_transform.py     # DAG 3 : Bronze → Silver
│   ├── dag_gold_aggregate.py       # DAG 4 : Silver → Gold + ML + GenAI
│   └── dag_ml_retrain.py           # DAG 5 : réentraînement hebdo
├── ML(SARA)/                       # module ML & GenAI autonome de Sara (+ .docx)
├── tests/
│   ├── conftest.py                 # rend scripts/ importable
│   ├── test_transform.py           # fonctions pures (sans Spark)
│   └── test_medallion.py           # Bronze → Silver → Gold (sans Spark)
├── configs/
│   ├── spark-defaults.conf         # Zstd 22, overwrite dynamique, mémoire
│   └── requirements.txt            # dépendances Python
├── web/                            # interface Next.js (Vercel) : app/, components/, lib/
├── Makefile                        # automatisation complète (make all)
├── deploy.sh                       # déploiement en 1 commande
└── README.md                       # ce document
```

---

## 14. Commandes utiles

```bash
# Workflow complet (recommandé) : Docker Desktop + make suffisent
make all                      # Docker -> tests -> cluster -> Bronze -> Silver -> Gold -> ML -> vérification -> vitrine
make help                     # toutes les cibles, sans démarrer de conteneur

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

# Tests unitaires (dans un conteneur : aucun Python requis sur la machine)
make test

# Dashboard local (hors Docker)
streamlit run dashboard/app.py --server.port 8501
```

---

## 15. Interface web (Next.js sur Vercel)

`web/` est une application **Next.js 14** (App Router, export statique) qui
restitue *toutes* les tables Gold : KPIs, températures par ville, précipitations,
profil climatique mensuel, événements extrêmes, prédictions J+1 vs réel,
bulletin IA et tendances hebdomadaires.

### La contrainte qui dicte l'architecture

**Vercel n'a aucun accès réseau au cluster** : HDFS, Kafka et Spark tournent sur
`localhost`, derrière Docker. Un site déployé ne peut pas interroger
`localhost:9870`. Les données **voyagent donc avec le site** :

```
tables Gold ──(make export-web)──> web/public/data/*.json ──(build)──> site statique
```

- le site est **toujours en ligne**, même cluster éteint — y compris depuis le
  téléphone du jury ;
- il affiche un **instantané**, dont la date est visible dans l'en-tête : personne
  ne peut le confondre avec du temps réel ;
- rafraîchir = `make export-web` puis `git push` (Vercel redéploie).

### Déployer

```bash
make export-web     # tables Gold -> web/public/data/*.json
make web-install    # npm install, DANS un conteneur node (une fois)
make web-dev        # http://localhost:3000
make web-build      # vérifie le build avant de pousser
```

Comme le reste du projet, **Node tourne dans un conteneur** : rien à installer
sur la machine. `node_modules` vit dans un volume Docker nommé et non sur le
montage Windows — y écrire des dizaines de milliers de petits fichiers prend
des minutes au lieu de quelques secondes.

> Sous **PowerShell**, `&&` n'est pas un séparateur valide (avant PowerShell 7) :
> lancez les commandes une par une, ou séparez-les par `;`.

Sur vercel.com : **New Project** → importer le dépôt → **Root Directory : `web`**.
Aucune variable d'environnement : le site ne contacte aucun service à l'exécution.

### Choix de visualisation

La palette catégorielle est **validée par outil**, pas à l'œil : séparation
daltonisme et contraste vérifiés sur les deux surfaces (claire et sombre). Une
couleur par ville dans un **ordre fixe** — la couleur suit la ville, jamais son
rang, donc un filtre ne repeint jamais les séries restantes. Trois teintes passent
sous 3:1 sur fond clair : la règle de secours impose alors une **bascule tableau**,
présente sur les graphiques concernés. Les couleurs de statut (alerte / extrême)
sont réservées et toujours accompagnées d'une pastille **et** d'un libellé : la
couleur ne porte jamais le sens seule. Thème clair/sombre au choix du visiteur,
le bouton l'emportant sur le réglage système dans les deux sens.

## 16. Résilience : analyse des modes de défaillance

**Le principe.** Un cluster distribué aura des pannes : un datanode tombe, une
API expire, un conteneur est tué par l'OOM killer. L'objectif n'est donc pas
« aucune panne », ce serait inatteignable, mais aucune panne non traitée.
Chaque défaillance se répare seule, ou s'arrête bruyamment au bon endroit avec
un message qui nomme sa cause. Jamais de corruption silencieuse, jamais de faux vert.

### La panne la plus grave est celle qui ne fait pas d'erreur

Le pire défaut trouvé dans ce projet ne levait aucune exception. Le producteur
Kafka remplaçait toute mesure absente par `0.0` :

```python
if current.get(key) is None:
    current[key] = 0.0        # 0 °C : une valeur parfaitement plausible
```

Aucun log, aucun échec, aucun test rouge. Mais `0.0 °C` traversait Bronze, était
conservé par la déduplication Silver, tirait les moyennes Gold vers zéro, faussait
la détection d'événements extrêmes, et entraînait le modèle ML sur des valeurs
inventées. Une mesure absente reste désormais `NULL` : `avg()`, `min()`, `max()`
et l'entraînement l'ignorent correctement. Un test verrouille cette régression, et
`0.0` reste accepté quand c'est une vraie mesure (pas de pluie, vent nul).

### Matrice des défaillances traitées

| Classe | Défaillance | Détection | Réponse |
|---|---|---|---|
| **Données** | Mesure absente | - | `NULL`, jamais `0.0` |
| | Valeur physiquement impossible | bornes `MEASUREMENT_BOUNDS` | mise à `NULL` + log |
| | Relevé sans aucune mesure | `has_usable_measurement` | non publié |
| | Champs vides Météo-France | `mf_parse_number` | `NULL`, jamais `0` |
| | Colonne obligatoire manquante | `validate_required_columns` | **échec explicite** |
| | Doublons (checkpoint Kafka perdu) | dédup `(station_id, timestamp)` | éliminés en Silver |
| **Réseau** | `meteo.data.gouv.fr` injoignable | code retour | repli `--synthetic` |
| | Open-Meteo en erreur | 3 tentatives + backoff | ville sautée, cycle poursuivi |
| | Broker Kafka pas encore prêt | `connect_producer` | 12 tentatives × 5 s |
| | Ollama absent | `genai_summary` | bulletin de repli (règles) |
| **Ressources** | Quota Bronze atteint | `quota_reached` | producteur et DAG s'arrêtent |
| | HDFS muet pour un checkpoint | exception capturée | avertissement, le traitement continue |
| | Tâche bloquée sans fin | `execution_timeout` | tuée puis relancée (`retries=2`) |
| **Démarrage** | Service dépendant démarré trop tôt | `depends_on: condition: service_healthy` | attend la **disponibilité**, pas le simple démarrage |
| | Sonde basée sur un outil JVM | — | sondes TCP (`/dev/tcp`), en millisecondes |
| **Cycle de vie** | Crash transitoire d'un service | Docker | `restart: unless-stopped` (sauf one-shots) |
| | Aucun modèle sous `/models` | `model_available` | inférence sautée, code 0 |
| | Interruption en cours d'ingestion | checkpoints | reprise à l'unité près |
| | `make all` interrompu | checkpoints `workflow` | reprend à l'étape suivante |
| **Concurrence** | Deux exécutions du même DAG | `max_active_runs=1` | la seconde attend |
| | Réécriture d'une partition | overwrite dynamique + `_SUCCESS` | idempotent |
| **Contrôle** | Datalake vide mais « réussi » | `verify_medallion` | **sortie non nulle** |
| | Étape ML silencieusement sautée | `verify --with-ml` | **sortie non nulle** |

### Risques résiduels, assumés et non masqués

1. **Connecteur Spark-Kafka résolu depuis Maven Central au premier lancement.**
   Sur un réseau filtré, la tâche de streaming échoue. Le DAG Bronze survit et la
   source batch continue de l'alimenter, mais le flux temps réel reste vide.
2. **`foreachBatch` est at-least-once.** Un micro-batch rejoué réécrit ses JSON en
   Bronze. La déduplication Silver les absorbe ; Bronze peut contenir des doublons,
   ce qui est conforme à sa vocation de format brut.
3. **Pas de reprise au niveau ligne** dans les jobs Spark : l'unité de reprise est
   la partition `dt`. Un job tué à mi-partition la recalcule entièrement.
4. **`_SUCCESS` et données ne sont pas écrits atomiquement.** Un crash entre les
   deux laisse une partition écrite mais non marquée : elle sera recalculée
   (overwrite dynamique), coûteux mais jamais incorrect.
5. **Le mode `--synthetic` produit des données vraisemblables, pas réelles.** Il
   sert à démontrer la chaîne quand le réseau bloque, et le dit dans ses logs.

### Ce qui est vérifié automatiquement

Les garde-fous ci-dessus ne sont pas que des intentions : 62 tests les verrouillent,
dont la non-régression du `0.0`, le rejet des valeurs impossibles, la présence
d'`execution_timeout` et de `max_active_runs=1` dans chaque DAG, les politiques de
redémarrage (et leur absence sur les one-shots), et le scénario de reprise après
interruption.

## 17. Notes & limites

- **Météo-France** : les valeurs manquantes sont des **champs vides** (pas de
  sentinelle numérique) ; les unités sont déjà en °C / mm / m·s⁻¹.
  Si `meteo.data.gouv.fr` est filtré par le réseau, `--synthetic` produit des lots
  au schéma identique et la chaîne complète reste démontrable.
- Le **partitionnement batch** est fait par lot d'ingestion (année/mois) : c'est une
  convention de dépôt (le sujet n'impose pas le découpage du contenu) ; chaque
  fichier reste brut et n'est stocké qu'une fois. La fenêtre temporelle est appliquée
  en Silver (`SILVER_START_DATE` / `SILVER_END_DATE`).
- **Configuration Hadoop (piège classique)** : l'image officielle `apache/hadoop`
  convertit ses variables d'environnement en fichiers XML via `/opt/envtoconf.py`.
  Ce script **découpe le nom de la variable sur `_` ou `.`** et traite le 2ᵉ segment
  comme l'extension du fichier cible. La bonne forme est donc
  `CORE-SITE.XML_<propriété>` / `HDFS-SITE.XML_<propriété>`.

  Le préfixe `CORE_CONF_` / `HDFS_CONF_`, omniprésent dans les tutoriels, mais propre
  aux images `bde2020/hadoop-*`, donne un 2ᵉ segment `conf`, qui figure dans les
  formats connus du script. Celui-ci appelle alors `transformation.to_conf()`, dont le
  code itère `for key, val in props` sur un **dictionnaire** : le namenode meurt avant
  même de démarrer, sur `ValueError: too many values to unpack`, et Compose signale
  seulement `dependency failed to start: container namenode exited (1)`.

  **Règle à retenir** : le 2ᵉ segment du nom d'une variable passée aux conteneurs HDFS
  ne doit jamais valoir `conf`, `cfg`, `env`, `sh`, `yaml`, `yml` ni `properties`.
  Les emplacements `dfs.namenode.name.dir` / `dfs.datanode.data.dir` sont par ailleurs
  déclarés explicitement (sinon HDFS écrit dans `/tmp` et rien ne survit au redémarrage,
  malgré les volumes nommés), et les deux démons tournent en `root`, propriétaire de ces
  volumes. Le *healthcheck* est une sonde TCP bash : l'image ne fournit pas `curl`.
- **Entrypoint Airflow** : l'entrypoint de l'image officielle n'accepte que `bash`
  ou `python` comme premier argument ; **tout le reste est passé à la CLI `airflow`**.
  Un `command: ["/bin/bash", "-c", ...]` devient donc
  `airflow /bin/bash -c ...` → `invalid choice: '/bin/bash'`, sortie 2, et Compose
  n'affiche que `service "airflow-init" didn't complete successfully: exit 2`.
  Écrire `bash` (sans chemin). À noter : `docker compose exec` **ne passe pas** par
  l'entrypoint. Les commandes du `Makefile` (`exec ... python3 ...`) ne sont pas concernées.
- **Healthcheck du namenode** : `dfs.namenode.rpc-address: namenode:9000` lie le RPC à
  l'IP résolue du nom d'hôte (`eth0`), **pas à la loopback** : une sonde sur
  `localhost:9000` échoue même namenode parfaitement démarré. D'où les
  `*-bind-host: 0.0.0.0` et une sonde qui vise `127.0.0.1:9870` (WebHDFS) plus le RPC
  via le nom d'hôte.
- **Images Docker** : l'image officielle `apache/airflow` **refuse `pip install` en root** ;
  le `Dockerfile` repasse sur l'utilisateur `airflow` avant d'installer les paquets.
  L'image `apache/spark` ne fournit pas de lien `python` : les commandes du `Makefile`
  utilisent `python3`, présent dans les trois images du projet.
- **Inférence sans modèle** : sur un cluster neuf, `/models` est vide.
  `ml/inference.py` le détecte et **se retire avec un code 0** au lieu de lever :
  sans cela la tâche `inference_ml` échouait, `bulletin_genai` était sauté et tout
  le DAG Gold passait en `failed`. Même philosophie que le bulletin IA, qui a
  toujours eu un fallback pour ne jamais casser le pipeline.
- **Variables d'environnement ignorées par l'image** : `SPARK_MODE`,
  `SPARK_MASTER_URL`, `SPARK_WORKER_CORES`… sont des conventions de l'image
  **bitnami/spark**. L'image **officielle `apache/spark`** les ignore purement et
  simplement : sans `command:` explicite, **aucun processus Master ne démarrait** et
  rien n'écoutait sur 7077 — alors que le conteneur affichait `Started`. Tous les
  jobs `spark-submit` (Silver, Gold, ML, streaming) auraient échoué à se connecter
  au cluster. Le Master et le Worker sont désormais lancés explicitement via
  `spark-class`, et un test interdit le retour des variables bitnami.
- **Ordonnancement au démarrage** : `depends_on: [x]` (forme courte) n'attend que
  le *démarrage* du conteneur, jamais sa disponibilité. Kafka sortait ainsi au bout
  de ses 18 s par défaut (`Timed out waiting for connection while in state:
  CONNECTING`) parce que Zookeeper n'acceptait pas encore de connexion. Toutes les
  dépendances utilisent désormais la forme longue avec `condition:`, et **un test
  interdit le retour de la forme courte**.
- **Sondes de disponibilité** : jamais d'outil JVM (`kafka-topics`, `hdfs`). Leur
  seul démarrage dépasse souvent le `timeout` de la sonde, qui échoue alors même
  service parfaitement sain. Toutes les sondes du projet sont des tests TCP bash,
  et un test le verrouille.
- **Ollama** est optionnel (profil `genai`) : sans lui, le bulletin fallback
  (règles) est généré et le DAG ne casse pas.
- Le connecteur **spark-sql-kafka** (`--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1`)
  est téléchargé depuis Maven Central au premier lancement du job streaming
  (le driver Spark a besoin d'accéder à internet une fois).
- En cas de perte du checkpoint Kafka, des doublons peuvent transiter par Bronze ;
  la dédup Silver (`station_id + timestamp`) les élimine.
