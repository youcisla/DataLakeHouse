# Module ML & GenAI - Data Lakehouse Meteo

Ce module consomme une table Silver/Gold contenant les colonnes `date`, `temperature`, `humidite`, `vent` et `precipitation`.

## Pipeline

```text
Silver/Gold Parquet sur HDFS
          |
          v
train_model.py  --> weather_xgboost_model.json
                --> weather_model_metrics.json
          |
          v
predict.py      --> weather_prediction.json
                --> bulletin meteo
          |
          v
 genai_bulletin.py --> weather_bulletin.txt
```

Le modele predit la temperature du lendemain a partir de la temperature actuelle, des conditions meteo, des retards et d'une moyenne mobile. Le split train/test est chronologique: aucune donnee future n'est melangee dans l'entrainement.

## Installation

```powershell
.\.venv-1\Scripts\python.exe -m pip install pandas numpy scikit-learn xgboost pyarrow
```

## Entrainement depuis Gold

```powershell
.\.venv-1\Scripts\python.exe train_model.py `
  --data "hdfs://namenode:9000/lake/gold/weather" `
  --model weather_xgboost_model.json `
  --metrics weather_model_metrics.json
```

Les chemins locaux (`gold_weather.parquet`), les dossiers Parquet partitionnes (`gold/weather/`) et les chemins HDFS sont acceptes. Pour HDFS, l'environnement Spark/Hadoop et les connecteurs necessaires doivent etre disponibles sur la machine qui execute le job.

Le chargement controle aussi le schema, les dates dupliquees et les colonnes numeriques invalides avant de construire les features.

Le fichier de metriques permet de montrer la qualite du modele et sa gouvernance: RMSE, MAE, R2, baseline "temperature d'hier", indicateur `beats_baseline` et importance des variables.

## Inférence

Le fichier Parquet doit contenir au moins les trois dernieres observations afin de calculer les retards et la moyenne mobile 3 jours.

```powershell
.\.venv-1\Scripts\python.exe predict.py `
  --data "hdfs://namenode:9000/lake/gold/weather" `
  --model weather_xgboost_model.json `
  --metrics weather_model_metrics.json `
  --output weather_prediction.json
```

`weather_prediction.json` est une sortie contractuelle facile a lire depuis un dashboard ou a deposer dans Gold. Elle contient la date cible, la prediction, un intervalle indicatif base sur le RMSE, les dernieres conditions et le bulletin.

L'explication locale XGBoost indique la variable qui a le plus contribue a la prediction courante. Cela rend le resultat lisible pour un utilisateur metier et evite de presenter le modele comme une boite noire.

## Bonus GenAI

Mode fiable sans service externe:

```powershell
.\.venv-1\Scripts\python.exe genai_bulletin.py
```

Mode GenAI avec un LLM local Ollama:

```powershell
ollama run mistral
.\.venv-1\Scripts\python.exe genai_bulletin.py --use-ollama --model mistral
```

Le prompt transmet uniquement le JSON de prediction au modele local. Si Ollama est arrete, le script revient automatiquement au bulletin deterministe: le DAG ne tombe pas pour une raison de disponibilite du LLM.

## Idee de DAG Airflow

- `ingestion_batch`: depot Bronze et marker `_SUCCESS`.
- `silver_weather`: validation, deduplication et normalisation.
- `gold_weather`: KPIs et export Parquet.
- `ml_weather`: `train_model.py` apres Gold, avec les artefacts du modele dans un dossier versionne.
- `weather_inference`: `predict.py`, puis `genai_bulletin.py`.

Pour la soutenance, montrer le RMSE du modele face a la baseline, le JSON de metriques, la prediction et le bulletin GenAI constitue une chaine complete et reproductible.
