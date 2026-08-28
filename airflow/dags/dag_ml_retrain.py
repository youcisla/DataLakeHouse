# -*- coding: utf-8 -*-
"""
dag_ml_retrain.py : DAG 4, réentraînement hebdomadaire du modèle ML
===================================================================
1. feature_engineering.py : construction des features (lags J-1/J-2/J-7,
   moyennes mobiles 3j/7j, encodage ville/saison, normalisation) depuis Silver.
2. train_model.py : entraînement XGBoost Regressor (prédiction température
   J+1), GridSearchCV, split temporel 70/15/15, sauvegarde
   /models/temperature_predictor_v{N}/ (version incrémentée) + métriques JSON.

Planification : chaque lundi à 03h00. Relançable à tout moment.

Auteur : Youcef, équipe DataLake Météo
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DEFAULT_ARGS = {
    "owner": "youcef",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=3),
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="dag_ml_retrain",
    description="Réentraînement hebdomadaire du modèle de prédiction (XGBoost)",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 3 * * 1",   # chaque lundi à 03h00
    catchup=False,
    max_active_runs=1,
    tags=["ml", "retrain"],
) as dag:

    t_features = SparkSubmitOperator(
        task_id="feature_engineering",
        application="/opt/project/ml/feature_engineering.py",
        conn_id="spark_default",
        deploy_mode="cluster",
        application_args=["--days", "730", "--max-locations", "200"],
        verbose=False,
        doc_md="Construit le jeu de features depuis Silver "
               "(/features/meteo/features.parquet).",
    )

    t_train = SparkSubmitOperator(
        task_id="entrainement_xgboost",
        application="/opt/project/ml/train_model.py",
        conn_id="spark_default",
        deploy_mode="cluster",
        verbose=False,
        doc_md="Entraîne XGBoost (GridSearchCV, split 70/15/15) et sauvegarde "
               "/models/temperature_predictor_v{N}/.",
    )

    t_features >> t_train

if __name__ == "__main__":
    dag.test()
