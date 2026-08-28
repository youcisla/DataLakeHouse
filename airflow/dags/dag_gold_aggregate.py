# -*- coding: utf-8 -*-
"""
dag_gold_aggregate.py : DAG 3, agrégation Silver → Gold + ML + GenAI
=====================================================================
Job Spark qui calcule depuis Silver :
  - daily_aggregates  : KPIs journaliers par ville (temp moy/min/max, précip, vent...)
  - weekly_trends     : tendances hebdomadaires + écart à la semaine précédente
  - extreme_events    : détection d'événements extrêmes (canicule, fortes pluies...)
  - climate_profile   : profil météo mensuel par ville (normales, amplitude,
                        jours de pluie, saison), l'équivalent d'un profil client

Puis, si aucun modèle n'existe encore sous /models :
  - entraîne XGBoost (feature_engineering + train_model), versionné.

Puis, dans tous les cas :
  - ml/inference.py     : prédictions température J+1 -> ml_predictions
  - ml/genai_summary.py : bulletin météo (Ollama, fallback si absent) -> ai_insights

Le réentraînement périodique reste confié à dag_ml_retrain (hebdomadaire).

Idempotence : overwrite dynamique des partitions + marqueurs _SUCCESS.
Déclenché après dag_silver_transform, relançable manuellement.

Auteur : Youcef, équipe DataLake Météo
"""

from __future__ import annotations

import os
import re
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DEFAULT_ARGS = {
    "owner": "youcef",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(hours=2),
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="dag_gold_aggregate",
    description="Silver -> Gold : agrégations, événements extrêmes, ML, bulletin IA",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,          # déclenché par dag_silver_transform ou manuellement
    catchup=False,
    max_active_runs=1,
    tags=["gold", "agregation", "ml", "genai"],
) as dag:

    def _modele_necessaire(**context):
        """Saute l'entraînement si un modèle existe déjà sous /models."""
        sys.path.insert(0, "/opt/project/scripts")
        import hdfs_utils

        if not hdfs_utils.hdfs_exists("/models"):
            return
        for name in hdfs_utils.hdfs_list("/models"):
            if re.match(r"temperature_predictor_v\d+$", name):
                raise AirflowSkipException("Un modèle existe déjà : entraînement sauté.")

    t_gold = SparkSubmitOperator(
        task_id="agregation_silver_vers_gold",
        application="/opt/project/scripts/gold_transform.py",
        conn_id="spark_default",
        application_args=[
            "--start-date", os.environ.get("SILVER_START_DATE", "2022-01-01"),
            "--end-date", os.environ.get("SILVER_END_DATE", "2025-12-31"),
            "--only-new",
        ],
        verbose=False,
        doc_md="Calcule daily_aggregates, weekly_trends, extreme_events et climate_profile "
               "(overwrite dynamique + _SUCCESS). --only-new saute les partitions dt "
               "déjà calculées.",
    )

    t_check_modele = PythonOperator(
        task_id="verifier_modele",
        python_callable=_modele_necessaire,
        doc_md="Saute l'entraînement si /models contient déjà une version.",
    )

    t_train_features = SparkSubmitOperator(
        task_id="entrainement_features",
        application="/opt/project/ml/feature_engineering.py",
        conn_id="spark_default",
        application_args=["--days", "730", "--max-locations", "200"],
        verbose=False,
        doc_md="Construit les features depuis Silver (premier entraînement).",
    )

    t_train_modele = SparkSubmitOperator(
        task_id="entrainement_modele",
        application="/opt/project/ml/train_model.py",
        conn_id="spark_default",
        verbose=False,
        doc_md="Entraîne XGBoost et versionne le modèle sous /models.",
    )

    t_inference = SparkSubmitOperator(
        task_id="inference_ml",
        application="/opt/project/ml/inference.py",
        conn_id="spark_default",
        application_args=["--days", "30"],
        verbose=False,
        trigger_rule="none_failed",
        doc_md="Charge le dernier modèle et écrit /gold/meteo/ml_predictions.",
    )

    t_genai = SparkSubmitOperator(
        task_id="bulletin_genai",
        application="/opt/project/ml/genai_summary.py",
        conn_id="spark_default",
        verbose=False,
        doc_md="Génère le bulletin météo (Ollama, fallback si absent) -> ai_insights.",
    )

    t_gold >> t_check_modele >> t_train_features >> t_train_modele >> t_inference >> t_genai

if __name__ == "__main__":
    dag.test()
