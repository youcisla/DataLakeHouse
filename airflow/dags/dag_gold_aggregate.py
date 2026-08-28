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
Puis enchaîne :
  - ml/inference.py   : prédictions température J+1 (modèle XGBoost) -> ml_predictions
  - ml/genai_summary.py : bulletin météo généré par LLM (Ollama, fallback si absent)

Idempotence : overwrite dynamique des partitions + marqueurs _SUCCESS.
Déclenché après dag_silver_transform, relançable manuellement.

Auteur : Youcef, équipe DataLake Météo
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DEFAULT_ARGS = {
    "owner": "youcef",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="dag_gold_aggregate",
    description="Silver -> Gold : agrégations, événements extrêmes, ML, bulletin IA",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,          # déclenché par dag_silver_transform ou manuellement
    catchup=False,
    tags=["gold", "agregation", "ml", "genai"],
) as dag:

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
               "deja calculees : un DAG relance ne recalcule pas tout le Gold.",
    )

    t_inference = SparkSubmitOperator(
        task_id="inference_ml",
        application="/opt/project/ml/inference.py",
        conn_id="spark_default",
        application_args=["--days", "30"],
        verbose=False,
        doc_md="Charge le dernier modèle XGBoost depuis /models, prédit la "
               "température J+1 et écrit /gold/meteo/ml_predictions.",
    )

    t_genai = SparkSubmitOperator(
        task_id="bulletin_genai",
        application="/opt/project/ml/genai_summary.py",
        conn_id="spark_default",
        verbose=False,
        doc_md="Génère le bulletin météo via Ollama (fallback template si "
               "indisponible) -> /gold/meteo/ai_insights.",
    )

    t_gold >> t_inference >> t_genai

if __name__ == "__main__":
    dag.test()
