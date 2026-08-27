# -*- coding: utf-8 -*-
"""
dag_silver_transform.py : DAG 2, transformation Bronze → Silver
================================================================
Job Spark qui :
  1. lit les partitions Bronze ayant un marqueur _SUCCESS (batch NOAA + stream Open-Meteo),
  2. valide le schéma, déduplique (station_id + timestamp) et normalise
     vers le schéma Silver unifié,
  3. calcule les indicateurs (moyennes mobiles 3j/7j, écart-type 7j, anomalie),
  4. écrit en Parquet Zstd partitionné par dt avec OVERWRITE DYNAMIQUE
     (relancer le DAG ne duplique jamais : seules les partitions présentes
     dans l'input sont réécrites) + marqueurs _SUCCESS par partition.

Déclenché automatiquement après dag_bronze_ingest (TriggerDagRunOperator),
relançable manuellement sans risque (idempotence).

Auteur : Youcef, équipe DataLake Météo
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

DEFAULT_ARGS = {
    "owner": "youcef",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="dag_silver_transform",
    description="Bronze -> Silver : validation, dédup, normalisation, Parquet Zstd",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,          # déclenché par dag_bronze_ingest ou manuellement
    catchup=False,
    tags=["silver", "transformation"],
) as dag:

    t_silver = SparkSubmitOperator(
        task_id="transformation_bronze_vers_silver",
        application="/opt/project/scripts/silver_transform.py",
        conn_id="spark_default",
        application_args=[
            "--start-date", os.environ.get("SILVER_START_DATE", "2022-01-01"),
            "--end-date", os.environ.get("SILVER_END_DATE", "2025-12-31"),
            "--only-new",
        ],
        verbose=False,
        doc_md="Lecture Bronze (partitions _SUCCESS) -> Silver Parquet Zstd "
               "partitionné dt, overwrite dynamique + _SUCCESS.",
    )

    # Déclenche le DAG Gold une fois Silver terminé (dépendance explicite).
    t_trigger_gold = TriggerDagRunOperator(
        task_id="declencher_dag_gold",
        trigger_dag_id="dag_gold_aggregate",
        wait_for_completion=False,
        trigger_rule="all_success",
        doc_md="Déclenche dag_gold_aggregate (dépendance explicite entre les couches).",
    )

    t_silver >> t_trigger_gold

if __name__ == "__main__":
    dag.test()
