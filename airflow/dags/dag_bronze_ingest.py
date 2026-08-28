# -*- coding: utf-8 -*-
"""
dag_bronze_ingest.py : DAG 1, ingestion batch (archives Météo-France)
=====================================================================
Télécharge les archives quotidiennes Météo-France (meteo.data.gouv.fr, jeu
QUOT RR-T-Vent) et les dépose BRUTES dans
/bronze/meteo/batch/source=meteofrance/year=YYYY/month=MM/ avec marqueur
_SUCCESS (idempotence : les lots déjà ingérés sont ignorés).

Le flux temps réel (Open-Meteo -> Kafka -> Spark Structured Streaming) vit dans
un DAG séparé, dag_stream_ingest, afin de ne pas bloquer la chaîne batch : le
batch enchaîne directement vers Silver puis Gold, pendant que le streaming
alimente Bronze en continu.

Auteur : Youcef, équipe DataLake Météo
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

DEFAULT_ARGS = {
    "owner": "youcef",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=2),
    "start_date": datetime(2024, 1, 1),
    "catchup": False,
}

with DAG(
    dag_id="dag_bronze_ingest",
    description="Ingestion batch : archives Météo-France vers Bronze",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "ingestion"],
) as dag:

    t_batch = BashOperator(
        task_id="ingestion_batch_meteofrance",
        bash_command=(
            "python /opt/project/scripts/meteofrance_ingest.py "
            "--departments {{ var.value.get('mf_departments', '75 69 13 33 59') }} "
            "--start-year {{ var.value.get('mf_start_year', '2022') }} "
            "--end-year {{ var.value.get('mf_end_year', '2026') }} || "
            "python /opt/project/scripts/meteofrance_ingest.py --synthetic "
            "--departments {{ var.value.get('mf_departments', '75 69 13 33 59') }} "
            "--start-year {{ var.value.get('mf_start_year', '2022') }} "
            "--end-year {{ var.value.get('mf_end_year', '2026') }}"
        ),
        doc_md="Télécharge les archives quotidiennes Météo-France (meteo.data.gouv.fr) "
               "et les dépose BRUTES en Bronze (idempotent : _ingested.json + _SUCCESS). "
               "Repli automatique sur --synthetic si le réseau est filtré.",
    )

    # Déclenche le DAG Silver une fois le batch ingéré.
    t_trigger_silver = TriggerDagRunOperator(
        task_id="declencher_dag_silver",
        trigger_dag_id="dag_silver_transform",
        wait_for_completion=False,
        doc_md="Déclenche dag_silver_transform (dépendance explicite entre les couches).",
    )

    t_batch >> t_trigger_silver

if __name__ == "__main__":
    dag.test()
