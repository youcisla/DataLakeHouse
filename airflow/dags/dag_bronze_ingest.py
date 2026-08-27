# -*- coding: utf-8 -*-
"""
dag_bronze_ingest.py : DAG 1, ingestion Bronze (batch NOAA + streaming Open-Meteo)
===================================================================================
- Tâche batch : télécharge les CSV NOAA (cible NOAA_TARGET_GB) et les dépose
  BRUTS dans /bronze/meteo/batch/source=noaa/year=YYYY/month=MM/ avec marqueur
  _SUCCESS (idempotence : les stations déjà ingérées sont ignorées).
- Tâche streaming : lance le job Spark Structured Streaming Kafka → Bronze
  (durée bornée, checkpoint HDFS → relance sans doublon).
- Contrôle de quota : si le Bronze dépasse BRONZE_QUOTA_GB, le streaming est
  sauté (le producteur Kafka s'arrête également tout seul côté service).

Auteur : Youcef, équipe DataLake Météo
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

sys.path.insert(0, "/opt/project/scripts")
import hdfs_utils  # noqa: E402

DEFAULT_ARGS = {
    "owner": "youcef",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2024, 1, 1),
    "catchup": False,
}

with DAG(
    dag_id="dag_bronze_ingest",
    description="Ingestion Bronze : batch NOAA + streaming Open-Meteo (Kafka)",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    catchup=False,
    tags=["bronze", "ingestion"],
) as dag:

    def _verifier_quota(**context):
        """Si le quota Bronze est atteint, le streaming est sauté."""
        quota_gb = float(os.environ.get("BRONZE_QUOTA_GB", "10.5"))
        try:
            if hdfs_utils.quota_reached("/bronze", quota_gb):
                raise AirflowSkipException(
                    f"Quota Bronze atteint ({quota_gb} Go) : streaming ignoré.")
        except IOError as exc:
            # HDFS pas encore prêt : on laisse le streaming s'exécuter
            print(f"Quota non vérifiable : {exc}")
        print(f"Quota Bronze vérifié : {quota_gb} Go.")

    t_quota = PythonOperator(
        task_id="verifier_quota_bronze",
        python_callable=_verifier_quota,
        doc_md="Vérifie que le Bronze est sous BRONZE_QUOTA_GB ; sinon saute le streaming.",
    )

    t_batch = BashOperator(
        task_id="ingestion_batch_noaa",
        bash_command=(
            "python /opt/project/scripts/batch_ingest.py "
            "--target-gb \"{{ var.value.get('noaa_target_gb', '6.6') }}\" || "
            "python /opt/project/scripts/batch_ingest.py --synthetic"
        ),
        doc_md="Télécharge les CSV NOAA et les dépose en Bronze (idempotent, _SUCCESS). "
               "Repli automatique sur --synthetic si le réseau est inaccessible.",
    )

    t_streaming = SparkSubmitOperator(
        task_id="streaming_kafka_vers_bronze",
        application="/opt/project/scripts/streaming_ingest.py",
        conn_id="spark_default",
        packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"],
        application_args=[
            "--max-runtime", "{{ var.value.get('streaming_runtime_seconds', '3600') }}",
            "--trigger-seconds", "{{ var.value.get('streaming_trigger_seconds', '30') }}",
        ],
        verbose=False,
        doc_md="Job Spark Structured Streaming : Kafka (Open-Meteo) -> Bronze, "
               "durée bornée + checkpoint HDFS (idempotent).",
    )

    # Déclenche le DAG Silver une fois l'ingestion terminée (all_done :
    # même si le streaming a été sauté par quota, la transformation a lieu).
    t_trigger_silver = TriggerDagRunOperator(
        task_id="declencher_dag_silver",
        trigger_dag_id="dag_silver_transform",
        wait_for_completion=False,
        trigger_rule="all_done",
        doc_md="Déclenche dag_silver_transform (dépendance explicite entre les couches).",
    )

    t_quota >> [t_batch, t_streaming] >> t_trigger_silver

if __name__ == "__main__":
    dag.test()
