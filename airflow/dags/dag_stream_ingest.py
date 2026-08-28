# -*- coding: utf-8 -*-
"""
dag_stream_ingest.py : DAG streaming, Kafka (Open-Meteo) vers Bronze
====================================================================
Consomme le topic Kafka "meteo-stream" en continu via Spark Structured
Streaming et écrit les JSON bruts dans
/bronze/meteo/stream/source=openmeteo/year=.../month=.../day=.../hour=.../
avec marqueur _SUCCESS par heure (checkpoint HDFS : relance sans doublon).

Planifié toutes les 10 minutes ; chaque exécution consomme pendant
STREAMING_RUNTIME_SECONDS (5 min par défaut) puis déclenche Silver, qui
transforme les nouvelles partitions (--only-new). La chaîne batch
(dag_bronze_ingest) est indépendante : elle n'attend pas ce DAG.

Auteur : Youcef, équipe DataLake Météo
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

DEFAULT_ARGS = {
    "owner": "youcef",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    # Le job stream lui-même tourne ~5 min (STREAMING_RUNTIME_SECONDS) ; on laisse
    # une large marge pour le démarrage de spark-submit et le premier
    # téléchargement du connecteur Kafka depuis Maven.
    "execution_timeout": timedelta(minutes=20),
    "start_date": datetime(2024, 1, 1),
    "catchup": False,
}

with DAG(
    dag_id="dag_stream_ingest",
    description="Streaming temps réel : Kafka (Open-Meteo) vers Bronze",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/10 * * * *",   # toutes les 10 minutes
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "streaming"],
) as dag:

    t_streaming = SparkSubmitOperator(
        task_id="streaming_kafka_vers_bronze",
        application="/opt/project/scripts/streaming_ingest.py",
        conn_id="spark_default",
        deploy_mode="cluster",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        application_args=[
            "--max-runtime", "{{ var.value.get('streaming_runtime_seconds', '300') }}",
            "--trigger-seconds", "{{ var.value.get('streaming_trigger_seconds', '30') }}",
        ],
        verbose=False,
        doc_md="Spark Structured Streaming : Kafka -> Bronze (durée bornée + checkpoint).",
    )

    # Après chaque fenêtre de streaming, on pousse les nouvelles partitions
    # Bronze vers Silver (qui déclenchera Gold).
    t_trigger_silver = TriggerDagRunOperator(
        task_id="declencher_dag_silver",
        trigger_dag_id="dag_silver_transform",
        wait_for_completion=False,
        doc_md="Déclenche Silver pour transformer les nouvelles partitions stream.",
    )

    t_streaming >> t_trigger_silver

if __name__ == "__main__":
    dag.test()
