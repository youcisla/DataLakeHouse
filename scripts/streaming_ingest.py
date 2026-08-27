# -*- coding: utf-8 -*-
"""
streaming_ingest.py : consommateur Spark Structured Streaming (Kafka → Bronze)
==============================================================================
Consomme en continu le topic Kafka "meteo-stream" (produit par
kafka_producer.py) et écrit les messages JSON BRUTS dans Bronze :

    /bronze/meteo/stream/source=openmeteo/year=YYYY/month=MM/day=DD/hour=HH/

Chaque répertoire-heure reçoit un marqueur _SUCCESS dès qu'un micro-batch
l'a alimenté (créé une seule fois, jamais écrasé).

IDEMPOTENCE : le checkpoint HDFS (/checkpoints/kafka_to_bronze) mémorise
les offsets consommés ; une relance reprend exactement là où le job s'est
arrêté (exactement-une-fois côté lecture). En cas de perte du checkpoint,
les doublons éventuels sont éliminés plus tard dans Silver (dédup sur
station_id + timestamp).

Le job s'arrête proprement après --max-runtime secondes (ou sur SIGTERM),
ce qui permet à Airflow de le relancer régulièrement.

Auteur : Youcef, équipe DataLake Météo
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__))))
import hdfs_utils  # noqa: E402

logger = logging.getLogger("streaming_ingest")


def hdfs_base() -> str:
    """URL de base HDFS (ex: hdfs://namenode:9000) depuis l'environnement."""
    namenode = os.environ.get("HDFS_NAMENODE", "namenode")
    rpc_port = os.environ.get("HDFS_RPC_PORT", "9000")
    return f"hdfs://{namenode}:{rpc_port}"

# Schéma du message Kafka (contrat Open-Meteo)
MESSAGE_SCHEMA = {
    "type": "struct",
    "fields": [
        {"name": "city", "type": "string", "nullable": True},
        {"name": "latitude", "type": "double", "nullable": True},
        {"name": "longitude", "type": "double", "nullable": True},
        {"name": "timestamp", "type": "string", "nullable": True},
        {"name": "temperature", "type": "double", "nullable": True},
        {"name": "windspeed", "type": "double", "nullable": True},
        {"name": "winddirection", "type": "double", "nullable": True},
        {"name": "weathercode", "type": "long", "nullable": True},
        {"name": "precipitation", "type": "double", "nullable": True},
        {"name": "source", "type": "string", "nullable": True},
    ],
}


def build_spark_session(app_name: str):
    """Crée la SparkSession (import PySpark ici, pour les tests)."""
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "spark://spark-master:7077"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.schemaInference", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("city", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("windspeed", DoubleType(), True),
        StructField("winddirection", DoubleType(), True),
        StructField("weathercode", LongType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("source", StringType(), True),
    ])
    return spark, schema


def hour_dir_for(hour_str: str) -> str:
    """'2026-08-27 14' -> /bronze/meteo/stream/source=openmeteo/year=2026/month=08/day=27/hour=14"""
    date_part, hour = hour_str.split(" ")
    y, m, d = date_part.split("-")
    return (f"/bronze/meteo/stream/source=openmeteo/year={y}/month={m}/"
            f"day={d}/hour={hour}")


def write_batch(df, epoch_id: int, spark) -> None:
    """
    foreachBatch : écrit les JSON bruts (format texte, une ligne = un message)
    dans le répertoire-heure correspondant, puis dépose le marqueur _SUCCESS
    (créé une seule fois par répertoire).
    """
    from pyspark.sql import functions as F

    parsed = (df
              .select(F.from_json(F.col("json_value"), spark._meteo_schema).alias("d"),
                      F.col("json_value"))
              .withColumn("hour", F.date_format(F.to_timestamp(F.col("d.timestamp")), "yyyy-MM-dd HH"))
              .filter(F.col("hour").isNotNull()))

    hours = [row["hour"] for row in parsed.select("hour").distinct().collect()]
    if not hours:
        logger.info("Batch %d : aucun message valide.", epoch_id)
        return

    for hour in hours:
        target_dir = hour_dir_for(hour)
        (parsed
         .filter(F.col("hour") == hour)
         .select("json_value")
         .write
         .mode("append")
         .text(f"{hdfs_base()}{target_dir}"))
        hdfs_utils.write_success(target_dir)
        logger.info("Batch %d : heure %s -> %s (marqueur OK)", epoch_id, hour, target_dir)

    logger.info("Batch %d : %d lignes écrites (%d heures).",
                epoch_id, parsed.count(), len(hours))


def run_streaming(args: argparse.Namespace) -> int:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.environ.get("METEO_TOPIC", "meteo-stream")
    checkpoint = os.environ.get("STREAMING_CHECKPOINT", "/checkpoints/kafka_to_bronze")
    max_runtime = args.max_runtime or int(os.environ.get("STREAMING_RUNTIME_SECONDS", "3600"))
    trigger_seconds = args.trigger_seconds or int(
        os.environ.get("STREAMING_TRIGGER_SECONDS", "30"))
    starting = "earliest" if args.from_earliest else "latest"

    spark, schema = build_spark_session("Streaming Kafka vers Bronze (Open-Meteo)")
    spark._meteo_schema = schema  # schéma accessible dans write_batch

    stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", starting)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 20000)
        .load()
        .selectExpr("CAST(value AS STRING) AS json_value")
        .filter("json_value IS NOT NULL")
    )

    query = (stream
             .writeStream
             .foreachBatch(lambda df, eid: write_batch(df, eid, spark))
             .option("checkpointLocation", f"{hdfs_base()}{checkpoint}")
             .trigger(processingTime=f"{trigger_seconds} seconds")
             .start())

    logger.info("Streaming démarré : topic=%s, trigger=%ss, max_runtime=%ss, offsets=%s",
                topic, trigger_seconds, max_runtime, starting)
    start = time.time()
    try:
        while time.time() - start < max_runtime:
            if not query.isActive:
                logger.warning("La requête streaming s'est arrêtée prématurément.")
                break
            query.awaitTermination(min(30, int(max_runtime - (time.time() - start))))
    finally:
        logger.info("Arrêt de la requête streaming (durée max atteinte ou signal)...")
        query.stop()
        spark.stop()
    logger.info("Streaming terminé proprement après %.0f secondes.", time.time() - start)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Spark Structured Streaming : Kafka (Open-Meteo) -> Bronze HDFS")
    parser.add_argument("--max-runtime", type=int, default=None,
                        help="Durée max en secondes (défaut : env STREAMING_RUNTIME_SECONDS)")
    parser.add_argument("--trigger-seconds", type=int, default=None,
                        help="Fréquence des micro-batchs (défaut : env STREAMING_TRIGGER_SECONDS)")
    parser.add_argument("--from-earliest", action="store_true",
                        help="Consomme depuis le début du topic (sinon latest)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s [streaming_ingest] %(message)s")
    return run_streaming(args)


if __name__ == "__main__":
    sys.exit(main())
