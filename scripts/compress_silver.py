# -*- coding: utf-8 -*-
"""
compress_silver.py — Job Spark de re-compression du Silver (DataLake Météo).
===========================================================================
Relit /silver/meteo et le réécrit en place en Parquet partitionné par dt,
en forçant la compression Zstd niveau 22 (overwrite dynamique → idempotent).
Affiche la taille avant/après via hdfs_utils.hdfs_size_gb.

Usage :
    spark-submit compress_silver.py [--path /silver/meteo]
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import List, Optional

logger = logging.getLogger("compress_silver")


def hdfs_base() -> str:
    """Préfixe HDFS utilisé par Spark (RPC), ex. hdfs://namenode:9000."""
    namenode = os.environ.get("HDFS_NAMENODE", "namenode")
    rpc_port = os.environ.get("HDFS_RPC_PORT", "9000")
    return f"hdfs://{namenode}:{rpc_port}"


def build_spark_session(app_name: str) -> "SparkSession":
    """Construit la SparkSession avec compression zstd 22 + overwrite dynamique."""
    from pyspark.sql import SparkSession

    master = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")
    return (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.parquet.compression.codec", "zstd")
        .config("spark.sql.parquet.compression.codec.zstd.level", "22")
        .config("spark.sql.parquet.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def run(args: argparse.Namespace) -> None:
    """Réécrit en place le Silver en forçant zstd niveau 22 (idempotent)."""
    import hdfs_utils

    hdfs_dir = args.path
    spark_path = f"{hdfs_base()}{hdfs_dir}"

    if not hdfs_utils.hdfs_exists(hdfs_dir):
        logger.info("Répertoire Silver inexistant (%s), rien à compresser.", hdfs_dir)
        return

    before_gb = hdfs_utils.hdfs_size_gb(hdfs_dir)
    logger.info("Taille Silver AVANT compression : %.3f Go", before_gb)

    spark = build_spark_session("Recompression Silver")
    try:
        df = spark.read.parquet(spark_path)
        (
            df.write
            .partitionBy("dt")
            .mode("overwrite")
            .option("partitionOverwriteMode", "dynamic")
            .option("compression", "zstd")
            .option("compression.level", "22")
            .parquet(spark_path)
        )
    finally:
        spark.stop()

    hdfs_utils.write_success(hdfs_dir)
    after_gb = hdfs_utils.hdfs_size_gb(hdfs_dir)
    logger.info("Taille Silver APRÈS compression : %.3f Go", after_gb)
    logger.info("Différence : %.3f Go", before_gb - after_gb)


def parse_args(argv: Optional[List[str]]) -> argparse.Namespace:
    """Construit le parseur d'arguments CLI."""
    parser = argparse.ArgumentParser(description="Job Spark : re-compression Silver (zstd 22)")
    parser.add_argument(
        "--path",
        default="/silver/meteo",
        help="Répertoire HDFS du Silver à re-compresser.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run(parse_args(argv))
    return 0


if __name__ == "__main__":
    sys.exit(main())
