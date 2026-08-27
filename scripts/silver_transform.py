# -*- coding: utf-8 -*-
"""
silver_transform.py — Job Spark « Bronze vers Silver » (DataLake Météo).
=======================================================================
Lit les données Bronze (NOAA en CSV + Open-Meteo en JSON) sur HDFS, les
normalise vers un schéma Silver unifié, calcule des indicateurs de fenêtre
(moyennes mobiles, écart-type, anomalie) puis écrit le résultat en Parquet
partitionné par dt, compressé Zstd niveau 22.

Conception :
    - Tous les imports pyspark sont réalisés À L'INTÉRIEUR des fonctions
      afin que ce module reste importable SANS Spark (tests unitaires).
    - Toute la logique pure (parsing, conversion, validation de schéma) est
      exposée sous forme de fonctions pures, testables via pytest.

Usage :
    spark-submit silver_transform.py --start-date 2022-01-01 --end-date 2025-12-31 [--only-new]
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from functools import reduce
from typing import List, Optional, Tuple

logger = logging.getLogger("silver_transform")

# ---------------------------------------------------------------------------
# Constantes du contrat Silver (aucune dépendance Spark ici)
# ---------------------------------------------------------------------------

#: Schéma Silver unifié (l'ordre est libre ; utilisé pour le select final).
SILVER_SCHEMA: List[str] = [
    "station_id",
    "station_name",
    "city",
    "country",
    "latitude",
    "longitude",
    "elevation",
    "timestamp",
    "temperature",
    "precipitation",
    "wind_speed",
    "snow",
    "source",
    "dt",
]

#: Colonnes d'enrichissement calculées par fenêtre et persistées en Silver.
INDICATOR_COLUMNS: List[str] = [
    "temp_ma3",
    "temp_ma7",
    "temp_std7",
    "temp_anomaly",
]

#: Seuil des valeurs manquantes NOAA (dixièmes d'unité).
_MISSING_LOW = -900.0
#: Valeur sentinelle « manquant » pour SNOW / SNWD.
_MISSING_SNOW = 9999.0

#: Colonnes obligatoires du CSV NOAA.
NOAA_REQUIRED_COLUMNS: List[str] = [
    "STATION", "NAME", "LATITUDE", "LONGITUDE", "ELEVATION",
    "DATE", "PRCP", "TMAX", "TMIN", "TAVG", "SNOW", "AWND",
]

#: Colonnes obligatoires du JSON Open-Meteo.
OPENMETEO_REQUIRED_COLUMNS: List[str] = [
    "city", "latitude", "longitude", "timestamp",
    "temperature", "windspeed", "precipitation",
]


# ---------------------------------------------------------------------------
# Fonctions pures (importables et testables SANS Spark)
# ---------------------------------------------------------------------------

def parse_city_country(name: str) -> Tuple[str, str]:
    """
    Sépare un nom de station au format « VILLE, CC » en (ville, code pays).

    Exemples :
        "PARIS  , FR"      -> ("Paris", "FR")
        "LYON-BRON , FR"   -> ("Lyon-Bron", "FR")
        "MARSEILLE"        -> ("Marseille", "")   (pas de virgule)
    """
    if not name:
        return ("", "")
    name = name.strip()
    if "," in name:
        city_raw, country_raw = name.split(",", 1)
        return (city_raw.strip().title(), country_raw.strip().upper())
    # Pas de virgule : on retourne le nom nettoyé seul, sans code pays.
    return (name.title(), "")


def is_missing(value: Optional[float]) -> bool:
    """
    Retourne True si la valeur est un code « manquant » NOAA.

    Règle : None, valeur <= -900 (ex. -9999) ou valeur == 9999 (SNOW/SNWD).
    """
    if value is None:
        return True
    try:
        v = float(value)
    except (TypeError, ValueError):
        return True
    return v <= _MISSING_LOW or v == _MISSING_SNOW


def _to_unit(tenths: Optional[float]) -> Optional[float]:
    """Convertit des dixièmes d'unité en unité (÷ 10, arrondi à 2 décimales)."""
    if is_missing(tenths):
        return None
    return round(float(tenths) / 10.0, 2)


def to_celsius(tenths: Optional[float]) -> Optional[float]:
    """Convertit des dixièmes de °C en °C (None si valeur manquante)."""
    return _to_unit(tenths)


def validate_required_columns(columns: List[str], required: List[str]) -> List[str]:
    """Retourne la liste des colonnes required absentes de columns."""
    present = set(columns)
    return [col for col in required if col not in present]


def dedup_keys() -> List[str]:
    """Clés de déduplication du contrat Silver."""
    return ["station_id", "timestamp"]


# ---------------------------------------------------------------------------
# Helpers d'infrastructure (imports pyspark locaux)
# ---------------------------------------------------------------------------

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


def _write_parquet_dynamic(df: "DataFrame", hdfs_path: str, partition_cols: List[str]) -> None:
    """
    Écrit un DataFrame en Parquet partitionné (overwrite dynamique, zstd 22).

    L'overwrite dynamique garantit l'idempotence : seules les partitions
    présentes dans l'input sont réécrites, les autres sont conservées.
    """
    (
        df.write
        .partitionBy(*partition_cols)
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .option("compression", "zstd")
        .option("compression.level", "22")
        .parquet(hdfs_path)
    )


# ---------------------------------------------------------------------------
# Lecture du Bronze
# ---------------------------------------------------------------------------

def read_noaa(spark: "SparkSession") -> Optional["DataFrame"]:
    """Lit les CSV NOAA du Bronze (une partition par year/month)."""
    path = f"{hdfs_base()}/bronze/meteo/batch/source=noaa/year=*/month=*/*.csv"
    logger.info("Lecture NOAA : %s", path)
    try:
        return (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(path)
        )
    except Exception as exc:  # chemin inexistant / répertoire vide
        logger.warning("Lecture NOAA impossible (%s) : %s", path, exc)
        return None


def read_openmeteo(spark: "SparkSession") -> Optional["DataFrame"]:
    """Lit les JSON Open-Meteo du Bronze (lignes JSON par heure)."""
    path = f"{hdfs_base()}/bronze/meteo/stream/source=openmeteo/year=*/month=*/day=*/hour=*/*.json"
    logger.info("Lecture Open-Meteo : %s", path)
    try:
        return spark.read.json(path)
    except Exception as exc:  # chemin inexistant / répertoire vide
        logger.warning("Lecture Open-Meteo impossible (%s) : %s", path, exc)
        return None


# ---------------------------------------------------------------------------
# Transformations vers le schéma Silver
# ---------------------------------------------------------------------------

def _register_udfs():
    """Enregistre les UDFs à partir des fonctions pures (types Spark requis)."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType

    is_missing_udf = F.udf(is_missing, BooleanType())
    to_celsius_udf = F.udf(to_celsius, DoubleType())
    to_unit_udf = F.udf(_to_unit, DoubleType())
    parse_udf = F.udf(parse_city_country, StructType([
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
    ]))
    return is_missing_udf, to_celsius_udf, to_unit_udf, parse_udf


def transform_noaa(noaa_df: "DataFrame") -> "DataFrame":
    """Mappe le CSV NOAA vers le schéma Silver unifié."""
    from pyspark.sql import functions as F

    missing = validate_required_columns(noaa_df.columns, NOAA_REQUIRED_COLUMNS)
    if missing:
        raise ValueError(f"Colonnes NOAA manquantes : {missing}")

    is_missing_udf, to_celsius_udf, to_unit_udf, parse_udf = _register_udfs()

    return (
        noaa_df
        .withColumn("_parsed", parse_udf(F.col("NAME")))
        .withColumn("station_id", F.col("STATION"))
        .withColumn("station_name", F.col("NAME"))
        .withColumn("city", F.col("_parsed").getField("city"))
        .withColumn("country", F.col("_parsed").getField("country"))
        .withColumn("latitude", F.col("LATITUDE"))
        .withColumn("longitude", F.col("LONGITUDE"))
        .withColumn("elevation", F.col("ELEVATION"))
        .withColumn(
            "temperature",
            F.when(
                (~is_missing_udf(F.col("TMAX"))) & (~is_missing_udf(F.col("TMIN"))),
                F.round((F.col("TMAX") + F.col("TMIN")) / 2.0 / 10.0, 2),
            ).otherwise(to_celsius_udf(F.col("TAVG"))),
        )
        .withColumn("precipitation", to_unit_udf(F.col("PRCP")))
        .withColumn("wind_speed", to_unit_udf(F.col("AWND")))
        .withColumn("snow", to_unit_udf(F.col("SNOW")))
        .withColumn("timestamp", F.to_timestamp(F.col("DATE"), "yyyyMMdd"))
        .withColumn("dt", F.to_date(F.col("timestamp")))
        .withColumn("source", F.lit("NOAA"))
        .select(*SILVER_SCHEMA)
    )


def transform_openmeteo(om_df: "DataFrame") -> "DataFrame":
    """Mappe les JSON Open-Meteo vers le schéma Silver unifié."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType

    missing = validate_required_columns(om_df.columns, OPENMETEO_REQUIRED_COLUMNS)
    if missing:
        raise ValueError(f"Colonnes Open-Meteo manquantes : {missing}")

    return (
        om_df
        .withColumn("station_id", F.concat(F.lit("OPENMETEO_"), F.upper(F.col("city"))))
        .withColumn("station_name", F.col("city"))
        .withColumn("country", F.lit("FR"))
        .withColumn("latitude", F.col("latitude"))
        .withColumn("longitude", F.col("longitude"))
        .withColumn("elevation", F.lit(None).cast(DoubleType()))
        .withColumn("temperature", F.col("temperature"))
        .withColumn("precipitation", F.col("precipitation"))
        .withColumn("wind_speed", F.col("windspeed"))
        .withColumn("snow", F.lit(None).cast(DoubleType()))
        .withColumn("timestamp", F.to_timestamp(F.col("timestamp")))
        .withColumn("dt", F.to_date(F.col("timestamp")))
        .withColumn("source", F.lit("OPENMETEO"))
        .select(*SILVER_SCHEMA)
    )


def add_indicators(silver: "DataFrame") -> "DataFrame":
    """
    Ajoute les indicateurs de fenêtre par station (ordonnés par timestamp).

    - temp_ma7 / temp_std7 : fenêtre de 7 jours (rowsBetween -6..0).
    - temp_ma3 : fenêtre de 3 jours (rowsBetween -2..0).
    - temp_anomaly : écart entre la température et sa moyenne mobile 7 jours.
    """
    from pyspark.sql import functions as F
    from pyspark.sql import Window

    win7 = Window.partitionBy("station_id").orderBy("timestamp").rowsBetween(-6, 0)
    win3 = Window.partitionBy("station_id").orderBy("timestamp").rowsBetween(-2, 0)

    return (
        silver
        .withColumn("temp_ma7", F.avg("temperature").over(win7))
        .withColumn("temp_ma3", F.avg("temperature").over(win3))
        .withColumn("temp_std7", F.stddev("temperature").over(win7))
        .withColumn("temp_anomaly", F.col("temperature") - F.col("temp_ma7"))
    )


def transform_to_silver(spark: "SparkSession", start_date: str, end_date: str) -> Optional["DataFrame"]:
    """Lit le Bronze, l'unifie en Silver, déduplique et calcule les indicateurs."""
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F

    noaa_df = read_noaa(spark)
    om_df = read_openmeteo(spark)

    frames: List["DataFrame"] = []
    if noaa_df is not None:
        frames.append(transform_noaa(noaa_df))
    if om_df is not None:
        frames.append(transform_openmeteo(om_df))

    if not frames:
        logger.info("Aucune donnée Bronze disponible.")
        return None

    silver = reduce(lambda a, b: a.unionByName(b), frames)

    # Suppression des timestamp null, puis déduplication (station_id, timestamp).
    silver = (
        silver
        .filter(F.col("timestamp").isNotNull())
        .dropDuplicates(dedup_keys())
    )

    # Indicateurs de fenêtre.
    silver = add_indicators(silver)

    # Filtre temporel appliqué sur la colonne de partition dt.
    start = F.lit(start_date).cast("date")
    end = F.lit(end_date).cast("date")
    silver = silver.filter((F.col("dt") >= start) & (F.col("dt") <= end))

    return silver


def write_silver(silver: "DataFrame", only_new: bool) -> None:
    """Écrit le Silver en Parquet (zstd 22) + marqueurs _SUCCESS idempotents."""
    import hdfs_utils
    from pyspark.sql import functions as F

    output_cols = SILVER_SCHEMA + INDICATOR_COLUMNS
    silver = silver.select(*output_cols)

    # Partitions dt à écrire (au format YYYY-MM-DD).
    all_dts = [
        row.dt.strftime("%Y-%m-%d")
        for row in silver.select("dt").distinct().orderBy("dt").collect()
    ]

    if only_new:
        dts = [d for d in all_dts if not hdfs_utils.has_success(f"/silver/meteo/dt={d}")]
        skipped = set(all_dts) - set(dts)
        if skipped:
            logger.info("Partitions déjà écrites (--only-new) ignorées : %s", sorted(skipped))
    else:
        dts = all_dts

    if not dts:
        logger.info("Aucune partition Silver à écrire.")
        return

    # Restreindre l'écriture aux partitions retenues (cas --only-new).
    if len(dts) != len(all_dts):
        silver = silver.filter(F.col("dt").cast("string").isin(dts))

    silver_path = f"{hdfs_base()}/silver/meteo"
    logger.info("Écriture Silver vers %s (%d partition(s) dt)", silver_path, len(dts))
    _write_parquet_dynamic(silver, silver_path, ["dt"])

    # Marqueurs d'idempotence (par partition puis racine).
    for d in dts:
        hdfs_utils.write_success(f"/silver/meteo/dt={d}")
    hdfs_utils.write_success("/silver/meteo")
    logger.info("Silver écrit : %d partition(s), _SUCCESS déposés.", len(dts))


def run(args: argparse.Namespace) -> None:
    """Exécute le pipeline Bronze -> Silver."""
    spark = build_spark_session("Bronze vers Silver")
    try:
        silver = transform_to_silver(spark, args.start_date, args.end_date)
        if silver is None:
            return
        write_silver(silver, only_new=args.only_new)
    finally:
        spark.stop()


def parse_args(argv: Optional[List[str]]) -> argparse.Namespace:
    """Construit le parseur d'arguments CLI."""
    parser = argparse.ArgumentParser(description="Job Spark : Bronze vers Silver (DataLake Météo)")
    parser.add_argument(
        "--start-date",
        default=os.environ.get("SILVER_START_DATE", "2022-01-01"),
        help="Borne inférieure du filtre sur dt (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date",
        default=os.environ.get("SILVER_END_DATE", "2025-12-31"),
        help="Borne supérieure du filtre sur dt (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--only-new",
        action="store_true",
        help="Ne pas réécrire les partitions dt dont le _SUCCESS existe déjà.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args(argv)
    run(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
