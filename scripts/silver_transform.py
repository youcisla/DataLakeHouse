# -*- coding: utf-8 -*-
"""
silver_transform.py : job Spark « Bronze vers Silver » (DataLake Météo).
=======================================================================
Lit les données Bronze (archives Météo-France en CSV ';' gzippé, et
stream Open-Meteo en JSON) sur HDFS, les
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
import re
import sys
from functools import reduce
from typing import List, Optional

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

#: Colonnes obligatoires du CSV Météo-France (jeu QUOT « RR-T-Vent »).
#: Source : https://meteo.data.gouv.fr (données climatologiques quotidiennes).
METEOFRANCE_REQUIRED_COLUMNS: List[str] = [
    "NUM_POSTE", "NOM_USUEL", "LAT", "LON", "ALTI", "AAAAMMJJ",
    "RR", "TN", "TX",
]

#: Colonnes Météo-France facultatives (absentes de certains millésimes).
METEOFRANCE_OPTIONAL_COLUMNS: List[str] = ["TM", "FFM", "NEIGETOT"]

#: Séparateur de colonnes des CSV Météo-France.
METEOFRANCE_SEPARATOR = ";"

#: Colonnes obligatoires du JSON Open-Meteo.
OPENMETEO_REQUIRED_COLUMNS: List[str] = [
    "city", "latitude", "longitude", "timestamp",
    "temperature", "windspeed", "precipitation",
]


# ---------------------------------------------------------------------------
# Fonctions pures (importables et testables SANS Spark)
# ---------------------------------------------------------------------------

def validate_required_columns(columns: List[str], required: List[str]) -> List[str]:
    """Retourne la liste des colonnes required absentes de columns."""
    present = set(columns)
    return [col for col in required if col not in present]


def mf_parse_number(value) -> Optional[float]:
    """
    Convertit une valeur numérique Météo-France en float.

    Les fichiers QUOT laissent les mesures manquantes **vides** ; certains
    exports utilisent la virgule décimale. Retourne None si non convertible.

    Exemples :
        "12.4"  -> 12.4        "12,4" -> 12.4
        ""      -> None        "   "  -> None        None -> None
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        result = float(value)
        return None if result != result else result  # écarte NaN
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def mf_parse_date(value) -> Optional[str]:
    """
    Convertit la date Météo-France ``AAAAMMJJ`` en ``YYYY-MM-DD``.

    Exemples :
        "20250115" -> "2025-01-15"      20250115 -> "2025-01-15"
        "2025-01-15" -> "2025-01-15"    ""/None/"2025" -> None
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        text = text.replace("-", "")
    if len(text) != 8 or not text.isdigit():
        return None
    year, month, day = int(text[:4]), int(text[4:6]), int(text[6:])
    if not (1 <= month <= 12 and 1 <= day <= 31):
        return None
    return f"{year:04d}-{month:02d}-{day:02d}"


def mf_station_id(num_poste) -> str:
    """
    Identifiant Silver d'un poste Météo-France : ``MF_`` + NUM_POSTE sur 8.

    Exemples :
        "75114001" -> "MF_75114001"       7511 -> "MF_00007511"
    """
    if num_poste is None:
        return ""
    text = str(num_poste).strip()
    if not text:
        return ""
    if text.isdigit():
        text = text.zfill(8)
    return f"MF_{text}"


def mf_city_name(nom_usuel) -> str:
    """
    Déduit la ville depuis le nom usuel du poste Météo-France.

    Le nom usuel est en majuscules et suffixé par le site de la station
    (« PARIS-MONTSOURIS », « LYON-BRON »). On conserve le premier segment,
    en casse de titre, ce qui aligne les archives sur les villes du flux
    temps réel Open-Meteo et rend les agrégats Gold comparables.

    Exemples :
        "PARIS-MONTSOURIS"      -> "Paris"
        "BORDEAUX-MERIGNAC"     -> "Bordeaux"
        "LILLE LESQUIN"         -> "Lille"
        "SAINT-BRIEUC"          -> "Saint-Brieuc"  (préfixe conservé)
        ""                      -> ""
    """
    if not nom_usuel:
        return ""
    text = " ".join(str(nom_usuel).split()).strip()
    if not text:
        return ""
    # Les préfixes composés (SAINT-, SAINTE-, LE-, LA-, LES-) ne sont pas des
    # séparateurs de site : on ne coupe qu'après eux.
    prefixes = ("SAINT", "SAINTE", "ST", "STE", "LE", "LA", "LES")
    # re.split avec groupe capturant : les séparateurs d'origine sont conservés
    # (« LE HAVRE » -> « Le Havre », « SAINT-BRIEUC » -> « Saint-Brieuc »).
    tokens = re.split(r"([-\s])", text.upper())
    words = tokens[0::2]
    separators = tokens[1::2]
    kept = [words[0]]
    used_separators: List[str] = []
    index = 1
    while index < len(words) and kept[-1] in prefixes:
        used_separators.append(separators[index - 1])
        kept.append(words[index])
        index += 1
    result = kept[0].capitalize()
    for separator, word in zip(used_separators, kept[1:]):
        result += ("-" if separator == "-" else " ") + word.capitalize()
    return result


def mf_mean_temperature(tm, tn, tx) -> Optional[float]:
    """
    Température moyenne journalière Météo-France.

    Utilise ``TM`` lorsqu'elle est renseignée, sinon la demi-somme
    ``(TN + TX) / 2``, sinon la seule valeur disponible, sinon None.
    """
    mean = mf_parse_number(tm)
    if mean is not None:
        return round(mean, 2)
    low, high = mf_parse_number(tn), mf_parse_number(tx)
    if low is not None and high is not None:
        return round((low + high) / 2.0, 2)
    if low is not None:
        return round(low, 2)
    if high is not None:
        return round(high, 2)
    return None


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

def read_meteofrance(spark: "SparkSession") -> Optional["DataFrame"]:
    """
    Lit les archives Météo-France du Bronze (CSV ';' gzippés, lecture brute).

    Spark décompresse le gzip de façon transparente ; les colonnes sont lues
    en texte puis converties explicitement, car les mesures manquantes sont
    des champs vides et non des sentinelles numériques.
    """
    path = f"{hdfs_base()}/bronze/meteo/batch/source=meteofrance/year=*/month=*/*.csv.gz"
    logger.info("Lecture Météo-France : %s", path)
    try:
        return (
            spark.read
            .option("header", "true")
            .option("sep", METEOFRANCE_SEPARATOR)
            .option("inferSchema", "false")
            .csv(path)
        )
    except Exception as exc:  # chemin inexistant / répertoire vide
        logger.warning("Lecture Météo-France impossible (%s) : %s", path, exc)
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

def transform_meteofrance(mf_df: "DataFrame") -> "DataFrame":
    """
    Mappe le CSV Météo-France (QUOT RR-T-Vent) vers le schéma Silver unifié.

    - validation stricte des colonnes obligatoires (échec explicite sinon) ;
    - conversion des champs vides en NULL (``mf_parse_number``) ;
    - température = TM si disponible, sinon (TN + TX) / 2 ;
    - la neige (NEIGETOT) n'est présente que dans le jeu « autres paramètres » :
      elle est lue si la colonne existe, sinon NULL.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, StringType

    missing = validate_required_columns(mf_df.columns, METEOFRANCE_REQUIRED_COLUMNS)
    if missing:
        raise ValueError(f"Colonnes Météo-France manquantes : {missing}")

    num_udf = F.udf(mf_parse_number, DoubleType())
    date_udf = F.udf(mf_parse_date, StringType())
    station_udf = F.udf(mf_station_id, StringType())
    city_udf = F.udf(mf_city_name, StringType())
    mean_udf = F.udf(mf_mean_temperature, DoubleType())

    def _optional(column: str):
        """Colonne facultative : NULL typé si absente du millésime lu."""
        return F.col(column) if column in mf_df.columns else F.lit(None).cast(StringType())

    return (
        mf_df
        .withColumn("station_id", station_udf(F.col("NUM_POSTE")))
        .withColumn("station_name", F.col("NOM_USUEL"))
        .withColumn("city", city_udf(F.col("NOM_USUEL")))
        .withColumn("country", F.lit("FR"))
        .withColumn("latitude", num_udf(F.col("LAT")))
        .withColumn("longitude", num_udf(F.col("LON")))
        .withColumn("elevation", num_udf(F.col("ALTI")))
        .withColumn("temperature", mean_udf(_optional("TM"), F.col("TN"), F.col("TX")))
        .withColumn("precipitation", num_udf(F.col("RR")))
        .withColumn("wind_speed", num_udf(_optional("FFM")))
        .withColumn("snow", num_udf(_optional("NEIGETOT")))
        .withColumn("timestamp", F.to_timestamp(date_udf(F.col("AAAAMMJJ")), "yyyy-MM-dd"))
        .withColumn("dt", F.to_date(F.col("timestamp")))
        .withColumn("source", F.lit("METEOFRANCE"))
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

    mf_df = read_meteofrance(spark)
    om_df = read_openmeteo(spark)

    frames: List["DataFrame"] = []
    # Source batch principale : archives Météo-France (meteo.data.gouv.fr).
    if mf_df is not None:
        frames.append(transform_meteofrance(mf_df))
    # Source temps réel : Open-Meteo via Kafka.
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
        # Double filet : le checkpoint (rapide, une seule lecture) ET le
        # marqueur _SUCCESS (source de verite cote donnees). Une partition
        # n'est rejouee que si les DEUX disent qu'elle manque.
        import checkpoint

        candidates = checkpoint.pending_keys(checkpoint.STAGE_SILVER, all_dts)
        dts = [d for d in candidates if not hdfs_utils.has_success(f"/silver/meteo/dt={d}")]
        skipped = [d for d in all_dts if d not in dts]
        if skipped:
            logger.info("Partitions deja ecrites (--only-new) ignorees : %d (%s...)",
                        len(skipped), ", ".join(sorted(skipped)[:5]))
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

    # Marqueurs d'idempotence (par partition puis racine) + checkpoints.
    import checkpoint

    run = checkpoint.run_id("silver")
    for d in dts:
        hdfs_utils.write_success(f"/silver/meteo/dt={d}")
        checkpoint.mark_done(checkpoint.STAGE_SILVER, d)
    hdfs_utils.write_success("/silver/meteo")
    checkpoint.record_run(checkpoint.STAGE_SILVER, run, "success",
                          partitions_written=len(dts))
    logger.info("Silver ecrit : %d partition(s), _SUCCESS et checkpoints deposes.", len(dts))


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
