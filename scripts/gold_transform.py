# -*- coding: utf-8 -*-
"""
gold_transform.py : job Spark « Silver vers Gold » (DataLake Météo).
=====================================================================
Lit le Silver (/silver/meteo) et produit quatre agrégats Gold :
    1. daily_aggregates  : agrégats quotidiens par (dt, city, source) ;
    2. weekly_trends     : tendances hebdomadaires + pente de régression ;
    3. extreme_events    : événements extrêmes via classify_extreme ;
    4. climate_profile   : « profil météo » mensuel par ville (normales
       climatiques, amplitude thermique, jours de pluie, saison) construit
       depuis les archives Météo-France, l'équivalent météo d'un profil
       client, et le socle des features du modèle ML.

Conception :
    - Les imports pyspark sont réalisés À L'INTÉRIEUR des fonctions pour
      rester importable SANS Spark (tests unitaires).
    - La logique métier (classification d'événements, pente de régression)
      est exposée en fonctions pures testables.

Usage :
    spark-submit gold_transform.py --start-date 2022-01-01 --end-date 2025-12-31
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Dict, List, Optional

logger = logging.getLogger("gold_transform")


# ---------------------------------------------------------------------------
# Fonctions pures (testables sans Spark)
# ---------------------------------------------------------------------------

def get_thresholds() -> Dict[str, float]:
    """
    Lit les seuils d'événements extrêmes depuis l'environnement.

    Variables lues (avec valeurs par défaut) :
        THRESHOLD_HEATWAVE_C      -> 35.0
        THRESHOLD_HEAVY_RAIN_MM   -> 20.0
        THRESHOLD_STRONG_WIND_MS  -> 20.0
        THRESHOLD_COLD_SNAP_C     -> -10.0
    """
    return {
        "heatwave": float(os.environ.get("THRESHOLD_HEATWAVE_C", "35.0")),
        "heavy_rain": float(os.environ.get("THRESHOLD_HEAVY_RAIN_MM", "20.0")),
        "strong_wind": float(os.environ.get("THRESHOLD_STRONG_WIND_MS", "20.0")),
        "cold_snap": float(os.environ.get("THRESHOLD_COLD_SNAP_C", "-10.0")),
    }


def linear_slope(xs: List[float], ys: List[float]) -> float:
    """
    Pente de la régression linéaire simple y = a*x + b (moindres carrés).

    Formule : a = (n*Σxy - Σx*Σy) / (n*Σx² - (Σx)²).
    Retourne 0.0 si n < 2, si les tailles diffèrent ou si le dénominateur est nul.
    """
    n = len(xs)
    if n < 2 or n != len(ys):
        return 0.0
    sum_x = sum(xs)
    sum_y = sum(ys)
    sum_xy = sum(x * y for x, y in zip(xs, ys))
    sum_x2 = sum(x * x for x in xs)
    denominator = n * sum_x2 - sum_x * sum_x
    if denominator == 0.0:
        return 0.0
    return (n * sum_xy - sum_x * sum_y) / denominator


def season_of_month(month) -> str:
    """
    Saison météorologique (hémisphère nord) d'un mois.

    hiver : 12, 1, 2 · printemps : 3-5 · été : 6-8 · automne : 9-11.
    Retourne "" si le mois est invalide ou manquant.
    """
    try:
        value = int(month)
    except (TypeError, ValueError):
        return ""
    if value in (12, 1, 2):
        return "hiver"
    if value in (3, 4, 5):
        return "printemps"
    if value in (6, 7, 8):
        return "ete"
    if value in (9, 10, 11):
        return "automne"
    return ""


def rain_day_ratio(rain_days, total_days) -> float:
    """
    Part de jours de pluie sur la période (0.0 -> 1.0, arrondie à 3 décimales).

    Retourne 0.0 si le dénominateur est nul, négatif ou non numérique.
    """
    try:
        rain = float(rain_days)
        total = float(total_days)
    except (TypeError, ValueError):
        return 0.0
    if total <= 0:
        return 0.0
    return round(max(0.0, min(rain / total, 1.0)), 3)


def _format_date(dt) -> str:
    """Formate une date (objet date ou str) au format YYYY-MM-DD."""
    if dt is None:
        return ""
    if hasattr(dt, "strftime"):
        return dt.strftime("%Y-%m-%d")
    return str(dt)[:10]


def classify_extreme(row: dict, thresholds: dict) -> List[dict]:
    """
    Détecte les événements extrêmes d'une ligne d'agrégat quotidien.

    Règles (seuils par défaut) :
        - temp_max >= heatwave      -> « canicule »
        - precip_sum >= heavy_rain  -> « fortes_pluies »
        - wind_avg >= strong_wind   -> « vents_violents »
        - temp_min <= cold_snap     -> « vague_de_froid »

    Sévérité : « extreme » si la valeur dépasse 1.25× le seuil (pour le froid,
    value <= 1.25 * threshold car le seuil est négatif), sinon « alerte ».
    """
    events: List[dict] = []
    city = row.get("city", "")
    dt_str = _format_date(row.get("dt"))

    def _severity(value: float, threshold: float, is_cold: bool = False) -> str:
        if is_cold:
            return "extreme" if value <= 1.25 * threshold else "alerte"
        return "extreme" if value >= 1.25 * threshold else "alerte"

    def _num(value) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    temp_max = _num(row.get("temp_max"))
    temp_min = _num(row.get("temp_min"))
    precip_sum = _num(row.get("precip_sum"))
    wind_avg = _num(row.get("wind_avg"))

    # Canicule.
    if temp_max is not None and thresholds.get("heatwave") is not None and temp_max >= thresholds["heatwave"]:
        thr = float(thresholds["heatwave"])
        events.append({
            "event_type": "canicule",
            "value": temp_max,
            "threshold": thr,
            "severity": _severity(temp_max, thr),
            "detail": f"{city} : {temp_max:.1f}°C le {dt_str}",
        })

    # Fortes pluies.
    if precip_sum is not None and thresholds.get("heavy_rain") is not None and precip_sum >= thresholds["heavy_rain"]:
        thr = float(thresholds["heavy_rain"])
        events.append({
            "event_type": "fortes_pluies",
            "value": precip_sum,
            "threshold": thr,
            "severity": _severity(precip_sum, thr),
            "detail": f"{city} : {precip_sum:.1f} mm de pluie le {dt_str}",
        })

    # Vents violents.
    if wind_avg is not None and thresholds.get("strong_wind") is not None and wind_avg >= thresholds["strong_wind"]:
        thr = float(thresholds["strong_wind"])
        events.append({
            "event_type": "vents_violents",
            "value": wind_avg,
            "threshold": thr,
            "severity": _severity(wind_avg, thr),
            "detail": f"{city} : {wind_avg:.1f} m/s de vent le {dt_str}",
        })

    # Vague de froid.
    if temp_min is not None and thresholds.get("cold_snap") is not None and temp_min <= thresholds["cold_snap"]:
        thr = float(thresholds["cold_snap"])
        events.append({
            "event_type": "vague_de_froid",
            "value": temp_min,
            "threshold": thr,
            "severity": _severity(temp_min, thr, is_cold=True),
            "detail": f"{city} : {temp_min:.1f}°C le {dt_str}",
        })

    return events


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
        .config("spark.sql.parquet.compression.codec.zstd.level", "3")
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
        .option("compression.level", "3")
        .parquet(hdfs_path)
    )


# ---------------------------------------------------------------------------
# Lecture du Silver
# ---------------------------------------------------------------------------

def read_silver(spark: "SparkSession") -> Optional["DataFrame"]:
    """Lit le Silver (Parquet partitionné par dt)."""
    path = f"{hdfs_base()}/silver/meteo"
    logger.info("Lecture Silver : %s", path)
    try:
        return spark.read.parquet(path)
    except Exception as exc:  # chemin inexistant / répertoire vide
        logger.warning("Lecture Silver impossible (%s) : %s", path, exc)
        return None


# ---------------------------------------------------------------------------
# Agrégats Gold
# ---------------------------------------------------------------------------

def compute_daily_aggregates(silver: "DataFrame") -> "DataFrame":
    """Calcule les agrégats quotidiens par (dt, city, source)."""
    from pyspark.sql import functions as F

    return (
        silver
        .groupBy("dt", "city", "source")
        .agg(
            F.count("*").alias("n_obs"),
            F.avg("temperature").alias("temp_avg"),
            F.min("temperature").alias("temp_min"),
            F.max("temperature").alias("temp_max"),
            F.sum("precipitation").alias("precip_sum"),
            F.avg("wind_speed").alias("wind_avg"),
            F.sum("snow").alias("snow_sum"),
            F.stddev("temperature").alias("temp_std"),
        )
    )


def compute_weekly_trends(daily: "DataFrame") -> "DataFrame":
    """
    Calcule les tendances hebdomadaires par (year, week, city, source).

    trend_slope = pente de régression de temp_avg journalière sur l'indice du
    jour (x = nombre de jours depuis le premier jour de la semaine), calculée
    par sommes agrégées : a = (n*Σxy - Σx*Σy) / (n*Σx² - (Σx)²).
    """
    from pyspark.sql import functions as F
    from pyspark.sql import Window

    daily = daily.withColumn("year", F.year("dt")).withColumn("week", F.weekofyear("dt"))

    # Date minimale de chaque semaine : x = nombre de jours depuis ce minimum.
    min_dt = daily.groupBy("year", "week", "city", "source").agg(F.min("dt").alias("_min_dt"))
    daily = daily.join(min_dt, ["year", "week", "city", "source"], "left")
    daily = daily.withColumn("_x", F.datediff(F.col("dt"), F.col("_min_dt")))

    weekly = daily.groupBy("year", "week", "city", "source").agg(
        F.avg("temp_avg").alias("temp_avg"),
        F.min("temp_min").alias("temp_min"),
        F.max("temp_max").alias("temp_max"),
        F.sum("precip_sum").alias("precip_sum"),
        F.countDistinct("dt").alias("n_days"),
        # Sommes nécessaires au calcul de la pente de régression.
        F.sum("_x").alias("_sum_x"),
        F.sum("temp_avg").alias("_sum_y"),
        F.sum(F.col("_x") * F.col("temp_avg")).alias("_sum_xy"),
        F.sum(F.col("_x") * F.col("_x")).alias("_sum_x2"),
        F.count("dt").alias("_n"),
    )

    denominator = F.col("_n") * F.col("_sum_x2") - F.col("_sum_x") * F.col("_sum_x")
    numerator = F.col("_n") * F.col("_sum_xy") - F.col("_sum_x") * F.col("_sum_y")
    weekly = weekly.withColumn(
        "trend_slope",
        F.when(denominator != 0.0, numerator / denominator).otherwise(0.0),
    )

    # Écart par rapport à la semaine précédente (même ville / même source).
    week_window = Window.partitionBy("city", "source").orderBy("year", "week")
    weekly = weekly.withColumn(
        "temp_vs_prev_week",
        F.col("temp_avg") - F.lag("temp_avg").over(week_window),
    )

    return weekly.select(
        "year", "week", "city", "source",
        "temp_avg", "temp_min", "temp_max", "precip_sum",
        "n_days", "trend_slope", "temp_vs_prev_week",
    )


def compute_extreme_events(daily: "DataFrame", thresholds: Dict[str, float]) -> "DataFrame":
    """Détecte les événements extrêmes à partir des agrégats quotidiens."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType

    event_schema = ArrayType(StructType([
        StructField("event_type", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("threshold", DoubleType(), True),
        StructField("detail", StringType(), True),
    ]))

    def _classify_row(row) -> List[dict]:
        data = row.asDict() if hasattr(row, "asDict") else dict(row)
        return classify_extreme(data, thresholds)

    classify_udf = F.udf(_classify_row, event_schema)

    events = daily.withColumn(
        "events",
        classify_udf(F.struct("city", "dt", "temp_max", "temp_min", "precip_sum", "wind_avg")),
    )
    events = events.filter(F.size("events") > 0)
    events = events.withColumn("event", F.explode("events"))

    return events.select(
        F.col("dt"),
        F.col("city"),
        F.col("source"),
        F.col("event").getField("event_type").alias("event_type"),
        F.col("event").getField("severity").alias("severity"),
        F.col("event").getField("value").alias("value"),
        F.col("event").getField("threshold").alias("threshold"),
        F.col("event").getField("detail").alias("detail"),
    )


def compute_climate_profile(daily: "DataFrame") -> "DataFrame":
    """
    Construit le « profil météo » mensuel par (city, month).

    Indicateurs : normales de température (moyenne / min / max), amplitude
    thermique moyenne, cumul de précipitations moyen, nombre de jours de pluie
    et leur proportion, vent moyen, saison et profondeur d'historique.
    Ce profil est l'analogue météo d'un profil client : il caractérise une
    ville indépendamment d'un jour donné.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType

    season_udf = F.udf(season_of_month, StringType())

    profile = (
        daily
        .withColumn("month", F.month("dt"))
        .withColumn("_is_rain_day", F.when(F.col("precip_sum") > 1.0, 1).otherwise(0))
        .withColumn("_amplitude", F.col("temp_max") - F.col("temp_min"))
        .groupBy("city", "month")
        .agg(
            F.avg("temp_avg").alias("temp_normal"),
            F.min("temp_min").alias("temp_min_record"),
            F.max("temp_max").alias("temp_max_record"),
            F.avg("_amplitude").alias("temp_amplitude_avg"),
            F.avg("precip_sum").alias("precip_avg"),
            F.sum("_is_rain_day").alias("rain_days"),
            F.avg("wind_avg").alias("wind_avg"),
            F.countDistinct("dt").alias("n_days"),
            F.countDistinct(F.year("dt")).alias("n_years"),
        )
    )

    return (
        profile
        .withColumn("season", season_udf(F.col("month")))
        .withColumn(
            "rain_day_ratio",
            F.when(F.col("n_days") > 0, F.round(F.col("rain_days") / F.col("n_days"), 3))
            .otherwise(F.lit(0.0)),
        )
        .select(
            "city", "month", "season", "temp_normal", "temp_min_record",
            "temp_max_record", "temp_amplitude_avg", "precip_avg",
            "rain_days", "rain_day_ratio", "wind_avg", "n_days", "n_years",
        )
    )


# ---------------------------------------------------------------------------
# Écritures Gold
# ---------------------------------------------------------------------------

def gold_key(table: str, partition: str) -> str:
    """Cle de checkpoint d'une partition Gold : ``<table>:<dt>``."""
    return f"{table}:{partition}"


def write_daily_aggregates(spark: "SparkSession", daily: "DataFrame") -> None:
    """
    Écrit daily_aggregates (partition dt) + _SUCCESS par dt.

    Marqueurs deposes nativement (FileSystem Hadoop de la JVM) et checkpoint
    ecrit en un seul aller-retour : la boucle precedente faisait, par
    partition, un appel WebHDFS ET une relecture-reecriture complete de l'etat.
    """
    import checkpoint
    import silver_transform

    path = f"{hdfs_base()}/gold/meteo/daily_aggregates"
    dts = [r.dt.strftime("%Y-%m-%d") for r in daily.select("dt").distinct().orderBy("dt").collect()]
    _write_parquet_dynamic(daily, path, ["dt"])

    written = silver_transform.write_success_markers(spark, path, dts)
    checkpoint.mark_many(checkpoint.STAGE_GOLD,
                         [gold_key("daily_aggregates", d) for d in dts])
    logger.info("daily_aggregates ecrit (%d partition(s) dt, %d marqueur(s)).",
                len(dts), written)


def write_weekly_trends(weekly: "DataFrame") -> None:
    """Écrit weekly_trends (partition year/week) + _SUCCESS racine."""
    path = f"{hdfs_base()}/gold/meteo/weekly_trends"
    _write_parquet_dynamic(weekly, path, ["year", "week"])
    logger.info("weekly_trends écrit.")


def write_extreme_events(extreme: "DataFrame") -> None:
    """Écrit extreme_events (partition dt) + _SUCCESS racine."""
    path = f"{hdfs_base()}/gold/meteo/extreme_events"
    _write_parquet_dynamic(extreme, path, ["dt"])
    logger.info("extreme_events écrit.")


def write_climate_profile(profile: "DataFrame") -> None:
    """Écrit climate_profile (partition month) + _SUCCESS racine."""
    path = f"{hdfs_base()}/gold/meteo/climate_profile"
    _write_parquet_dynamic(profile, path, ["month"])
    logger.info("climate_profile écrit.")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run(args: argparse.Namespace) -> None:
    """Exécute le pipeline Silver -> Gold."""
    spark = build_spark_session("Silver vers Gold")
    try:
        silver = read_silver(spark)
        if silver is None:
            logger.info("Aucune donnée Silver disponible.")
            return

        from pyspark.sql import functions as F

        start = F.lit(args.start_date).cast("date")
        end = F.lit(args.end_date).cast("date")
        silver = silver.filter((F.col("dt") >= start) & (F.col("dt") <= end))

        # Fenêtre complète : les agrégats multi-jours (hebdo, mensuel) doivent
        # couvrir TOUT l'historique, pas seulement les nouvelles partitions.
        # Sans cela, --only-new recalculerait une semaine/mois sur un
        # sous-ensemble de jours et écraserait un agrégat déjà complet.
        full_silver = silver

        if args.only_new:
            # Ne recalculer que les dt dont daily_aggregates n'est pas deja
            # marque : sans cela, Gold recalculait TOUT a chaque execution.
            import checkpoint
            import silver_transform

            all_dts = [r.dt.strftime("%Y-%m-%d")
                       for r in silver.select("dt").distinct().orderBy("dt").collect()]
            # Un seul globStatus natif au lieu d'un appel WebHDFS par partition.
            already = silver_transform.existing_partitions(
                spark, f"{hdfs_base()}/gold/meteo/daily_aggregates")
            done = set(checkpoint.load(checkpoint.STAGE_GOLD).get("done", []))
            todo = [d for d in all_dts
                    if gold_key("daily_aggregates", d) not in done and d not in already]
            skipped = len(all_dts) - len(todo)
            if skipped:
                logger.info("Gold --only-new : %d partition(s) deja calculee(s), ignoree(s).",
                            skipped)
            if not todo:
                logger.info("Gold : rien de nouveau a calculer.")
                return
            silver = silver.filter(F.col("dt").cast("string").isin(todo))

        daily = compute_daily_aggregates(silver)
        daily.cache()
        # Source des agrégats hebdo / extrêmes / mensuel : l'historique complet.
        full_daily = daily if not args.only_new else compute_daily_aggregates(full_silver)
        if full_daily is not daily:
            full_daily.cache()

        import checkpoint

        run = checkpoint.run_id("gold")
        try:
            write_daily_aggregates(spark, daily)
            write_weekly_trends(compute_weekly_trends(full_daily))
            write_extreme_events(compute_extreme_events(full_daily, get_thresholds()))
            write_climate_profile(compute_climate_profile(full_daily))
            checkpoint.record_run(checkpoint.STAGE_GOLD, run, "success", tables=4)
        except Exception:
            checkpoint.record_run(checkpoint.STAGE_GOLD, run, "failed")
            raise
        finally:
            daily.unpersist()
            if full_daily is not daily:
                full_daily.unpersist()
    finally:
        spark.stop()


def parse_args(argv: Optional[List[str]]) -> argparse.Namespace:
    """Construit le parseur d'arguments CLI."""
    parser = argparse.ArgumentParser(description="Job Spark : Silver vers Gold (DataLake Météo)")
    parser.add_argument(
        "--start-date",
        default=os.environ.get("GOLD_START_DATE", "2022-01-01"),
        help="Borne inférieure du filtre sur dt (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date",
        default=os.environ.get("GOLD_END_DATE", "2025-12-31"),
        help="Borne supérieure du filtre sur dt (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--only-new",
        action="store_true",
        help="Ne recalculer que les partitions dt pas encore presentes en Gold.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args(argv)
    run(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
