# -*- coding: utf-8 -*-
"""
test_medallion.py : tests de la couche Medallion (Bronze -> Silver -> Gold).
============================================================================
Ces tests couvrent la chaîne complète **sans Spark ni HDFS** :

  * Bronze  : convention de partitionnement, construction des URLs
              Météo-France, idempotence du plan d'ingestion, génération et
              relecture d'un lot brut ``.csv.gz`` au schéma officiel ;
  * Silver  : fonctions pures de normalisation Météo-France, puis un
              équivalent pandas de la transformation Spark (validation de
              schéma, conversion, déduplication, indicateurs de fenêtre) ;
  * Gold    : profil climatique mensuel et détection d'événements extrêmes
              calculés sur le Silver produit ci-dessus.

La source batch est le jeu **« Données climatologiques de base - quotidiennes »**
de Météo-France (meteo.data.gouv.fr), la source temps réel est Open-Meteo.
"""

from __future__ import annotations

import csv
import datetime as dt
import gzip
import sys
from pathlib import Path

import pandas as pd
import pytest

import checkpoint
import gold_transform
import kafka_producer
import meteofrance_ingest as mf
import pipeline_ctl
import silver_transform
import verify_medallion


# ===========================================================================
# BRONZE : convention de dépôt et idempotence
# ===========================================================================

def test_normalize_department():
    assert mf.normalize_department("1") == "01"
    assert mf.normalize_department("75") == "75"
    assert mf.normalize_department("2a") == "2A"
    assert mf.normalize_department(" 971 ") == "971"
    with pytest.raises(ValueError):
        mf.normalize_department("  ")


def test_periods_for_years():
    # Fenêtre entièrement historique -> uniquement le fichier « previous ».
    assert mf.periods_for_years(2022, 2024) == [mf.PERIOD_PREVIOUS]
    # Fenêtre entièrement récente -> uniquement le fichier « latest ».
    assert mf.periods_for_years(2025, 2026) == [mf.PERIOD_LATEST]
    # Fenêtre à cheval -> les deux fichiers.
    assert mf.periods_for_years(2022, 2026) == [mf.PERIOD_PREVIOUS, mf.PERIOD_LATEST]
    with pytest.raises(ValueError):
        mf.periods_for_years(2026, 2022)


def test_build_url_matches_meteo_data_gouv():
    url = mf.build_url("1", mf.PERIOD_LATEST)
    assert url.endswith("/Q_01_latest-2025-2026_RR-T-Vent.csv.gz")
    assert url.startswith(mf.DEFAULT_BASE_URL)
    # Une base personnalisée (miroir, cache local) est respectée.
    assert mf.build_url("75", mf.PERIOD_PREVIOUS, "http://miroir/QUOT/") == (
        "http://miroir/QUOT/Q_75_previous-1950-2024_RR-T-Vent.csv.gz"
    )


def test_bronze_partition_convention():
    """La convention imposée par le sujet : source=X/year=YYYY/month=MM."""
    day = dt.date(2026, 8, 27)
    assert mf.bronze_partition(day) == (
        "/bronze/meteo/batch/source=meteofrance/year=2026/month=08"
    )
    assert mf.bronze_target("75", mf.PERIOD_LATEST, day) == (
        "/bronze/meteo/batch/source=meteofrance/year=2026/month=08"
        "/Q_75_latest-2025-2026_RR-T-Vent.csv.gz"
    )
    # Le mois est toujours sur deux chiffres.
    assert mf.bronze_partition(dt.date(2026, 1, 5)).endswith("month=01")


def test_plan_batches_is_idempotent():
    """Relancer l'ingestion ne replanifie jamais un lot déjà présent."""
    plan = mf.plan_batches(["75", "69"], 2022, 2026)
    assert len(plan) == 4  # 2 départements x 2 périodes
    assert ("75", mf.PERIOD_LATEST) in plan

    # Après une première exécution complète, le plan est vide.
    done = {mf.batch_key(dep, period) for dep, period in plan}
    assert mf.plan_batches(["75", "69"], 2022, 2026, done) == []

    # Interruption au milieu : seuls les lots manquants sont replanifiés.
    partial = {mf.batch_key("75", mf.PERIOD_PREVIOUS)}
    replan = mf.plan_batches(["75", "69"], 2022, 2026, partial)
    assert mf.batch_key("75", mf.PERIOD_PREVIOUS) not in {
        mf.batch_key(d, p) for d, p in replan
    }
    assert len(replan) == 3


def test_batch_key_and_file_name_normalize():
    assert mf.batch_key("1", mf.PERIOD_LATEST) == f"01:{mf.PERIOD_LATEST}"
    assert mf.file_name("2b", mf.PERIOD_PREVIOUS) == (
        f"Q_2B_{mf.PERIOD_PREVIOUS}_RR-T-Vent.csv.gz"
    )


def test_synthetic_rows_follow_official_schema():
    rows = mf.synthetic_rows("75", dt.date(2025, 1, 1), dt.date(2025, 1, 31), seed=7)
    assert len(rows) == 31
    assert all(len(row) == len(mf.MF_COLUMNS) for row in rows)

    header_index = {name: i for i, name in enumerate(mf.MF_COLUMNS)}
    first = rows[0]
    assert first[header_index["AAAAMMJJ"]] == "20250101"
    assert first[header_index["NOM_USUEL"]] == "PARIS-MONTSOURIS"
    # TN <= TX sur toutes les lignes générées.
    for row in rows:
        tn = float(row[header_index["TN"]])
        tx = float(row[header_index["TX"]])
        assert tn <= tx


def test_write_synthetic_file_roundtrip(tmp_path):
    """Le lot déposé en Bronze est un .csv.gz relisible au schéma Météo-France."""
    target = tmp_path / mf.file_name("75", mf.PERIOD_LATEST)
    mf.write_synthetic_file(str(target), "75",
                            dt.date(2025, 3, 1), dt.date(2025, 3, 10), seed=3)
    assert target.exists()

    with gzip.open(target, "rt", encoding="utf-8") as handle:
        reader = csv.reader(handle, delimiter=mf.CSV_SEPARATOR)
        header = next(reader)
        rows = list(reader)
    assert header == mf.MF_COLUMNS
    assert len(rows) == 10


def test_manifest_entry_documents_the_batch(tmp_path):
    target = tmp_path / mf.file_name("69", mf.PERIOD_LATEST)
    mf.write_synthetic_file(str(target), "69",
                            dt.date(2025, 1, 1), dt.date(2025, 1, 5), seed=9)
    entry = mf.manifest_entry("69", mf.PERIOD_LATEST, str(target), dt.date(2026, 8, 27))
    assert entry["department"] == "69"
    assert entry["partition"] == "year=2026/month=08"
    assert entry["bytes"] > 0
    assert entry["source_url"].endswith("Q_69_latest-2025-2026_RR-T-Vent.csv.gz")


# ===========================================================================
# SILVER : fonctions pures de normalisation Météo-France
# ===========================================================================

def test_mf_parse_number_handles_empty_and_comma():
    assert silver_transform.mf_parse_number("12.4") == 12.4
    assert silver_transform.mf_parse_number("12,4") == 12.4
    assert silver_transform.mf_parse_number(-3) == -3.0
    # Les mesures manquantes sont des champs VIDES chez Météo-France.
    assert silver_transform.mf_parse_number("") is None
    assert silver_transform.mf_parse_number("   ") is None
    assert silver_transform.mf_parse_number(None) is None
    assert silver_transform.mf_parse_number("n/a") is None
    assert silver_transform.mf_parse_number(float("nan")) is None


def test_mf_parse_date():
    assert silver_transform.mf_parse_date("20250115") == "2025-01-15"
    assert silver_transform.mf_parse_date(20250115) == "2025-01-15"
    assert silver_transform.mf_parse_date("2025-01-15") == "2025-01-15"
    assert silver_transform.mf_parse_date("") is None
    assert silver_transform.mf_parse_date(None) is None
    assert silver_transform.mf_parse_date("2025") is None
    assert silver_transform.mf_parse_date("20251315") is None  # mois 13


def test_mf_station_id():
    assert silver_transform.mf_station_id("75114001") == "MF_75114001"
    assert silver_transform.mf_station_id(7511) == "MF_00007511"
    assert silver_transform.mf_station_id("") == ""
    assert silver_transform.mf_station_id(None) == ""


def test_mf_city_name_aligns_with_openmeteo_cities():
    """Le nom de poste est réduit à la ville, pour recouper le flux temps réel."""
    assert silver_transform.mf_city_name("PARIS-MONTSOURIS") == "Paris"
    assert silver_transform.mf_city_name("LYON-BRON") == "Lyon"
    assert silver_transform.mf_city_name("BORDEAUX-MERIGNAC") == "Bordeaux"
    assert silver_transform.mf_city_name("LILLE LESQUIN") == "Lille"
    assert silver_transform.mf_city_name("NICE") == "Nice"
    # Les préfixes composés ne sont pas des séparateurs de site.
    assert silver_transform.mf_city_name("SAINT-BRIEUC") == "Saint-Brieuc"
    assert silver_transform.mf_city_name("LE HAVRE") == "Le Havre"
    assert silver_transform.mf_city_name("") == ""
    assert silver_transform.mf_city_name(None) == ""


def test_mf_mean_temperature():
    # TM renseignée : utilisée telle quelle.
    assert silver_transform.mf_mean_temperature("9.9", "5", "15") == 9.9
    # TM vide : demi-somme TN/TX.
    assert silver_transform.mf_mean_temperature("", "5.0", "15.0") == 10.0
    # Une seule borne disponible.
    assert silver_transform.mf_mean_temperature("", "5.0", "") == 5.0
    assert silver_transform.mf_mean_temperature("", "", "15.0") == 15.0
    # Rien d'exploitable.
    assert silver_transform.mf_mean_temperature("", "", "") is None


def test_meteofrance_schema_validation():
    """Un fichier amputé d'une colonne obligatoire doit être détecté."""
    required = silver_transform.METEOFRANCE_REQUIRED_COLUMNS
    assert silver_transform.validate_required_columns(mf.MF_COLUMNS, required) == []

    truncated = [c for c in mf.MF_COLUMNS if c not in ("TX", "RR")]
    assert sorted(silver_transform.validate_required_columns(truncated, required)) == [
        "RR", "TX",
    ]


# ===========================================================================
# SILVER : équivalent pandas de la transformation Spark
# ===========================================================================

def _read_bronze_batch(path: str) -> pd.DataFrame:
    """Relit un lot Bronze brut (.csv.gz Météo-France) tel que Spark le lirait."""
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return pd.read_csv(handle, sep=mf.CSV_SEPARATOR, dtype=str)


def _normalize_meteofrance(raw: pd.DataFrame) -> pd.DataFrame:
    """Réplique ``silver_transform.transform_meteofrance`` avec pandas."""
    missing = silver_transform.validate_required_columns(
        list(raw.columns), silver_transform.METEOFRANCE_REQUIRED_COLUMNS)
    if missing:
        raise ValueError(f"Colonnes Météo-France manquantes : {missing}")

    def column(name):
        return raw[name] if name in raw.columns else pd.Series([None] * len(raw))

    out = pd.DataFrame({
        "station_id": raw["NUM_POSTE"].map(silver_transform.mf_station_id),
        "station_name": raw["NOM_USUEL"],
        "city": raw["NOM_USUEL"].map(silver_transform.mf_city_name),
        "country": "FR",
        "latitude": raw["LAT"].map(silver_transform.mf_parse_number),
        "longitude": raw["LON"].map(silver_transform.mf_parse_number),
        "elevation": raw["ALTI"].map(silver_transform.mf_parse_number),
        "timestamp": raw["AAAAMMJJ"].map(silver_transform.mf_parse_date),
        "temperature": [
            silver_transform.mf_mean_temperature(tm, tn, tx)
            for tm, tn, tx in zip(column("TM"), raw["TN"], raw["TX"])
        ],
        "precipitation": raw["RR"].map(silver_transform.mf_parse_number),
        "wind_speed": column("FFM").map(silver_transform.mf_parse_number),
        "snow": column("NEIGETOT").map(silver_transform.mf_parse_number),
        "source": "METEOFRANCE",
    })
    out["dt"] = out["timestamp"]
    return out[silver_transform.SILVER_SCHEMA]


@pytest.fixture()
def silver_frame(tmp_path) -> pd.DataFrame:
    """Bronze synthétique -> Silver normalisé + dédupliqué (2 villes, 1 an)."""
    frames = []
    for dep, seed in (("75", 11), ("13", 12)):
        path = tmp_path / mf.file_name(dep, mf.PERIOD_LATEST)
        mf.write_synthetic_file(str(path), dep,
                                dt.date(2025, 1, 1), dt.date(2025, 12, 31), seed=seed)
        frames.append(_normalize_meteofrance(_read_bronze_batch(str(path))))
    silver = pd.concat(frames, ignore_index=True)
    silver = silver[silver["timestamp"].notna()]
    return silver.drop_duplicates(subset=silver_transform.dedup_keys())


def test_silver_normalization_produces_the_unified_schema(silver_frame):
    assert list(silver_frame.columns) == silver_transform.SILVER_SCHEMA
    assert len(silver_frame) == 730  # 2 villes x 365 jours, aucun doublon
    assert set(silver_frame["source"]) == {"METEOFRANCE"}
    assert set(silver_frame["city"]) == {"Paris", "Marseille"}
    assert silver_frame["station_id"].str.startswith("MF_").all()
    # Les champs vides du Bronze sont devenus des NULL, pas des 0.
    assert silver_frame["temperature"].notna().all()
    assert silver_frame["precipitation"].isna().any()


def test_silver_deduplication_on_station_and_timestamp(silver_frame):
    """La dédup (station_id, timestamp) élimine un lot réingéré deux fois."""
    doubled = pd.concat([silver_frame, silver_frame], ignore_index=True)
    assert len(doubled) == 2 * len(silver_frame)

    deduped = doubled.drop_duplicates(subset=silver_transform.dedup_keys())
    assert len(deduped) == len(silver_frame)
    assert deduped.duplicated(subset=silver_transform.dedup_keys()).sum() == 0


def test_silver_indicators_are_rolling_windows(silver_frame):
    """temp_ma3 / temp_ma7 / temp_std7 / temp_anomaly, par station et par date."""
    frame = silver_frame.sort_values(["station_id", "timestamp"]).copy()
    grouped = frame.groupby("station_id")["temperature"]
    frame["temp_ma3"] = grouped.transform(lambda s: s.rolling(3, min_periods=1).mean())
    frame["temp_ma7"] = grouped.transform(lambda s: s.rolling(7, min_periods=1).mean())
    frame["temp_std7"] = grouped.transform(lambda s: s.rolling(7, min_periods=2).std())
    frame["temp_anomaly"] = frame["temperature"] - frame["temp_ma7"]

    for column in silver_transform.INDICATOR_COLUMNS:
        assert column in frame.columns

    first = frame.iloc[0]
    assert first["temp_ma3"] == pytest.approx(first["temperature"])
    assert first["temp_anomaly"] == pytest.approx(0.0)
    # La moyenne mobile 7j du 7e jour est bien la moyenne des 7 valeurs.
    station = frame[frame["station_id"] == first["station_id"]].head(7)
    assert station.iloc[6]["temp_ma7"] == pytest.approx(station["temperature"].mean())


# ===========================================================================
# GOLD : agrégats calculés sur le Silver
# ===========================================================================

def _daily_aggregates(silver: pd.DataFrame) -> pd.DataFrame:
    """Réplique ``gold_transform.compute_daily_aggregates`` avec pandas."""
    return (
        silver
        .groupby(["dt", "city", "source"], as_index=False)
        .agg(
            n_obs=("temperature", "size"),
            temp_avg=("temperature", "mean"),
            temp_min=("temperature", "min"),
            temp_max=("temperature", "max"),
            precip_sum=("precipitation", "sum"),
            wind_avg=("wind_speed", "mean"),
        )
    )


def test_gold_daily_aggregates_cover_every_city_day(silver_frame):
    daily = _daily_aggregates(silver_frame)
    assert len(daily) == 730
    assert (daily["n_obs"] == 1).all()
    assert (daily["temp_min"] <= daily["temp_max"]).all()


def test_season_of_month():
    assert gold_transform.season_of_month(12) == "hiver"
    assert gold_transform.season_of_month(1) == "hiver"
    assert gold_transform.season_of_month(4) == "printemps"
    assert gold_transform.season_of_month("7") == "ete"
    assert gold_transform.season_of_month(10) == "automne"
    assert gold_transform.season_of_month(0) == ""
    assert gold_transform.season_of_month(None) == ""
    assert gold_transform.season_of_month("juillet") == ""


def test_rain_day_ratio():
    assert gold_transform.rain_day_ratio(9, 30) == 0.3
    assert gold_transform.rain_day_ratio(0, 30) == 0.0
    assert gold_transform.rain_day_ratio(30, 30) == 1.0
    # Dénominateur invalide ou entrée non numérique -> 0.0, jamais d'exception.
    assert gold_transform.rain_day_ratio(1, 0) == 0.0
    assert gold_transform.rain_day_ratio(1, -5) == 0.0
    assert gold_transform.rain_day_ratio(None, 5) == 0.0
    # Le ratio reste borné à 1.0.
    assert gold_transform.rain_day_ratio(40, 30) == 1.0


def test_gold_climate_profile(silver_frame):
    """Le « profil météo » mensuel : 12 mois par ville, saisons cohérentes."""
    daily = _daily_aggregates(silver_frame)
    daily["month"] = pd.to_datetime(daily["dt"]).dt.month
    daily["_rain"] = (daily["precip_sum"] > 1.0).astype(int)

    profile = daily.groupby(["city", "month"], as_index=False).agg(
        temp_normal=("temp_avg", "mean"),
        temp_min_record=("temp_min", "min"),
        temp_max_record=("temp_max", "max"),
        precip_avg=("precip_sum", "mean"),
        rain_days=("_rain", "sum"),
        n_days=("dt", "nunique"),
    )
    profile["season"] = profile["month"].map(gold_transform.season_of_month)
    profile["rain_day_ratio"] = [
        gold_transform.rain_day_ratio(r, n)
        for r, n in zip(profile["rain_days"], profile["n_days"])
    ]

    assert len(profile) == 24  # 2 villes x 12 mois
    assert set(profile["season"]) == {"hiver", "printemps", "ete", "automne"}
    assert profile["rain_day_ratio"].between(0.0, 1.0).all()
    assert (profile["temp_min_record"] <= profile["temp_max_record"]).all()
    assert profile["n_days"].sum() == 730

    # Le profil capture bien la saisonnalité : juillet plus chaud que janvier.
    for city in profile["city"].unique():
        city_profile = profile[profile["city"] == city].set_index("month")
        assert city_profile.loc[7, "temp_normal"] > city_profile.loc[1, "temp_normal"]


def test_gold_extreme_events_on_silver(silver_frame):
    """classify_extreme appliqué aux agrégats quotidiens issus du Silver."""
    daily = _daily_aggregates(silver_frame)
    thresholds = gold_transform.get_thresholds()

    # Aucun événement avec les seuils par défaut sur des données tempérées.
    events = [
        event
        for row in daily.to_dict("records")
        for event in gold_transform.classify_extreme(row, thresholds)
    ]
    assert all(e["event_type"] in {
        "canicule", "fortes_pluies", "vents_violents", "vague_de_froid"
    } for e in events)

    # Avec un seuil de canicule abaissé, les jours d'été sont détectés.
    lowered = dict(thresholds, heatwave=20.0)
    detected = [
        event
        for row in daily.to_dict("records")
        for event in gold_transform.classify_extreme(row, lowered)
        if event["event_type"] == "canicule"
    ]
    assert detected, "Des jours au-dessus de 20 °C devraient être détectés."
    assert all(e["value"] >= 20.0 for e in detected)
    assert all(e["severity"] in {"alerte", "extreme"} for e in detected)


# ===========================================================================
# VÉRIFICATION AUTOMATIQUE : contrat de `make verify`
# ===========================================================================

def _result(path="/silver/meteo", layer="SILVER", required=True, exists=True,
            has_success=True, size=1024, expect_success=True, expect_data=True):
    return {
        "layer": layer, "path": path, "required": required,
        "expect_success": expect_success, "expect_data": expect_data,
        "exists": exists, "has_success": has_success, "bytes": size,
    }


def test_expected_checks_cover_the_three_layers():
    checks = verify_medallion.expected_checks()
    paths = [c.path for c in checks]
    layers = {c.layer for c in checks}

    assert layers == {"BRONZE", "SILVER", "GOLD"}
    # Les deux sources hétérogènes exigées par le sujet.
    assert "/bronze/meteo/batch/source=meteofrance" in paths
    assert "/bronze/meteo/stream/source=openmeteo" in paths
    assert "/silver/meteo" in paths
    # Les quatre tables Gold produites par gold_transform.
    for table in ("daily_aggregates", "weekly_trends", "extreme_events", "climate_profile"):
        assert f"/gold/meteo/{table}" in paths
    # Les couches du TP sont toutes obligatoires ; seules les tables issues du
    # bonus ML/GenAI sont facultatives tant que --with-ml n'est pas demande.
    bonus = {"/gold/meteo/ml_predictions", "/gold/meteo/ai_insights"}
    assert all(c.required for c in checks if c.path not in bonus)
    assert all(not c.required for c in checks if c.path in bonus)


def test_expected_checks_allow_empty_stream_on_a_fresh_cluster():
    """Sur un cluster neuf, aucune fenêtre horaire n'est encore close."""
    checks = {c.path: c for c in verify_medallion.expected_checks(allow_empty_stream=True)}
    stream = checks["/bronze/meteo/stream/source=openmeteo"]
    assert stream.required is False
    # Les couches Bronze batch, Silver et Gold restent obligatoires.
    assert checks["/bronze/meteo/batch/source=meteofrance"].required is True
    assert checks["/silver/meteo"].required is True
    assert checks["/gold/meteo/climate_profile"].required is True


def test_verdict_and_failure_rules():
    assert verify_medallion.verdict(_result()) == "OK"
    assert verify_medallion.verdict(_result(exists=False)) == "ABSENT"
    assert verify_medallion.verdict(_result(has_success=False)) == "SANS _SUCCESS"
    assert verify_medallion.verdict(_result(size=0)) == "VIDE"
    # Un contrôle facultatif absent est ignoré, jamais un échec.
    assert verify_medallion.verdict(_result(exists=False, required=False)) == "IGNORÉ"

    assert verify_medallion.is_failure(_result()) is False
    assert verify_medallion.is_failure(_result(exists=False)) is True
    assert verify_medallion.is_failure(_result(size=0)) is True
    assert verify_medallion.is_failure(_result(exists=False, required=False)) is False


def test_format_size():
    assert verify_medallion.format_size(0) == "0 o"
    assert verify_medallion.format_size(512) == "512 o"
    assert verify_medallion.format_size(1536) == "1.5 Ko"
    assert verify_medallion.format_size(3 * 1024 ** 2) == "3.0 Mo"
    assert verify_medallion.format_size(5 * 1024 ** 3) == "5.0 Go"
    assert verify_medallion.format_size(None) == "-"


def test_render_report_lists_every_layer():
    results = [_result(layer="BRONZE", path="/bronze/meteo/batch/source=meteofrance"),
               _result(layer="GOLD", path="/gold/meteo/climate_profile", size=0)]
    report = verify_medallion.render_report(results)
    assert "BRONZE" in report and "GOLD" in report
    assert "/bronze/meteo/batch/source=meteofrance" in report
    assert "OK" in report and "VIDE" in report


# ===========================================================================
# ORCHESTRATION : contrat multiplateforme de `make all`
# ===========================================================================

def test_pipeline_dags_follow_the_medallion_order():
    """La chaîne attendue est Bronze -> Silver -> Gold, dans cet ordre."""
    assert pipeline_ctl.PIPELINE_DAGS == [
        "dag_bronze_ingest", "dag_silver_transform", "dag_gold_aggregate",
    ]
    # Le DAG de réentraînement ML est hors chaîne (bonus, hebdomadaire).
    assert "dag_ml_retrain" in pipeline_ctl.ALL_DAGS
    assert "dag_ml_retrain" not in pipeline_ctl.PIPELINE_DAGS
    assert set(pipeline_ctl.PIPELINE_DAGS) < set(pipeline_ctl.ALL_DAGS)


def test_hdfs_dirs_cover_both_sources_and_the_three_layers():
    dirs = pipeline_ctl.HDFS_DIRS
    for root in ("/bronze", "/silver", "/gold", "/models", "/checkpoints"):
        assert root in dirs
    # Les deux sources hétérogènes exigées par le sujet.
    assert "/bronze/meteo/batch/source=meteofrance" in dirs
    assert "/bronze/meteo/stream/source=openmeteo" in dirs
    # Les racines sont créées avant leurs sous-répertoires.
    assert dirs.index("/bronze") < dirs.index("/bronze/meteo/batch/source=meteofrance")


def test_compose_command_is_a_plain_argument_list():
    """Aucun shell n'est invoqué : la commande reste portable Windows/Unix."""
    command = pipeline_ctl.compose_command()
    assert command[:2] == ["docker", "compose"]
    assert "--env-file" in command and "-f" in command
    assert "--profile" not in command
    assert all(isinstance(part, str) for part in command)

    with_profile = pipeline_ctl.compose_command("genai")
    assert with_profile[-2:] == ["--profile", "genai"]


def test_missing_requirements():
    """`make deps` n'installe que ce qui manque vraiment."""
    assert pipeline_ctl.missing_requirements(["pytest", "pandas"]) == []
    assert pipeline_ctl.missing_requirements(
        ["paquet_qui_nexiste_pas_12345"]) == ["paquet_qui_nexiste_pas_12345"]
    assert pipeline_ctl.missing_requirements([]) == []


def test_namenode_url_bridges_host_and_container(monkeypatch):
    """
    La meme fonction doit produire l'URL correcte des deux cotes de Docker.

    Sur l'hote, le Namenode n'est joignable que via le port publie
    (localhost:9870) ; dans un conteneur, via le nom de service Docker.
    """
    monkeypatch.delenv("NAMENODE_URL", raising=False)
    monkeypatch.delenv("HDFS_NAMENODE", raising=False)
    monkeypatch.delenv("HDFS_WEBHDFS_PORT", raising=False)
    assert pipeline_ctl.namenode_url() == "http://localhost:9870"

    monkeypatch.setenv("HDFS_NAMENODE", "namenode")
    assert pipeline_ctl.namenode_url() == "http://namenode:9870"

    monkeypatch.setenv("HDFS_WEBHDFS_PORT", "50070")
    assert pipeline_ctl.namenode_url() == "http://namenode:50070"

    # Une surcharge explicite l'emporte toujours, barre finale ignoree.
    monkeypatch.setenv("NAMENODE_URL", "http://autre:9870/")
    assert pipeline_ctl.namenode_url() == "http://autre:9870"


def test_airflow_command_depends_on_the_execution_context(monkeypatch):
    """
    Dans le conteneur Airflow, la CLI est appelee directement : aucun client
    Docker n'y est installe. Depuis l'hote, elle passe par docker compose exec.
    """
    captured = {}

    def fake_run(command, check=False, capture=False, timeout=None):
        captured["command"] = list(command)
        import subprocess as sp
        return sp.CompletedProcess(list(command), 0, "", "")

    monkeypatch.setattr(pipeline_ctl, "run", fake_run)

    monkeypatch.setattr(pipeline_ctl, "IN_CONTAINER", True)
    pipeline_ctl.airflow(["dags", "list"])
    assert captured["command"] == ["airflow", "dags", "list"]

    monkeypatch.setattr(pipeline_ctl, "IN_CONTAINER", False)
    pipeline_ctl.airflow(["dags", "list"])
    assert captured["command"][:2] == ["docker", "compose"]
    assert captured["command"][-4:] == ["airflow-webserver", "airflow", "dags", "list"]


# ===========================================================================
# CHECKPOINTS : reprise fine, unite de travail par unite de travail
# ===========================================================================

@pytest.fixture()
def cp(tmp_path, monkeypatch):
    """Magasin de checkpoints isole, sur le backend fichier."""
    monkeypatch.setenv("METEO_CHECKPOINT_BACKEND", "file")
    monkeypatch.setenv("METEO_CHECKPOINT_DIR", str(tmp_path / "cp"))
    return checkpoint


def test_state_schema_is_resilient_to_garbage():
    """Un checkpoint corrompu ne doit jamais faire echouer le traitement."""
    for garbage in (None, "", 42, [], {"done": "pas une liste"}):
        state = checkpoint.normalize_state("silver", garbage)
        assert state["stage"] == "silver"
        assert state["done"] == []
        assert state["runs"] == []

    # Les cles deja presentes sont conservees et dedupliquees.
    state = checkpoint.normalize_state("silver",
                                       {"done": ["b", "a", "a", "", "  "], "runs": [{"x": 1}]})
    assert state["done"] == ["a", "b"]
    assert state["runs"] == [{"x": 1}]


def test_mark_and_pending_are_pure_and_idempotent():
    state = checkpoint.new_state("silver")
    assert checkpoint.pending(state, ["d1", "d2", "d3"]) == ["d1", "d2", "d3"]

    marked = checkpoint.mark_done_in(state, "d2")
    # Fonction PURE : l'etat d'origine n'est pas modifie.
    assert checkpoint.is_done_in(state, "d2") is False
    assert checkpoint.is_done_in(marked, "d2") is True
    # Marquer deux fois ne change rien.
    assert checkpoint.mark_done_in(marked, "d2")["done"] == marked["done"]
    assert checkpoint.pending(marked, ["d1", "d2", "d3"]) == ["d1", "d3"]
    # Les doublons de la liste d'entree sont ecartes, l'ordre est conserve.
    assert checkpoint.pending(marked, ["d3", "d1", "d3"]) == ["d3", "d1"]
    # forget_in permet de rejouer une unite.
    assert checkpoint.is_done_in(checkpoint.forget_in(marked, "d2"), "d2") is False


def test_run_journal_is_bounded():
    """Le journal des runs ne doit pas croitre sans fin."""
    state = checkpoint.new_state("gold")
    for i in range(checkpoint.MAX_RUNS + 15):
        state = checkpoint.append_run(state, {"run_id": f"run-{i}", "status": "success"})
    assert len(state["runs"]) == checkpoint.MAX_RUNS
    # Ce sont les plus RECENTS qui sont conserves.
    assert state["runs"][-1]["run_id"] == f"run-{checkpoint.MAX_RUNS + 14}"


def test_checkpoint_roundtrip_on_disk(cp):
    cp.reset("silver")
    assert cp.pending_keys("silver", ["2025-01-01", "2025-01-02"]) == ["2025-01-01", "2025-01-02"]

    assert cp.mark_done("silver", "2025-01-01") is True
    # Relu depuis le disque : la marque a bien ete committee.
    assert cp.is_done("silver", "2025-01-01") is True
    assert cp.pending_keys("silver", ["2025-01-01", "2025-01-02"]) == ["2025-01-02"]

    cp.record_run("silver", "run-1", "success", partitions_written=1)
    resume = cp.summarize_state(cp.load("silver"))
    assert resume["done"] == 1 and resume["last_status"] == "success"


def test_interrupted_ingestion_resumes_where_it_stopped(cp):
    """
    Le scenario qui motive tout ce module.

    10 lots a ingerer, coupure apres le 7e : la reprise ne doit traiter que
    les 3 restants, et surtout jamais re-ingerer les 7 premiers.
    """
    cp.reset(cp.STAGE_BRONZE)
    lots = [mf.batch_key(dep, period)
            for dep in ("75", "69", "13", "33", "59")
            for period in (mf.PERIOD_PREVIOUS, mf.PERIOD_LATEST)]
    assert len(lots) == 10

    traites = []
    for lot in cp.pending_keys(cp.STAGE_BRONZE, lots):
        if len(traites) == 7:
            break  # coupure brutale (Ctrl-C, OOM, machine qui s'eteint)
        traites.append(lot)
        cp.mark_done(cp.STAGE_BRONZE, lot)  # commit IMMEDIAT, lot par lot
    assert len(traites) == 7

    restants = cp.pending_keys(cp.STAGE_BRONZE, lots)
    assert len(restants) == 3
    assert not set(restants) & set(traites)

    for lot in restants:
        cp.mark_done(cp.STAGE_BRONZE, lot)
    assert cp.pending_keys(cp.STAGE_BRONZE, lots) == []
    # Total ingere = 10, aucun doublon.
    assert len(cp.load(cp.STAGE_BRONZE)["done"]) == 10


def test_gold_keys_are_scoped_per_table():
    """Une meme dt peut etre faite pour une table Gold et pas pour une autre."""
    assert gold_transform.gold_key("daily_aggregates", "2025-01-01") == (
        "daily_aggregates:2025-01-01")
    assert gold_transform.gold_key("climate_profile", "2025-01-01") != (
        gold_transform.gold_key("daily_aggregates", "2025-01-01"))


def test_reset_replays_everything(cp):
    cp.reset(cp.STAGE_GOLD)
    cp.mark_done(cp.STAGE_GOLD, gold_transform.gold_key("daily_aggregates", "2025-01-01"))
    assert cp.pending_keys(cp.STAGE_GOLD, ["daily_aggregates:2025-01-01"]) == []
    cp.reset(cp.STAGE_GOLD)
    assert cp.pending_keys(cp.STAGE_GOLD, ["daily_aggregates:2025-01-01"]) == [
        "daily_aggregates:2025-01-01"]


def test_checkpoint_write_failure_never_raises(cp, monkeypatch):
    """Un checkpoint est une optimisation : son echec ne casse pas le run."""
    def boom(*args, **kwargs):
        raise OSError("disque plein")

    monkeypatch.setattr(checkpoint.Path, "mkdir", boom)
    assert checkpoint.mark_done("silver", "2025-01-01") is False  # signale, mais pas d'exception


# ===========================================================================
# `make all` : complet ET reprenable
# ===========================================================================

def test_workflow_is_a_known_checkpoint_stage():
    """Les etapes de `make all` sont checkpointees comme les donnees."""
    assert checkpoint.STAGE_WORKFLOW == "workflow"
    assert checkpoint.STAGE_WORKFLOW in checkpoint.KNOWN_STAGES
    # Les trois etapes de donnees restent presentes.
    for stage in (checkpoint.STAGE_BRONZE, checkpoint.STAGE_SILVER, checkpoint.STAGE_GOLD):
        assert stage in checkpoint.KNOWN_STAGES


def test_make_all_resumes_where_it_stopped(cp):
    """
    `make all` doit pouvoir etre relance en boucle sans tout refaire.

    Coupure pendant 'pipeline' : au passage suivant, init/unpause sont
    sautes et l'on reprend a 'pipeline' (Bronze -> Silver -> Gold -> ML).
    """
    etapes = ["init", "unpause", "pipeline"]
    cp.reset(cp.STAGE_WORKFLOW)

    executees = []
    for etape in cp.pending_keys(cp.STAGE_WORKFLOW, etapes):
        if etape == "pipeline":
            break  # coupure : le cluster tombe pendant la chaine Medallion + ML
        executees.append(etape)
        cp.mark_done(cp.STAGE_WORKFLOW, etape)
    assert executees == ["init", "unpause"]

    # Deuxieme `make all` : on reprend exactement la ou l'on s'etait arrete.
    reprise = cp.pending_keys(cp.STAGE_WORKFLOW, etapes)
    assert reprise == ["pipeline"]
    for etape in reprise:
        cp.mark_done(cp.STAGE_WORKFLOW, etape)

    # Troisieme `make all` : plus rien a faire, il est simplement vert.
    assert cp.pending_keys(cp.STAGE_WORKFLOW, etapes) == []


def test_force_replays_a_completed_step(cp, monkeypatch):
    """FORCE=1 doit rejouer une etape pourtant marquee terminee."""
    monkeypatch.setattr(pipeline_ctl, "IN_CONTAINER", True)
    cp.reset(cp.STAGE_WORKFLOW)
    cp.mark_done(cp.STAGE_WORKFLOW, "pipeline")

    assert pipeline_ctl.step_done("pipeline") is True
    assert pipeline_ctl.skip_if_done("pipeline", force=False) is True
    assert pipeline_ctl.skip_if_done("pipeline", force=True) is False
    # Une etape jamais faite n'est jamais sautee.
    assert pipeline_ctl.skip_if_done("unpause", force=False) is False


def test_step_guard_is_inert_outside_a_container(monkeypatch):
    """
    Hors conteneur, HDFS est injoignable : la garde ne doit rien casser.

    Elle repond 'pas fait' : l'etape est rejouee, ce qui est sans danger
    puisque chaque etape est idempotente.
    """
    monkeypatch.setattr(pipeline_ctl, "IN_CONTAINER", False)
    assert pipeline_ctl.step_done("pipeline") is False
    assert pipeline_ctl.skip_if_done("pipeline", force=False) is False
    pipeline_ctl.complete_step("pipeline")  # ne doit pas lever


def test_ml_tables_required_only_after_a_full_run():
    """
    ml_predictions et ai_insights sont exigees apres un `make all` complet,
    facultatives apres un simple `make pipeline`.
    """
    optional = {c.path: c for c in verify_medallion.expected_checks()}
    assert optional["/gold/meteo/ml_predictions"].required is False
    assert optional["/gold/meteo/ai_insights"].required is False

    full = {c.path: c for c in verify_medallion.expected_checks(with_ml=True)}
    assert full["/gold/meteo/ml_predictions"].required is True
    assert full["/gold/meteo/ai_insights"].required is True
    # Les couches de base restent obligatoires dans les deux cas.
    assert optional["/silver/meteo"].required is True


def test_inference_skips_cleanly_without_a_model(monkeypatch, tmp_path):
    """
    Sur un cluster neuf, aucun modele n'existe : l'inference doit se retirer
    proprement (code 0) au lieu de faire echouer tout le DAG Gold.
    """
    import types

    # inference.py importe joblib/numpy/pandas au niveau module : toujours
    # presents dans les images du projet, pas forcement sur l'hote nu.
    pytest.importorskip("joblib", reason="dependance ML absente hors conteneur")

    fake_hdfs = types.ModuleType("hdfs_utils")
    fake_hdfs.hdfs_exists = lambda path: False
    fake_hdfs.hdfs_list = lambda path: []
    monkeypatch.setitem(sys.modules, "hdfs_utils", fake_hdfs)

    ml_dir = Path(__file__).resolve().parent.parent / "ml"
    monkeypatch.syspath_prepend(str(ml_dir))
    import inference

    assert inference.model_available() is False

    # /models existe mais ne contient aucun modele -> toujours False.
    fake_hdfs.hdfs_exists = lambda path: True
    fake_hdfs.hdfs_list = lambda path: ["autre_chose", "README"]
    assert inference.model_available() is False

    # Un modele versionne est reconnu.
    fake_hdfs.hdfs_list = lambda path: ["temperature_predictor_v3"]
    assert inference.model_available() is True


# ===========================================================================
# ROBUSTESSE : la source temps reel ne doit jamais inventer de donnees
# ===========================================================================

def test_missing_measurement_stays_null_never_zero():
    """
    La regression a ne jamais reintroduire.

    L'ancienne version remplacait une mesure absente par 0.0, valeur
    parfaitement plausible pour une temperature. Rien n'echouait, mais les
    moyennes Gold etaient tirees vers zero et le modele ML apprenait sur des
    valeurs inventees : un succes silencieux avec des donnees fausses.
    """
    checked = kafka_producer.validate_measurements({
        "temperature_2m": None, "wind_speed_10m": None, "precipitation": None,
    })
    for key in ("temperature_2m", "wind_speed_10m", "precipitation"):
        assert checked[key] is None, f"{key} ne doit JAMAIS devenir 0.0"
        assert checked[key] != 0.0


def test_valid_measurements_pass_through_untouched():
    source = {"temperature_2m": 21.5, "wind_speed_10m": 0.0, "precipitation": 0.0,
              "wind_direction_10m": 180.0, "weather_code": 3}
    checked = kafka_producer.validate_measurements(source)
    assert checked == source
    # Fonction pure : l'entree n'est pas modifiee.
    assert source["temperature_2m"] == 21.5
    # 0.0 est une VRAIE mesure (pas de pluie, vent nul) et doit etre conservee.
    assert checked["precipitation"] == 0.0


def test_physically_impossible_values_are_rejected():
    cases = {
        "temperature_2m": [-9999, 999, float("nan"), "n/a", None],
        "wind_speed_10m": [-5, 5000, "vent"],
        "wind_direction_10m": [-10, 720],
        "precipitation": [-3, 99999],
    }
    for key, values in cases.items():
        for value in values:
            assert kafka_producer.validate_measurements({key: value})[key] is None, (
                f"{key}={value!r} aurait du etre rejete")

    low, high = kafka_producer.MEASUREMENT_BOUNDS["temperature_2m"]
    assert kafka_producer.validate_measurements({"temperature_2m": low})["temperature_2m"] == low
    assert kafka_producer.validate_measurements({"temperature_2m": high})["temperature_2m"] == high


def test_unknown_keys_survive_a_contract_change():
    checked = kafka_producer.validate_measurements({"humidite_relative": 80, "temperature_2m": 12.0})
    assert checked["humidite_relative"] == 80
    assert checked["temperature_2m"] == 12.0


def test_empty_records_are_not_published():
    assert kafka_producer.has_usable_measurement(
        {"temperature": None, "windspeed": None, "precipitation": None}) is False
    assert kafka_producer.has_usable_measurement({"temperature": 12.0}) is True
    assert kafka_producer.has_usable_measurement({"windspeed": 3.0}) is True
    assert kafka_producer.has_usable_measurement({"precipitation": 0.0}) is True


def test_producer_module_imports_without_kafka_or_hdfs():
    source = Path(__file__).resolve().parent.parent / "scripts" / "kafka_producer.py"
    header = source.read_text(encoding="utf-8").split("def load_cities")[0]
    for forbidden in ("\nimport requests", "\nfrom kafka import", "\nimport hdfs_utils"):
        assert forbidden not in header, f"import global interdit : {forbidden.strip()}"


# ===========================================================================
# ROBUSTESSE : garde-fous d'orchestration
# ===========================================================================

def _dag_sources():
    dags = Path(__file__).resolve().parent.parent / "airflow" / "dags"
    return {p.name: p.read_text(encoding="utf-8") for p in sorted(dags.glob("dag_*.py"))}


def test_every_dag_bounds_its_tasks_in_time():
    for name, source in _dag_sources().items():
        assert "execution_timeout" in source, f"{name} : aucune borne de duree"
        assert "retries" in source, f"{name} : aucune politique de reprise"


def test_every_dag_forbids_concurrent_runs():
    for name, source in _dag_sources().items():
        assert "max_active_runs=1" in source, f"{name} : executions concurrentes possibles"


def test_long_running_services_restart_but_one_shots_do_not():
    yaml = pytest.importorskip("yaml", reason="PyYAML absent hors conteneur")
    compose = Path(__file__).resolve().parent.parent / "docker" / "docker-compose.yml"
    services = yaml.safe_load(compose.read_text(encoding="utf-8"))["services"]

    for name in ("namenode", "datanode", "kafka", "zookeeper", "postgres",
                 "spark-master", "spark-worker", "airflow-webserver", "airflow-scheduler"):
        assert services[name].get("restart") == "unless-stopped", f"{name} ne redemarre pas"

    assert "restart" not in services["airflow-init"]
