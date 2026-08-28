# -*- coding: utf-8 -*-
"""
verify_medallion.py : contrôle automatique de la couche Medallion sur HDFS.
===========================================================================
Vérifie, sans aucune intervention manuelle, que les trois couches du datalake
existent, sont marquées ``_SUCCESS`` et contiennent des données :

    BRONZE  /bronze/meteo/batch/source=meteofrance   (archives Météo-France)
            /bronze/meteo/stream/source=openmeteo    (flux Kafka temps réel)
    SILVER  /silver/meteo                            (Parquet Zstd, dt=)
    GOLD    /gold/meteo/daily_aggregates | weekly_trends
            /gold/meteo/extreme_events   | climate_profile

Utilisé par ``make verify`` à la fin de ``make all``. Sort en code 1 si une
couche obligatoire est absente, vide ou non marquée : le workflow échoue donc
bruyamment plutôt que de laisser croire à un succès.

Usage :
    python verify_medallion.py [--allow-empty-stream] [--json]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from typing import Any, Dict, List, Optional

logger = logging.getLogger("verify_medallion")


# ---------------------------------------------------------------------------
# Contrat de vérification (pur : testable sans HDFS)
# ---------------------------------------------------------------------------

class Check:
    """Un chemin HDFS attendu et le niveau d'exigence associé."""

    def __init__(self, layer: str, path: str, required: bool = True,
                 expect_success: bool = True, expect_data: bool = True) -> None:
        self.layer = layer
        self.path = path
        self.required = required
        self.expect_success = expect_success
        self.expect_data = expect_data

    def __repr__(self) -> str:  # pragma: no cover - confort de debug
        return f"Check({self.layer}, {self.path}, required={self.required})"


def expected_checks(allow_empty_stream: bool = False,
                    with_ml: bool = False) -> List[Check]:
    """
    Liste ordonnée des contrôles Bronze -> Silver -> Gold.

    Le flux temps réel peut être vide lors d'une toute première exécution
    (aucune fenêtre horaire encore close) : ``allow_empty_stream`` le rend
    facultatif pour que ``make all`` reste vert sur un cluster neuf.
    """
    return [
        Check("BRONZE", "/bronze/meteo/batch/source=meteofrance"),
        Check("BRONZE", "/bronze/meteo/stream/source=openmeteo",
              required=not allow_empty_stream,
              expect_success=not allow_empty_stream,
              expect_data=not allow_empty_stream),
        Check("SILVER", "/silver/meteo"),
        Check("GOLD", "/gold/meteo/daily_aggregates"),
        Check("GOLD", "/gold/meteo/weekly_trends"),
        Check("GOLD", "/gold/meteo/extreme_events"),
        Check("GOLD", "/gold/meteo/climate_profile"),
        # Tables issues du bonus ML / GenAI : obligatoires seulement apres un
        # `make all` complet (option --with-ml), facultatives sinon.
        Check("GOLD", "/gold/meteo/ml_predictions", required=with_ml,
              expect_success=with_ml, expect_data=with_ml),
        Check("GOLD", "/gold/meteo/ai_insights", required=with_ml,
              expect_success=False, expect_data=with_ml),
    ]


def format_size(num_bytes: Optional[int]) -> str:
    """Formate une taille en octets de façon lisible (Ko / Mo / Go)."""
    if num_bytes is None:
        return "-"
    size = float(num_bytes)
    for unit in ("o", "Ko", "Mo", "Go", "To"):
        if size < 1024.0 or unit == "To":
            return f"{size:.1f} {unit}" if unit != "o" else f"{int(size)} o"
        size /= 1024.0
    return f"{size:.1f} To"


def verdict(result: Dict[str, Any]) -> str:
    """Statut lisible d'un contrôle : OK / VIDE / ABSENT / IGNORÉ."""
    if not result["exists"]:
        return "IGNORÉ" if not result["required"] else "ABSENT"
    if result["expect_success"] and not result["has_success"]:
        return "SANS _SUCCESS"
    if result["expect_data"] and not result["bytes"]:
        return "VIDE"
    return "OK"


def is_failure(result: Dict[str, Any]) -> bool:
    """True si le contrôle doit faire échouer le workflow."""
    if not result["required"]:
        return False
    return verdict(result) != "OK"


def render_report(results: List[Dict[str, Any]]) -> str:
    """Rend le rapport sous forme de tableau texte aligné."""
    lines = [
        f"{'COUCHE':<7} {'CHEMIN HDFS':<45} {'TAILLE':>10}  STATUT",
        "-" * 82,
    ]
    for result in results:
        lines.append(
            f"{result['layer']:<7} {result['path']:<45} "
            f"{format_size(result['bytes']):>10}  {verdict(result)}"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Exécution des contrôles (WebHDFS)
# ---------------------------------------------------------------------------

def run_check(check: Check) -> Dict[str, Any]:
    """Interroge HDFS pour un contrôle donné."""
    import hdfs_utils

    result: Dict[str, Any] = {
        "layer": check.layer,
        "path": check.path,
        "required": check.required,
        "expect_success": check.expect_success,
        "expect_data": check.expect_data,
        "exists": False,
        "has_success": False,
        "bytes": 0,
    }
    if not hdfs_utils.hdfs_exists(check.path):
        return result
    result["exists"] = True
    try:
        result["bytes"] = hdfs_utils.hdfs_size(check.path)
    except IOError:
        result["bytes"] = 0
    # Le marqueur peut être à la racine de la table ou dans ses partitions.
    result["has_success"] = (
        hdfs_utils.has_success(check.path)
        or bool(hdfs_utils.success_paths(check.path, max_depth=2))
    )
    return result


def run(args: argparse.Namespace) -> int:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    results = [run_check(check)
               for check in expected_checks(args.allow_empty_stream, args.with_ml)]

    if args.json:
        print(json.dumps(
            [dict(r, verdict=verdict(r)) for r in results], ensure_ascii=False, indent=2))
    else:
        print(render_report(results))

    failures = [r for r in results if is_failure(r)]
    if failures:
        print(f"\nÉCHEC : {len(failures)} couche(s) non conforme(s) : "
              + ", ".join(r["path"] for r in failures))
        return 1
    print("\nSUCCÈS : la couche Medallion est complète (Bronze -> Silver -> Gold).")
    return 0


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Vérifie la couche Medallion (Bronze -> Silver -> Gold) sur HDFS")
    parser.add_argument("--allow-empty-stream", action="store_true",
                        help="Tolère un flux Open-Meteo encore vide (cluster neuf).")
    parser.add_argument("--with-ml", action="store_true",
                        help="Exiger aussi ml_predictions et ai_insights (bonus ML/GenAI).")
    parser.add_argument("--json", action="store_true", help="Sortie JSON.")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.WARNING,
                        format="%(levelname)s [verify_medallion] %(message)s")
    return run(parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
