"""Genere un bulletin meteo avec un LLM local Ollama ou un fallback fiable.

Le script lit le JSON produit par predict.py. Pour activer le LLM local :
    python genai_bulletin.py --use-ollama --model mistral

Ollama doit etre lance localement et le modele doit deja etre telecharge :
    ollama run mistral
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen


def generate_with_ollama(prediction: dict, model_name: str) -> str:
    """Demande a Ollama un bulletin court, fonde uniquement sur les donnees fournies."""
    prompt = f"""Tu es un presentateur meteo francophone.
Redige un bulletin de 2 phrases, clair et professionnel, a partir de ces donnees JSON.
N'invente aucune information et mentionne une alerte si elle est pertinente.
Donnees : {json.dumps(prediction, ensure_ascii=False)}"""
    payload = json.dumps(
        {"model": model_name, "prompt": prompt, "stream": False}
    ).encode("utf-8")
    request = Request(
        "http://localhost:11434/api/generate",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=30) as response:
        result = json.loads(response.read().decode("utf-8"))
    return result["response"].strip()


def generate_bulletin(prediction: dict, use_ollama: bool, model_name: str) -> str:
    """Utilise le LLM local si demande, sinon preserve le bulletin du modele."""
    if use_ollama:
        try:
            return generate_with_ollama(prediction, model_name)
        except (KeyError, TimeoutError, URLError, OSError, json.JSONDecodeError) as error:
            print(f"Ollama indisponible ({error}); utilisation du fallback.")
    return prediction["bulletin"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prediction", type=Path, default=Path("weather_prediction.json"))
    parser.add_argument("--model", default="mistral", help="Modele Ollama local.")
    parser.add_argument(
        "--use-ollama",
        action="store_true",
        help="Utilise Ollama; sans cette option, le fallback ne depend d'aucun service.",
    )
    parser.add_argument("--output", type=Path, default=Path("weather_bulletin.txt"))
    args = parser.parse_args()

    prediction = json.loads(args.prediction.read_text(encoding="utf-8"))
    bulletin = generate_bulletin(prediction, args.use_ollama, args.model)
    args.output.write_text(bulletin + "\n", encoding="utf-8")
    print(bulletin)
    print(f"Bulletin sauvegarde dans : {args.output}")


if __name__ == "__main__":
    main()