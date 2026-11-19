"""Chargement de la configuration et utilitaires communs."""
from datetime import date, timedelta
from pathlib import Path

import yaml


def load_config(path: str = "config/pipeline.yaml") -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def periode(cfg: dict) -> tuple[date, date]:
    """Retourne (début inclus, fin exclue) de la période analysée."""
    debut = date.fromisoformat(cfg["periode"]["debut"])
    return debut, debut + timedelta(days=cfg["periode"]["jours"])


def chemin(cfg: dict, couche: str, *parties: str) -> str:
    return str(Path(cfg["paths"][couche], *parties))
