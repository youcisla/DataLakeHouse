# -*- coding: utf-8 -*-
"""Rend le paquet ``scripts/`` importable par les tests (équivalent PYTHONPATH)."""

from __future__ import annotations

import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
