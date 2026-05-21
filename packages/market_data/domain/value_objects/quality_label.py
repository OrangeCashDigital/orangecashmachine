# -*- coding: utf-8 -*-
"""
market_data/domain/value_objects/quality_label.py
==================================================

QualityLabel — clasificación de calidad de una vela OHLCV.

SSOT: definición canónica. Importar desde aquí, nunca desde __init__.

Principios: DDD · SSOT · KISS
"""

from __future__ import annotations

from enum import Enum


class QualityLabel(str, Enum):
    """
    Clasificación de calidad de una vela OHLCV.

    str-compatible: QualityLabel.CLEAN == "clean" → True.

    CLEAN   — vela válida, pasa a Silver sin flag
    SUSPECT — anomalía detectada, se escribe con quality_flag="suspect"
    CORRUPT — inválida, no se escribe en Silver, va a quarantine log
    """

    CLEAN = "clean"
    SUSPECT = "suspect"
    CORRUPT = "corrupt"


__all__ = ["QualityLabel"]
