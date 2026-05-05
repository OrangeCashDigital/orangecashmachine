# -*- coding: utf-8 -*-
"""
market_data/domain/entities/__init__.py
========================================

Entidades del bounded context market_data.

Entidades presentes
-------------------
OHLCVBar         — vela OHLCV inmutable del layer de streaming
ValidationResult — resultado de validar una vela cruda (identidad por candle)
ValidationSummary — agrega resultados de validate_batch para métricas
DataTier          — clasificación de tier de calidad de un dataset Silver

Nota sobre identidad (DDD)
--------------------------
En DDD, una Entity tiene identidad explícita que persiste en el tiempo.
OHLCVBar tiene identidad por (exchange, symbol, timeframe, timestamp).
ValidationResult tiene identidad por (candle,) — inmutable por frozen=True.
DataTier es un enum — value object de clasificación, re-exportado aquí
porque es el resultado de negocio de QualityPipeline.

Principios
----------
SSOT   — re-exports desde owners actuales; sin duplicar código
OCP    — agregar entidades no modifica consumidores existentes
DDD    — separación clara entre entidades (identidad) y VOs (valor)
"""
from __future__ import annotations

from enum import Enum

# OHLCVBar — entidad de streaming con identidad por (exchange, symbol, tf, ts)
from market_data.streaming.payloads import OHLCVBar  # noqa: F401

# ValidationResult — resultado de validación con identidad por candle
from market_data.processing.validation.candle_validator import (  # noqa: F401
    ValidationResult,
    ValidationSummary,
)


# ---------------------------------------------------------------------------
# DataTier — clasificación de tier de calidad de un dataset Silver
# Definida aquí como tipo de dominio; quality/pipeline.py importa desde aquí.
# ---------------------------------------------------------------------------

class DataTier(str, Enum):
    """
    Tier de calidad de un dataset Silver producido por QualityPipeline.

    CLEAN    — ACCEPT: sin issues; dato confiable para downstream
    FLAGGED  — ACCEPT_WITH_FLAGS: anomalías presentes, usable con precaución
    REJECTED — REJECT: datos inutilizables, no se escriben en Silver

    str-compatible: DataTier.CLEAN == "clean" → True.
    """
    CLEAN    = "clean"
    FLAGGED  = "flagged"
    REJECTED = "rejected"


__all__ = [
    "OHLCVBar",
    "ValidationResult",
    "ValidationSummary",
    "DataTier",
]
