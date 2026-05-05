# -*- coding: utf-8 -*-
"""
market_data/domain/entities/__init__.py
========================================

Entidades del bounded context market_data.

Entidades vs Value Objects (DDD)
---------------------------------
Una Entity tiene identidad explícita que persiste en el tiempo.
Un VO es definido por su valor — sin identidad propia.

Entidades presentes
-------------------
ValidationResult  — resultado de validar una Candle (identidad por candle)
ValidationSummary — agrega resultados de validate_batch para métricas
DataTier          — clasificación de calidad de un dataset Silver

Nota sobre OHLCVBar
--------------------
OHLCVBar (streaming/payloads.py) era re-exportado aquí anteriormente.
Eliminado: era una dependencia del dominio hacia infraestructura (streaming),
violando DIP. El dominio usa Candle (domain/value_objects/candle.py).
Los adapters de streaming usan OHLCVBar en su propia capa.

Principios
----------
DIP    — el dominio no depende de infraestructura (streaming, pandas, CCXT)
SSOT   — re-exports desde owners canónicos; sin duplicar código
OCP    — agregar entidades no modifica consumidores existentes
DDD    — separación clara entre entidades (identidad) y VOs (valor)
"""
from __future__ import annotations

from enum import Enum

# ValidationResult / ValidationSummary — resultado de validación con identidad por candle
from market_data.processing.validation.candle_validator import (  # noqa: F401
    ValidationResult,
    ValidationSummary,
)


# ---------------------------------------------------------------------------
# DataTier — clasificación de tier de calidad de un dataset Silver
# ---------------------------------------------------------------------------

class DataTier(str, Enum):
    """
    Tier de calidad de un dataset Silver producido por QualityPipeline.

    CLEAN    — sin issues; dato confiable para downstream
    FLAGGED  — anomalías presentes, usable con precaución
    REJECTED — datos inutilizables, no se escriben en Silver

    str-compatible: DataTier.CLEAN == "clean" → True.
    """
    CLEAN    = "clean"
    FLAGGED  = "flagged"
    REJECTED = "rejected"


__all__ = [
    "ValidationResult",
    "ValidationSummary",
    "DataTier",
]
