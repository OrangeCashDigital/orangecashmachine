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
DataTier — clasificación de tier de calidad de un dataset Silver

Nota sobre ValidationResult / ValidationSummary
------------------------------------------------
Viven en market_data.processing.validation.candle_validator.
No son entidades de dominio — son resultados de una operación
de processing layer. El dominio no depende de ellos.

Nota sobre OHLCVBar
--------------------
Eliminado: era una dependencia del dominio hacia infraestructura
(streaming), violando DIP.

Principios
----------
DIP  — el dominio no depende de infraestructura (processing, pandas, CCXT)
SSOT — tipos definidos aquí, no re-exportados desde otras capas
DDD  — separación clara entre entidades (identidad) y VOs (valor)
"""
from __future__ import annotations

from enum import Enum


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


__all__ = ["DataTier"]
