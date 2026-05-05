# -*- coding: utf-8 -*-
"""
market_data/domain/value_objects/__init__.py
=============================================

Value Objects canónicos del bounded context market_data.

Value Objects (DDD)
-------------------
Tipos inmutables definidos por su valor, sin identidad de negocio.
Dos VOs con los mismos campos son equivalentes (equality por valor).

Catálogo
--------
Timeframe             — enum canónico de timeframes válidos (str-compatible)
timeframe_to_ms       — conversión timeframe → milisegundos (O(1), tabla)
InvalidTimeframeError — excepción Fail-Fast de timeframe inválido
VALID_TIMEFRAMES      — frozenset[str] para validación O(1)
align_to_grid         — alineación de timestamp al grid del timeframe
Candle                — vela OHLCV inmutable con invariantes de dominio
Symbol                — par de trading (base/quote), formato canónico
OHLCVBatch            — lote inmutable de Candle con metadatos de contexto
RawCandle             — tipo alias para wire format CCXT (6-tupla tipada)
QualityLabel          — clasificación de calidad (CLEAN/SUSPECT/CORRUPT)
GapRange              — rango temporal de un gap detectado en un dataset

Principios
----------
DDD    — VOs puros: inmutables, definidos por valor, sin identidad
SSOT   — un único punto de definición para cada VO
DIP    — infra nunca define tipos de dominio; dominio no depende de infra
OCP    — agregar VOs aquí no modifica consumidores existentes
KISS   — sin lógica de negocio compleja; tipos y conversiones puras
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Tuple

import pandas as pd

# ---------------------------------------------------------------------------
# Timeframe — SSOT en domain/value_objects/timeframe.py
# ---------------------------------------------------------------------------
from market_data.domain.value_objects.timeframe import (  # noqa: F401
    Timeframe,
    timeframe_to_ms,
    InvalidTimeframeError,
    VALID_TIMEFRAMES,
    align_to_grid,
)

# ---------------------------------------------------------------------------
# Candle — VO de vela OHLCV con invariantes de dominio
# ---------------------------------------------------------------------------
from market_data.domain.value_objects.candle import Candle  # noqa: F401

# ---------------------------------------------------------------------------
# Symbol — VO de par de trading
# ---------------------------------------------------------------------------
from market_data.domain.value_objects.symbol import Symbol  # noqa: F401

# ---------------------------------------------------------------------------
# OHLCVBatch — VO de lote de velas con metadatos
# ---------------------------------------------------------------------------
from market_data.domain.value_objects.ohlcv_batch import OHLCVBatch  # noqa: F401

# ---------------------------------------------------------------------------
# RawCandle — tipo alias canónico para wire format CCXT crudo
# Usado solo en la frontera ACL (adapters inbound); el dominio usa Candle.
# ---------------------------------------------------------------------------
RawCandle = Tuple[int, float, float, float, float, float]

# ---------------------------------------------------------------------------
# QualityLabel — clasificación de calidad de una vela OHLCV
# ---------------------------------------------------------------------------
class QualityLabel(str, Enum):
    """
    Clasificación de calidad de una vela OHLCV.

    CLEAN   — vela válida, pasa a Silver sin flag
    SUSPECT — anomalía detectada, se escribe con quality_flag='suspect'
    CORRUPT — inválida, no se escribe en Silver, va a quarantine log

    str-compatible: QualityLabel.CLEAN == "clean" → True.
    """
    CLEAN   = "clean"
    SUSPECT = "suspect"
    CORRUPT = "corrupt"

# ---------------------------------------------------------------------------
# GapRange — rango temporal de un gap detectado en un dataset OHLCV
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class GapRange:
    """
    Rango temporal de un gap (hueco) detectado en un dataset OHLCV.

    Immutable (frozen=True) — los gaps son hechos históricos, no se modifican.

    Attributes
    ----------
    start_ms : timestamp Unix ms del último punto antes del gap
    end_ms   : timestamp Unix ms del primer punto después del gap
    expected : número de velas faltantes en el rango
    run_id   : run que detectó este gap (correlación de lineage)

    Properties
    ----------
    duration_ms : duración total del gap en milisegundos
    severity    : low / medium / high según velas faltantes
    """
    start_ms: int
    end_ms:   int
    expected: int
    run_id:   str = ""

    def __str__(self) -> str:
        start = pd.Timestamp(self.start_ms, unit="ms", tz="UTC").isoformat()
        end   = pd.Timestamp(self.end_ms,   unit="ms", tz="UTC").isoformat()
        return f"Gap[{start} → {end} expected={self.expected}]"

    @property
    def duration_ms(self) -> int:
        return self.end_ms - self.start_ms

    @property
    def severity(self) -> str:
        if self.expected <= 2:
            return "low"
        elif self.expected <= 10:
            return "medium"
        return "high"


# ---------------------------------------------------------------------------
# __all__
# ---------------------------------------------------------------------------
__all__ = [
    # Timeframe
    "Timeframe",
    "timeframe_to_ms",
    "InvalidTimeframeError",
    "VALID_TIMEFRAMES",
    "align_to_grid",
    # Market data VOs
    "Candle",
    "Symbol",
    "OHLCVBatch",
    # Wire format alias (ACL boundary)
    "RawCandle",
    # Quality
    "QualityLabel",
    # Gap
    "GapRange",
]
