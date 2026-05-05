# -*- coding: utf-8 -*-
"""
market_data/domain/value_objects/__init__.py
=============================================

Value Objects canónicos del bounded context market_data.

Value Objects (DDD)
-------------------
Tipos inmutables definidos por su valor, sin identidad de negocio.
Dos VOs con los mismos campos son equivalentes (equality por valor).

Contenido
---------
Timeframe             — enum canónico de timeframes válidos (str-compatible)
timeframe_to_ms       — conversión timeframe → milisegundos (O(1))
InvalidTimeframeError — excepción Fail-Fast de timeframe inválido
VALID_TIMEFRAMES      — frozenset[str] para validación O(1)
align_to_grid         — alineación de timestamp al grid del timeframe
RawCandle             — tipo alias para vela CCXT cruda (6-tupla tipada)
QualityLabel          — clasificación de calidad de vela (CLEAN/SUSPECT/CORRUPT)
GapRange              — rango temporal de un gap detectado en un dataset

Anti-Corruption Layer (ACL)
----------------------------
Timeframe y sus helpers vienen del Shared Kernel (domain.value_objects.timeframe)
vía el ACL intermedio en processing/utils/timeframe.py. Ningún módulo de
market_data importa desde domain/ directamente — toda dependencia del
Shared Kernel pasa por ese ACL (DIP · OCP).

RawCandle, QualityLabel y GapRange son nativos de este BC — SSOT aquí.

Principios
----------
DIP    — dependencia hacia abstracciones; infra nunca define tipos de dominio
SSOT   — un único punto de definición para cada VO
OCP    — agregar VOs aquí no modifica consumidores existentes
KISS   — sin lógica de negocio; solo tipos y conversiones puras
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum
from typing import Tuple

import pandas as pd

# ---------------------------------------------------------------------------
# Timeframe — desde Shared Kernel vía ACL existente
# ---------------------------------------------------------------------------
from market_data.processing.utils.timeframe import (  # noqa: F401
    Timeframe,
    timeframe_to_ms,
    InvalidTimeframeError,
    VALID_TIMEFRAMES,
    align_to_grid,
)


# ---------------------------------------------------------------------------
# RawCandle — tipo alias canónico para vela CCXT cruda
# SSOT: definido aquí; candle_validator.py importa desde aquí (DIP).
# ---------------------------------------------------------------------------

RawCandle = Tuple[int, float, float, float, float, float]
"""
Vela CCXT en formato raw: [timestamp_ms, open, high, low, close, volume].

  timestamp_ms : int   — Unix epoch en milisegundos
  open         : float — precio de apertura
  high         : float — precio máximo
  low          : float — precio mínimo
  close        : float — precio de cierre
  volume       : float — volumen negociado en el período
"""


# ---------------------------------------------------------------------------
# QualityLabel — clasificación de calidad de una vela OHLCV
# SSOT: definido aquí; candle_validator.py importa desde aquí (DIP).
# ---------------------------------------------------------------------------

class QualityLabel(str, Enum):
    """
    Clasificación de calidad de una vela OHLCV.

    CLEAN   — vela válida, pasa a Silver sin flag
    SUSPECT — anomalía detectada, se escribe con quality_flag='suspect'
    CORRUPT — inválida, no se escribe en Silver, va a quarantine log

    str-compatible: QualityLabel.CLEAN == "clean" → True.
    Permite persistencia directa sin serialización adicional.
    """
    CLEAN   = "clean"
    SUSPECT = "suspect"
    CORRUPT = "corrupt"


# ---------------------------------------------------------------------------
# GapRange — rango temporal de un gap detectado en un dataset OHLCV
# SSOT: definido aquí; gap_utils.py importa desde aquí (DIP).
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class GapRange:
    """
    Rango temporal de un gap (hueco) detectado en un dataset OHLCV.

    Immutable (frozen=True) — los gaps son hechos históricos, no se modifican.

    Attributes
    ----------
    start_ms : timestamp Unix ms del último punto de datos antes del gap
    end_ms   : timestamp Unix ms del primer punto de datos después del gap
    expected : número de velas faltantes en el rango
    run_id   : run que detectó este gap (correlación de lineage, SSOT)

    Properties
    ----------
    duration_ms : duración total del gap en milisegundos
    severity    : clasificación (low / medium / high) según velas faltantes
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
        """Duración total del gap en milisegundos."""
        return self.end_ms - self.start_ms

    @property
    def severity(self) -> str:
        """
        Clasificación del gap por número de velas faltantes.

        low    : 1–2  velas (ruido normal, exchange maintenance)
        medium : 3–10 velas (degradación notable)
        high   : >10  velas (pérdida significativa de datos)
        """
        if self.expected <= 2:
            return "low"
        elif self.expected <= 10:
            return "medium"
        return "high"


# ---------------------------------------------------------------------------
# __all__ — API pública explícita
# ---------------------------------------------------------------------------

__all__ = [
    # Timeframe (Shared Kernel via ACL)
    "Timeframe",
    "timeframe_to_ms",
    "InvalidTimeframeError",
    "VALID_TIMEFRAMES",
    "align_to_grid",
    # Candle domain types
    "RawCandle",
    "QualityLabel",
    # Gap domain type
    "GapRange",
]
