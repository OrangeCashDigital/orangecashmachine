# -*- coding: utf-8 -*-
"""
market_data/domain/value_objects/__init__.py
=============================================

Value Objects canónicos del bounded context market_data.

Re-exports desde Shared Kernel (domain.value_objects) vía ACL interna.
Ningún módulo de market_data importa desde domain/ directamente —
toda dependencia del Shared Kernel pasa por este módulo (DIP · ACL).

Value Objects presentes
-----------------------
Timeframe             — enum canónico de timeframes (str-compatible)
timeframe_to_ms       — conversión timeframe → milisegundos
InvalidTimeframeError — excepción Fail-Fast de timeframe inválido
VALID_TIMEFRAMES      — frozenset[str] para validación O(1)
align_to_grid         — alineación de timestamps al grid del timeframe
RawCandle             — tipo alias para vela CCXT cruda
QualityLabel          — clasificación de calidad de vela (CLEAN/SUSPECT/CORRUPT)

Principios
----------
DIP    — dependencia hacia abstracciones, no hacia infra
SSOT   — un único punto de importación para todo el BC
OCP    — agregar VOs aquí no modifica los consumidores existentes
ACL    — Anti-Corruption Layer entre Shared Kernel y market_data BC
"""
from __future__ import annotations

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
# Definido aquí como SSOT; candle_validator.py lo re-exportará desde aquí.
# ---------------------------------------------------------------------------
from typing import Tuple

RawCandle = Tuple[int, float, float, float, float, float]
"""
Vela CCXT en formato raw: [timestamp_ms, open, high, low, close, volume].

El timestamp es Unix epoch en milisegundos (int).
Los campos OHLCV son float64.
"""

# ---------------------------------------------------------------------------
# QualityLabel — clasificación de calidad de vela
# Importado desde su ubicación actual; candle_validator es el owner por ahora.
# TODO: mover QualityLabel aquí cuando se haga el full domain refactor.
# ---------------------------------------------------------------------------
from market_data.processing.validation.candle_validator import (  # noqa: F401
    QualityLabel,
)


__all__ = [
    # Timeframe
    "Timeframe",
    "timeframe_to_ms",
    "InvalidTimeframeError",
    "VALID_TIMEFRAMES",
    "align_to_grid",
    # Candle
    "RawCandle",
    "QualityLabel",
]
