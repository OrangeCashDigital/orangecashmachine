# -*- coding: utf-8 -*-
"""
market_data/domain/entities/__init__.py
========================================

Entidades del bounded context market_data.

Entidades presentes
-------------------
OHLCVBar        — vela OHLCV validada y enriquecida (streaming payload)
ValidationResult — resultado de validar una vela cruda (con identidad por candle)

Nota sobre identidad (DDD)
--------------------------
En DDD, una Entity tiene identidad explícita que persiste en el tiempo.
OHLCVBar tiene identidad por (exchange, symbol, timeframe, timestamp).
ValidationResult tiene identidad por (candle,) — inmutable por frozen=True.

Principios
----------
SSOT   — re-exports desde owners actuales; sin duplicar código
OCP    — agregar entidades no modifica consumidores existentes
DDD    — separación clara entre entidades (identidad) y VOs (valor)
"""
from __future__ import annotations

# OHLCVBar — entidad de streaming con identidad por (exchange, symbol, tf, ts)
from market_data.streaming.payloads import OHLCVBar  # noqa: F401

# ValidationResult — resultado de validación con identidad por candle
from market_data.processing.validation.candle_validator import (  # noqa: F401
    ValidationResult,
    ValidationSummary,
)

__all__ = [
    "OHLCVBar",
    "ValidationResult",
    "ValidationSummary",
]
