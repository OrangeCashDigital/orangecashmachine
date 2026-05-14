# -*- coding: utf-8 -*-
"""
market_data/processing/utils/timeframe.py
==========================================

⚠️  SHIM — re-exporta desde el SSOT canónico en domain/.

No definir lógica aquí. La fuente de verdad es:
  market_data.domain.value_objects.timeframe

Este módulo existe únicamente para backward-compatibility con los
10+ callers que importan desde esta ruta. Se eliminará cuando todos
los callers migren al import canónico.

Migración
---------
Sustituir cualquier import de esta forma:
  ✗ from market_data.processing.utils.timeframe import timeframe_to_ms
Por la forma canónica:
  ✔ from market_data.domain.value_objects.timeframe import timeframe_to_ms
  ✔ from market_data.domain.value_objects import timeframe_to_ms

Principios: SSOT · DIP · OCP (callers no se rompen durante migración)
"""
from __future__ import annotations

# Re-export completo desde SSOT canónico — sin lógica propia
from market_data.domain.value_objects.timeframe import (  # noqa: F401
    Timeframe,
    timeframe_to_ms,
    InvalidTimeframeError,
    VALID_TIMEFRAMES,
    align_to_grid,
)

__all__ = [
    "Timeframe",
    "timeframe_to_ms",
    "InvalidTimeframeError",
    "VALID_TIMEFRAMES",
    "align_to_grid",
]
