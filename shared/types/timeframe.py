# -*- coding: utf-8 -*-
"""
shared/types/timeframe.py
==========================

⚠️  TOMBSTONE — re-exporta desde el SSOT canónico.

La fuente de verdad es:
  market_data.domain.value_objects.timeframe

Este módulo existe para no romper imports legacy.
Migrar todos los callers a la ruta canónica y eliminar este archivo.

Migración:
  ✗ from shared.types.timeframe import Timeframe
  ✔ from market_data.domain.value_objects.timeframe import Timeframe

Principios: SSOT · OCP
"""
from __future__ import annotations

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
