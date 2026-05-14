# -*- coding: utf-8 -*-
"""
shared/types/timeframe.py — TOMBSTONE
======================================

⚠️  DEPRECATED — scheduled for removal after 2026-08-01

SSOT canónico:
    market_data.domain.value_objects.timeframe

Este módulo re-exporta sin lógica propia. No añadir lógica aquí.

Migración de callers:
    ✗  from shared.types.timeframe import Timeframe
    ✗  from shared.types import Timeframe
    ✔  from market_data.domain.value_objects.timeframe import Timeframe

Estado de callers conocidos al 2026-05-14:
    - shared/types/ohlcv.py          → MIGRADO (Cirugía A)
    - shared/types/__init__.py       → re-exporta Timeframe (tolerable como API pública)

Eliminar este archivo cuando __init__.py importe desde domain/ directamente.

Principles: SSOT · OCP · YAGNI
"""
from __future__ import annotations
import warnings

warnings.warn(
    "shared.types.timeframe está deprecated. "
    "Usa: from market_data.domain.value_objects.timeframe import Timeframe",
    DeprecationWarning,
    stacklevel=2,
)

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
