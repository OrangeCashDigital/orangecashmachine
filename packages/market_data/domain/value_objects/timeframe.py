# -*- coding: utf-8 -*-
"""
market_data/domain/value_objects/timeframe.py
===============================================

Re-exportador de Timeframe desde shared.types.timeframe.

SSOT canónico: shared.types.timeframe
---------------------------------------
Timeframe es un tipo transversal compartido entre market_data, trading,
portfolio y research. Vive en shared/ para que ningún BC dependa de otro.

Este módulo re-exporta sin lógica propia para mantener compatibilidad
con todos los callers internos de market_data que importan desde aquí.

Principios
----------
DIP    — la dependencia apunta hacia shared (nivel más bajo), no al revés
SSOT   — definición única en shared/types/timeframe.py
OCP    — callers existentes no necesitan cambiar sus imports
BC-09  — shared no importa market_data; market_data sí puede importar shared
"""

from __future__ import annotations

from shared.types.timeframe import (  # noqa: F401
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
