# -*- coding: utf-8 -*-
"""
market_data/processing/utils/timeframe.py
==========================================
DEPRECATED desde Fase 3 — tombstone de re-export.

SSOT movido a:
    domain/value_objects/timeframe.py

Por qué fue movido
------------------
timeframe_to_ms y el enum Timeframe son conocimiento del dominio puro,
no lógica de procesamiento. Vivir en processing/utils/ violaba la regla
de dependencias de Clean Architecture: el dominio no debe depender de
capas de aplicación o infraestructura.

Migración de imports (actualizar gradualmente)
----------------------------------------------
Antes:
    from market_data.processing.utils.timeframe import timeframe_to_ms
    from market_data.processing.utils.timeframe import Timeframe
    from market_data.processing.utils.timeframe import InvalidTimeframeError
    from market_data.processing.utils.timeframe import VALID_TIMEFRAMES

Ahora:
    from domain.value_objects.timeframe import timeframe_to_ms
    from domain.value_objects.timeframe import Timeframe
    from domain.value_objects.timeframe import InvalidTimeframeError
    from domain.value_objects.timeframe import VALID_TIMEFRAMES

Este módulo re-exporta todo para compatibilidad con imports existentes.
No rompe nada — todos los imports legacy siguen funcionando.

align_to_grid NO se mueve al dominio — requiere pandas y Prometheus,
que son dependencias de infraestructura. Permanece en grid_alignment.py
y se re-exporta desde aquí por compatibilidad.

Eliminación programada: cuando todos los imports estén migrados a domain/.
"""
from __future__ import annotations

import warnings

warnings.warn(
    "Importar desde market_data.processing.utils.timeframe está deprecated. "
    "Usa: from domain.value_objects.timeframe import <símbolo>",
    DeprecationWarning,
    stacklevel=2,
)

# Re-exports — todos los símbolos públicos del legacy
from domain.value_objects.timeframe import (  # noqa: F401, E402
    Timeframe,
    VALID_TIMEFRAMES,
    InvalidTimeframeError,
    timeframe_to_ms,
)

# align_to_grid permanece en grid_alignment — no pertenece al dominio puro
from market_data.processing.utils.grid_alignment import align_to_grid  # noqa: F401, E402
