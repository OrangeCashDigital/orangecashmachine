# -*- coding: utf-8 -*-
"""
market_data/domain/events/__init__.py
========================================

SSOT de todos los domain events del bounded context market_data.

Submódulos internos (no importar directamente desde fuera)
----------------------------------------------------------
_lineage.py  → PipelineLayer, LineageStatus, LineageEvent
ingestion.py → DomainEvent, CandleReceived, OHLCVBatchReceived

Regla de uso
------------
SIEMPRE importar desde este __init__, nunca desde submódulos:
  ✔ from market_data.domain.events import PipelineLayer
  ✗ from market_data.domain.events._lineage import PipelineLayer
"""
from __future__ import annotations

from market_data.domain.events._lineage import (    # noqa: F401
    LineageEvent,
    LineageStatus,
    PipelineLayer,
)
from market_data.domain.events.ingestion import (   # noqa: F401
    CandleReceived,
    CandleTuple,
    DomainEvent,
    OHLCVBatchReceived,
)

__all__ = [
    # Lineage
    "PipelineLayer",
    "LineageStatus",
    "LineageEvent",
    # Ingestion
    "DomainEvent",
    "CandleReceived",
    "CandleTuple",
    "OHLCVBatchReceived",
]
