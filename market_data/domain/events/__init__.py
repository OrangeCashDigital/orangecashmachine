# -*- coding: utf-8 -*-
"""
market_data/domain/events/__init__.py
=======================================

Eventos de dominio del bounded context market_data.

Eventos presentes
-----------------
LineageEvent    — evento de trazabilidad de un lote de datos (RAW→SILVER→GOLD)
PipelineLayer   — enum de capas del pipeline (RAW, SILVER, GOLD)
LineageStatus   — enum de estado de un lote (SUCCESS, PARTIAL, FAILED)

Nota sobre Domain Events (DDD)
------------------------------
Un Domain Event representa algo que ocurrió en el dominio y que
otros bounded contexts pueden observar. LineageEvent es el evento
más maduro del BC: captura qué datos pasaron por qué capa, cuándo,
y con qué resultado.

Los eventos de streaming (OHLCVBar publicado en Redis) son eventos
de infraestructura, no de dominio — viven en market_data/streaming/.

Principios
----------
OCP    — nuevos eventos se agregan sin modificar consumers existentes
SSOT   — re-exports desde lineage/tracker.py (owner actual)
DDD    — eventos son inmutables; LineageEvent es frozen-compatible
"""
from __future__ import annotations

from market_data.lineage.tracker import (  # noqa: F401
    LineageEvent,
    LineageStatus,
    PipelineLayer,
)

__all__ = [
    "LineageEvent",
    "LineageStatus",
    "PipelineLayer",
]
