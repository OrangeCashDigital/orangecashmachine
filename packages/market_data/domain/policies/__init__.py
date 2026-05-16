# -*- coding: utf-8 -*-
"""
market_data/domain/policies/
==============================

Shim de backward compatibility.

Las estrategias de ingestión (BackfillStrategy, IncrementalStrategy,
RepairStrategy) viven ahora en market_data.application.strategies/.

Este módulo re-exporta desde application para no romper imports existentes
durante la migración. Migrar los imports directamente:

    # Antes (deprecado):
    from market_data.domain.policies import BackfillStrategy

    # Ahora (correcto):
    from market_data.application.strategies.backfill    import BackfillStrategy
    from market_data.application.strategies.incremental import IncrementalStrategy
    from market_data.application.strategies.repair      import RepairStrategy

Los tipos de dominio (PipelineContext, PairResult, etc.) permanecen en
domain/policies/base.py y se pueden importar directamente desde allí.
"""
from market_data.domain.policies.base import (
    PipelineContext,
    PipelineMode,
    PairResult,
    PipelineSummary,
    StrategyMixin,
    classify_error,
)
from market_data.application.strategies.backfill    import BackfillStrategy
from market_data.application.strategies.incremental import IncrementalStrategy
from market_data.application.strategies.repair      import RepairStrategy

__all__ = [
    # Tipos de dominio — fuente permanente: domain/policies/base.py
    "PipelineContext",
    "PipelineMode",
    "PairResult",
    "PipelineSummary",
    "StrategyMixin",
    "classify_error",
    # Estrategias — fuente permanente: application/strategies/
    "BackfillStrategy",
    "IncrementalStrategy",
    "RepairStrategy",
]
