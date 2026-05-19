# -*- coding: utf-8 -*-
"""
market_data/domain/policies/
==============================

Contratos y tipos del sistema de pipeline unificado.

Re-exporta únicamente lo que existe en base.py.
BasePipelineStrategy fue renombrado a StrategyMixin — no re-exportar alias.

Estrategias concretas → market_data.application.strategies.*
    from market_data.application.strategies.backfill    import BackfillStrategy
    from market_data.application.strategies.incremental import IncrementalStrategy
    from market_data.application.strategies.repair      import RepairStrategy

Principios: SSOT · KISS · no re-exports que creen cascadas de inicialización.
"""
from market_data.domain.policies.base import (
    PairResult,
    PipelineContext,
    PipelineMode,
    PipelineStrategy,
    PipelineSummary,
    StrategyMixin,
    classify_error,
)

__all__ = [
    "PairResult",
    "PipelineContext",
    "PipelineMode",
    "PipelineStrategy",
    "PipelineSummary",
    "StrategyMixin",
    "classify_error",
]

from market_data.domain.policies.data_quality_policy import (  # noqa: F401
    QualityDecision,
    PolicyResult,
    DataQualityPolicy,
    default_policy,
)
