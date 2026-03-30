"""
market_data/processing/strategies
==================================
Estrategias de pipeline OHLCV: incremental, backfill, repair.
Cada estrategia implementa StrategyMixin y opera sobre PipelineContext.
"""
from market_data.processing.strategies.base import (
    PipelineContext, PipelineMode, PairResult, PipelineSummary, StrategyMixin,
)
from market_data.processing.strategies.incremental import IncrementalStrategy
from market_data.processing.strategies.backfill import BackfillStrategy
from market_data.processing.strategies.repair import RepairStrategy

__all__ = [
    "PipelineContext", "PipelineMode", "PairResult", "PipelineSummary", "StrategyMixin",
    "IncrementalStrategy", "BackfillStrategy", "RepairStrategy",
]
