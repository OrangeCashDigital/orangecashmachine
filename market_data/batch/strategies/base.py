"""
market_data/batch/strategies/base.py
=====================================

Contrato central del sistema de pipeline unificado.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Protocol, runtime_checkable

from market_data.batch.fetchers.fetcher import HistoricalFetcherAsync
from market_data.batch.storage.silver_storage import SilverStorage
from market_data.batch.storage.bronze_storage import BronzeStorage
from market_data.batch.pipelines.quality_pipeline import QualityPipeline
from services.state.cursor_store import CursorStore


class PipelineMode(str, Enum):
    INCREMENTAL = "incremental"
    BACKFILL    = "backfill"
    REPAIR      = "repair"


@dataclass
class PipelineContext:
    fetcher:     HistoricalFetcherAsync
    storage:     SilverStorage
    bronze:      BronzeStorage
    cursor:      CursorStore
    quality:     QualityPipeline
    exchange_id: str
    market_type: str
    start_date:  str


_TRANSIENT_ERROR_NAMES: frozenset[str] = frozenset({
    "TimeoutError", "ConnectionError", "OSError",
    "ConnectionRefusedError", "ConnectionResetError",
    "BrokenPipeError",
})


@dataclass
class PairResult:
    symbol:      str
    timeframe:   str
    mode:        PipelineMode
    rows:        int  = 0
    skipped:     bool = False
    error:       str  = ""
    duration_ms: int  = 0
    gaps_found:  int  = 0
    gaps_healed: int  = 0
    chunks:      int  = 0

    @property
    def success(self) -> bool:
        return not self.error and not self.skipped

    @property
    def is_transient_error(self) -> bool:
        """Clasifica error como transitorio por nombre de clase (el error ya es string)."""
        return any(name in self.error for name in _TRANSIENT_ERROR_NAMES)

    def __str__(self) -> str:
        if self.error:
            tag = "TRANSIENT" if self.is_transient_error else "FATAL"
            return f"[{self.mode.value}] {self.symbol}/{self.timeframe} ERROR[{tag}]: {self.error}"
        if self.skipped:
            return f"[{self.mode.value}] {self.symbol}/{self.timeframe} SKIPPED"
        if self.mode == PipelineMode.REPAIR:
            return (
                f"[repair] {self.symbol}/{self.timeframe} "
                f"gaps_found={self.gaps_found} gaps_healed={self.gaps_healed} "
                f"rows={self.rows} duration={self.duration_ms}ms"
            )
        if self.mode == PipelineMode.BACKFILL:
            return (
                f"[backfill] {self.symbol}/{self.timeframe} "
                f"chunks={self.chunks} rows={self.rows} duration={self.duration_ms}ms"
            )
        return (
            f"[incremental] {self.symbol}/{self.timeframe} "
            f"rows={self.rows} duration={self.duration_ms}ms"
        )


@dataclass
class PipelineSummary:
    results:     List[PairResult] = field(default_factory=list)
    duration_ms: int              = 0
    mode:        PipelineMode     = PipelineMode.INCREMENTAL

    @property
    def total(self)     -> int: return len(self.results)

    @property
    def succeeded(self) -> int: return sum(1 for r in self.results if r.success)

    @property
    def skipped(self)   -> int: return sum(1 for r in self.results if r.skipped)

    @property
    def failed(self)    -> int: return sum(1 for r in self.results if r.error)

    @property
    def total_rows(self) -> int: return sum(r.rows for r in self.results)

    @property
    def total_gaps_found(self) -> int: return sum(r.gaps_found for r in self.results)

    @property
    def total_gaps_healed(self) -> int: return sum(r.gaps_healed for r in self.results)

    @property
    def throughput_rows_per_sec(self) -> float:
        if not self.duration_ms:
            return 0.0
        return round(self.total_rows / (self.duration_ms / 1000), 2)

    def log(self, logger) -> None:
        logger.info(
            "Pipeline summary | mode={} total={} ok={} skipped={} failed={} "
            "rows={} throughput={} rows/s duration={}ms",
            self.mode.value,
            self.total, self.succeeded, self.skipped, self.failed,
            self.total_rows, self.throughput_rows_per_sec, self.duration_ms,
        )
        if self.mode == PipelineMode.REPAIR:
            logger.info(
                "Repair summary | gaps_found={} gaps_healed={}",
                self.total_gaps_found, self.total_gaps_healed,
            )
        for r in self.results:
            if r.error:
                logger.warning("  ✗ {}", r)
            elif r.skipped:
                logger.debug("  ↷ {}", r)
            else:
                logger.debug("  ✓ {}", r)


@runtime_checkable
class PipelineStrategy(Protocol):
    async def execute_pair(
        self,
        symbol:    str,
        timeframe: str,
        idx:       int,
        total:     int,
        ctx:       PipelineContext,
    ) -> PairResult:
        ...
