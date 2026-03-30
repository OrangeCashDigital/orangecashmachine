"""
market_data/batch/strategies/base.py
=====================================

Contrato central del sistema de pipeline unificado.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Protocol, runtime_checkable

from core.logging.setup import bind_pipeline
from market_data.ingestion.rest.ohlcv_fetcher import HistoricalFetcherAsync
from market_data.storage.silver.silver_storage import SilverStorage
from market_data.storage.bronze.bronze_storage import BronzeStorage
from market_data.quality.pipeline import QualityPipeline
from services.state.cursor_store import CursorStore

_log = bind_pipeline("base")


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


# Nombres de clase de excepción que indican error transitorio
_TRANSIENT_ERROR_NAMES: frozenset[str] = frozenset({
    "TimeoutError", "ConnectionError", "OSError",
    "ConnectionRefusedError", "ConnectionResetError",
    "BrokenPipeError", "ChunkFetchError",
    # asyncio / aiohttp
    "ClientConnectorError", "ServerDisconnectedError",
    "ClientOSError", "ClientResponseError",
})

# Substrings en el mensaje de error que indican error transitorio de red/exchange
_TRANSIENT_ERROR_MSGS: tuple[str, ...] = (
    "timeout", "timed out", "rate limit", "429", "503", "502",
    "connection", "network", "session is closed", "temporarily",
    "too many requests", "service unavailable",
)


@dataclass
class PairResult:
    symbol:      str
    timeframe:   str
    mode:        PipelineMode
    exchange_id: str  = ""
    rows:        int  = 0
    skipped:     bool = False
    error:       str  = ""
    error_type:  str  = ""
    duration_ms: int  = 0
    gaps_found:  int  = 0
    gaps_healed: int  = 0
    chunks:      int  = 0

    @property
    def success(self) -> bool:
        return not self.error and not self.skipped

    @property
    def is_transient_error(self) -> bool:
        """
        Clasifica el error como transitorio usando dos criterios independientes:
        1. error_type: nombre exacto de la clase de excepción (capturado en StrategyMixin)
        2. error: substrings de mensaje de red/exchange (insensible a mayúsculas)

        Separar los dos criterios evita falsos positivos: comparar el *mensaje*
        contra nombres de clase ("TimeoutError" in "connection timed out") nunca
        coincide, mientras que comparar substrings de red sí es semánticamente correcto.
        """
        if self.error_type in _TRANSIENT_ERROR_NAMES:
            return True
        err_lower = self.error.lower()
        return any(msg in err_lower for msg in _TRANSIENT_ERROR_MSGS)

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
    results:             List[PairResult] = field(default_factory=list)
    duration_ms:         int              = 0
    mode:                PipelineMode     = PipelineMode.INCREMENTAL
    degraded_exchanges:  List[str]        = field(default_factory=list)

    @property
    def failed_exchanges(self) -> List[str]:
        seen = []
        for r in self.results:
            if r.error and r.exchange_id and r.exchange_id not in seen:
                seen.append(r.exchange_id)
        return seen

    @property
    def status(self) -> str:
        all_degraded = set(self.degraded_exchanges) | set(self.failed_exchanges)
        if self.failed == 0 and not all_degraded:
            return "ok"
        if all_degraded and self.succeeded > 0:
            return "degraded"
        return "failed"

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

    def log(self, log) -> None:
        """Emite el resumen usando el logger inyectado (DI — no usa _log de módulo)."""
        all_degraded = (
            self.degraded_exchanges
            + [f"{e}(errors)" for e in self.failed_exchanges
               if e not in self.degraded_exchanges]
        ) or ["none"]
        log.info(
            "Pipeline summary",
            mode=self.mode.value, status=self.status,
            total=self.total, ok=self.succeeded,
            skipped=self.skipped, failed=self.failed,
            rows=self.total_rows,
            throughput_rows_per_sec=self.throughput_rows_per_sec,
            duration_ms=self.duration_ms,
            degraded=all_degraded,
        )
        if self.mode == PipelineMode.REPAIR:
            log.info(
                "Repair summary",
                gaps_found=self.total_gaps_found,
                gaps_healed=self.total_gaps_healed,
            )
        for r in self.results:
            if r.error:
                log.warning("Pair result", status="error", pair=str(r))
            elif r.skipped:
                log.debug("Pair result", status="skipped", pair=str(r))
            else:
                log.debug("Pair result", status="ok", pair=str(r))


# ==========================================================
# StrategyMixin
# ==========================================================

class StrategyMixin:
    """
    Boilerplate compartido: timing, error_type, métricas, logging de fallo.

    Uso
    ---
    class MiStrategy(StrategyMixin):
        _mode = PipelineMode.INCREMENTAL

        async def _run(self, symbol, timeframe, idx, total, ctx, result):
            ...  # solo lógica de negocio
    """

    _mode: "PipelineMode"

    async def execute_pair(
        self,
        symbol:    str,
        timeframe: str,
        idx:       int,
        total:     int,
        ctx:       "PipelineContext",
    ) -> "PairResult":
        from services.observability.metrics import PIPELINE_ERRORS  # evita circular

        result     = PairResult(symbol=symbol, timeframe=timeframe, mode=self._mode, exchange_id=ctx.exchange_id)
        pair_start = time.monotonic()

        try:
            await self._run(symbol, timeframe, idx, total, ctx, result)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            result.error       = str(exc)
            result.error_type  = type(exc).__name__
            result.duration_ms = int((time.monotonic() - pair_start) * 1000)
            error_label = "transient" if result.is_transient_error else "fatal"
            PIPELINE_ERRORS.labels(
                exchange=ctx.exchange_id, error_type=error_label,
            ).inc()
            _log.bind(
                mode=self._mode.value, exchange=ctx.exchange_id,
                symbol=symbol, timeframe=timeframe,
                idx=idx, total=total,
                error_type=result.error_type, error=str(exc),
                duration_ms=result.duration_ms,
            ).error("Strategy fallida")
        finally:
            if not result.duration_ms:
                result.duration_ms = int((time.monotonic() - pair_start) * 1000)

        return result

    async def _run(
        self,
        symbol:    str,
        timeframe: str,
        idx:       int,
        total:     int,
        ctx:       "PipelineContext",
        result:    "PairResult",
    ) -> None:
        raise NotImplementedError(f"{type(self).__name__} debe implementar _run()")


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
