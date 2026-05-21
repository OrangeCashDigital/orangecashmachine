# -*- coding: utf-8 -*-
"""
market_data/application/pipelines/trades_pipeline.py
====================================================

Pipeline de ingestion de trades (tick data).

Dominio
-------
Transacciones individuales — sin timeframe, append-only.
Schema: timestamp, price, amount, side, trade_id.

Diferencias vs OHLCVPipeline
------------------------------
- Sin timeframe  : cursor por símbolo, no por símbolo×timeframe
- Volumen masivo : paginación agresiva, storage append-only
- Sin repair     : trades son inmutables por definición

Principios: SOLID · KISS · DRY · SafeOps
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import List, Literal, Optional

from market_data.application.pipelines._worker_pool import run_worker_pool
from market_data.ports.inbound.pipeline_trigger import PipelineTriggerPort

from loguru import logger

# Dependencias concretas eliminadas del application layer (DIP — BC-05/BC-08).
# CCXTAdapter, TradesFetcher, TradesStorage se inyectan por constructor
# desde market_data.factories.pipeline_factory (composition root).
from market_data.ports.outbound.exchange_client import ExchangeClientPort
from market_data.ports.outbound.fetcher import TradesFetcherPort
from market_data.ports.outbound.storage import TradesStoragePort

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

TradesPipelineMode = Literal["incremental", "backfill", "repair"]

# ---------------------------------------------------------------------------
# Result / Summary
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class TradesResult:
    """Resultado de ingestion para un símbolo."""

    symbol: str
    success: bool = False
    rows: int = 0
    error: Optional[str] = None
    duration_ms: int = 0

    @property
    def skipped(self) -> bool:
        return self.success and self.rows == 0


@dataclass
class TradesSummary:
    """Resumen agregado de un run de TradesPipeline."""

    results: List[TradesResult] = field(default_factory=list)
    duration_ms: int = 0
    mode: str = "incremental"

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def succeeded(self) -> int:
        return sum(1 for r in self.results if r.success)

    @property
    def failed(self) -> int:
        return sum(1 for r in self.results if not r.success and r.error is not None)

    @property
    def skipped(self) -> int:
        return sum(1 for r in self.results if r.skipped)

    @property
    def total_rows(self) -> int:
        return sum(r.rows for r in self.results)

    @property
    def status(self) -> str:
        if self.failed == 0:
            return "ok"
        if self.succeeded > 0:
            return "partial"
        return "failed"


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


class TradesPipeline(PipelineTriggerPort):
    """
    Pipeline de ingestion de trades (tick data).

    Usa el mismo patrón producer/worker pool que OHLCVPipeline para
    controlar concurrencia sin crear coroutines ilimitadas.

    SafeOps
    -------
    Constructor valida en tiempo de construcción (fail-fast).
    Errores por símbolo son capturados — nunca abortan el pipeline completo.
    """

    def __init__(
        self,
        symbols: List[str],
        exchange_client: "ExchangeClientPort",
        fetcher: "TradesFetcherPort",  # obligatorio — inyectar desde factory (DIP)
        storage: "TradesStoragePort",  # obligatorio — inyectar desde factory (DIP)
        market_type: str = "spot",
        dry_run: bool = False,
        max_concurrency: int = 4,
    ) -> None:
        # Fail-fast: todas las dependencias concretas se inyectan desde
        # ConcretePipelineFactory — TradesPipeline no resuelve infraestructura.
        if not symbols:
            raise ValueError("TradesPipeline: symbols no puede estar vacío")
        if exchange_client is None:
            raise TypeError("TradesPipeline: 'exchange_client' es obligatorio")
        if fetcher is None:
            raise TypeError(
                "TradesPipeline: 'fetcher' es obligatorio. Inyectar TradesFetcher desde el composition root."
            )
        if storage is None:
            raise TypeError(
                "TradesPipeline: 'storage' es obligatorio. Inyectar TradesStorage desde el composition root."
            )
        if max_concurrency < 1:
            raise ValueError("TradesPipeline: max_concurrency debe ser >= 1")

        self.symbols: List[str] = symbols
        self.market_type: str = market_type.lower()
        self.dry_run: bool = dry_run
        self.max_concurrency: int = max_concurrency
        self._exchange_id: str = getattr(exchange_client, "_exchange_id", "unknown")
        self._log = logger.bind(
            exchange=self._exchange_id,
            pipeline="trades",
        )

        # DIP: fetcher y storage inyectados desde ConcretePipelineFactory.
        # TradesPipeline no conoce implementaciones concretas (Clean Architecture).
        self._fetcher = fetcher
        self._storage = storage

    async def run(self, mode: TradesPipelineMode = "incremental") -> TradesSummary:
        """
        Ejecuta la ingestion de trades para todos los símbolos.

        mode="incremental" : desde el último timestamp almacenado.
        mode="backfill"    : desde el principio disponible (since_ms=None).

        SafeOps: errores por símbolo se capturan y loguean — el pipeline
        continúa con los demás símbolos.
        """
        self._log.info(
            "TradesPipeline start | mode={} symbols={} concurrency={}",
            mode,
            len(self.symbols),
            self.max_concurrency,
        )

        pipeline_start = time.monotonic()
        results = await self._run_worker_pool(mode)
        duration_ms = int((time.monotonic() - pipeline_start) * 1000)

        summary = TradesSummary(
            results=results,
            duration_ms=duration_ms,
            mode=mode,
        )

        self._log.info(
            "TradesPipeline done | mode={} ok={} failed={} skipped={} total_rows={} duration_ms={}",
            mode,
            summary.succeeded,
            summary.failed,
            summary.skipped,
            summary.total_rows,
            duration_ms,
        )
        return summary

    async def _run_worker_pool(self, mode: str) -> List[TradesResult]:
        """Delega al worker pool genérico — sin lógica de concurrencia local."""
        items = list(enumerate(self.symbols, 1))

        async def _execute(item) -> TradesResult:
            idx, symbol = item
            return await self._fetch_symbol(symbol, mode, idx)

        results, _ = await run_worker_pool(
            items=items,
            execute_fn=_execute,
            max_concurrency=self.max_concurrency,
            exchange_id=self._exchange_id,
            log=self._log,
        )
        return results

    async def _fetch_symbol(self, symbol: str, mode: str, idx: int) -> TradesResult:
        """Fetcha trades para un símbolo con captura de errores."""
        start = time.monotonic()
        try:
            since_ms = None  # incremental usa cursor interno en TradesFetcher
            rows = await self._fetcher.fetch_symbol(symbol, since_ms=since_ms)
            duration_ms = int((time.monotonic() - start) * 1000)
            self._log.info(
                "  [{}/{}] {} | rows={} duration={}ms",
                idx,
                len(self.symbols),
                symbol,
                rows,
                duration_ms,
            )
            return TradesResult(
                symbol=symbol,
                success=True,
                rows=rows,
                duration_ms=duration_ms,
            )
        except Exception as exc:
            duration_ms = int((time.monotonic() - start) * 1000)
            self._log.error(
                "  [{}/{}] {} FAILED | err={} duration={}ms",
                idx,
                len(self.symbols),
                symbol,
                exc,
                duration_ms,
            )
            return TradesResult(
                symbol=symbol,
                success=False,
                error=str(exc),
                duration_ms=duration_ms,
            )

    def __repr__(self) -> str:
        return (
            f"TradesPipeline("
            f"exchange={self._exchange_id!r}, "
            f"symbols={len(self.symbols)}, "
            f"market_type={self.market_type!r}, "
            f"dry_run={self.dry_run})"
        )
