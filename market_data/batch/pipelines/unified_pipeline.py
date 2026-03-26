"""
market_data/batch/pipelines/unified_pipeline.py
================================================

Pipeline unificado con mode explicito.
Unico punto de entrada para todos los modos de ingestion OHLCV.

Concurrencia
------------
Usa un producer/worker pool en lugar de asyncio.gather ilimitado.
Esto evita crear miles de coroutines simultáneas ("over-scheduling")
y da control real sobre el paralelismo.
"""

from __future__ import annotations

import asyncio
import time
from typing import List, Literal, Optional

from loguru import logger

from market_data.batch.pipelines.quality_pipeline import QualityPipeline
from market_data.batch.storage.bronze_storage import BronzeStorage
from market_data.batch.storage.silver_storage import SilverStorage
from market_data.batch.strategies.base import (
    PairResult,
    PipelineContext,
    PipelineMode,
    PipelineStrategy,
    PipelineSummary,
)
from market_data.batch.strategies.backfill import BackfillStrategy
from market_data.batch.strategies.incremental import IncrementalStrategy
from market_data.batch.strategies.repair import RepairStrategy
from market_data.batch.transformers.transformer import OHLCVTransformer
from services.exchange.ccxt_adapter import CCXTAdapter
from services.state.cursor_store import (
    CursorStore,
    InMemoryCursorStore,
    build_cursor_store_from_config,
)

DEFAULT_MAX_CONCURRENCY: int = 6
PipelineModeStr = Literal["incremental", "backfill", "repair"]


def _build_cursor_store_safe() -> CursorStore:
    try:
        store = build_cursor_store_from_config()
        if store.is_healthy():
            return store
        logger.warning("Redis no disponible -- cursor store en memoria (fallback)")
        return InMemoryCursorStore()
    except Exception as exc:
        logger.warning("CursorStore init failed (fallback) | error={}", exc)
        return InMemoryCursorStore()


class UnifiedPipeline:
    """
    Pipeline unificado de ingestion OHLCV.

    Uso
    ---
    pipeline = UnifiedPipeline(
        symbols         = ["BTC/USDT"],
        timeframes      = ["1h", "4h", "1d"],
        start_date      = "2024-01-01",
        exchange_client = adapter,
        market_type     = "spot",
    )

    summary = await pipeline.run(mode="incremental")
    summary = await pipeline.run(mode="backfill")
    summary = await pipeline.run(mode="repair")
    """

    def __init__(
        self,
        symbols:         List[str],
        timeframes:      List[str],
        start_date:      str,
        exchange_client: CCXTAdapter,
        max_concurrency: int                   = DEFAULT_MAX_CONCURRENCY,
        cursor_store:    Optional[CursorStore] = None,
        backfill_mode:   bool                  = True,
        market_type:     str                   = "spot",
    ) -> None:
        if not symbols:
            raise ValueError("symbols no puede estar vacio")
        if not timeframes:
            raise ValueError("timeframes no puede estar vacio")
        if not start_date:
            raise ValueError("start_date es obligatorio")
        if exchange_client is None:
            raise ValueError("exchange_client es obligatorio")

        self.symbols         = symbols
        self.timeframes      = timeframes
        self.start_date      = start_date
        self.max_concurrency = max_concurrency
        self.market_type     = market_type.lower()
        self.backfill_mode   = backfill_mode
        self._exchange_id    = getattr(exchange_client, "_exchange_id", "unknown")

        cursor  = cursor_store or _build_cursor_store_safe()
        bronze  = BronzeStorage(exchange=self._exchange_id)
        silver  = SilverStorage(
            exchange=self._exchange_id,
            market_type=self.market_type,
            redis_client=getattr(cursor, '_client', None),
        )
        quality = QualityPipeline()

        from market_data.batch.fetchers.fetcher import HistoricalFetcherAsync
        fetcher = HistoricalFetcherAsync(
            storage           = silver,
            transformer       = OHLCVTransformer(),
            exchange_client   = exchange_client,
            cursor_store      = cursor,
            backfill_mode     = self.backfill_mode,
            market_type       = market_type,
            config_start_date = start_date,
        )

        self._ctx = PipelineContext(
            fetcher     = fetcher,
            storage     = silver,
            bronze      = bronze,
            cursor      = cursor,
            quality     = quality,
            exchange_id = self._exchange_id,
            market_type = self.market_type,
            start_date  = start_date,
        )

        self._strategies: dict[PipelineMode, PipelineStrategy] = {
            PipelineMode.INCREMENTAL: IncrementalStrategy(),
            PipelineMode.BACKFILL:    BackfillStrategy(),
            PipelineMode.REPAIR:      RepairStrategy(),
        }

    # ======================================================
    # Public
    # ======================================================

    async def run(self, mode: PipelineModeStr = "incremental") -> PipelineSummary:
        pipeline_mode = PipelineMode(mode)
        strategy      = self._strategies[pipeline_mode]
        pairs         = [(s, tf) for s in self.symbols for tf in self.timeframes]
        total_pairs   = len(pairs)

        await self._ctx.fetcher.ensure_exchange()

        logger.info(
            "UnifiedPipeline iniciando | mode={} exchange={} market={} "
            "simbolos={} timeframes={} pares={} concurrencia_max={}",
            mode, self._exchange_id, self.market_type,
            len(self.symbols), len(self.timeframes),
            total_pairs, self.max_concurrency,
        )

        pipeline_start = time.monotonic()
        results        = await self._run_worker_pool(strategy, pairs, pipeline_mode)
        duration_ms    = int((time.monotonic() - pipeline_start) * 1000)

        summary = PipelineSummary(
            results     = results,
            duration_ms = duration_ms,
            mode        = pipeline_mode,
        )
        summary.log(logger)

        if summary.failed:
            logger.warning(
                "Pipeline con errores | mode={} fallidos={}/{} transient={}",
                mode, summary.failed, summary.total,
                sum(1 for r in summary.results if r.is_transient_error),
            )
        else:
            logger.success(
                "Pipeline completado | mode={} rows={} pares={} "
                "throughput={} rows/s duration={}ms",
                mode, summary.total_rows, summary.total,
                summary.throughput_rows_per_sec, duration_ms,
            )

        return summary

    # ======================================================
    # Worker pool (producer/consumer)
    # ======================================================

    async def _run_worker_pool(
        self,
        strategy:     PipelineStrategy,
        pairs:        List[tuple[str, str]],
        mode:         PipelineMode,
    ) -> List[PairResult]:
        """
        Producer/consumer pool: evita crear todas las coroutines a la vez.

        El producer encola pares de a uno. Cada worker toma un par,
        lo procesa, y deposita el resultado. El número de workers activos
        nunca supera max_concurrency.
        """
        queue:   asyncio.Queue = asyncio.Queue()
        results: List[PairResult] = []
        total   = len(pairs)

        # Encolar todos los trabajos
        for idx, (symbol, tf) in enumerate(pairs, 1):
            await queue.put((idx, symbol, tf))

        async def worker() -> None:
            while True:
                item = await queue.get()
                try:
                    idx, symbol, tf = item
                    result = await self._execute_pair(strategy, symbol, tf, idx, total, mode)
                    results.append(result)
                finally:
                    queue.task_done()

        workers = [
            asyncio.create_task(worker())
            for _ in range(self.max_concurrency)
        ]

        try:
            # queue.join() bloquea hasta que todos los items fueron procesados.
            # Es más limpio que sentinels: no hay riesgo de contar mal los None.
            await queue.join()
        except asyncio.CancelledError:
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            logger.warning("Pipeline cancelado — {} workers detenidos", len(workers))
            raise
        finally:
            for w in workers:
                if not w.done():
                    w.cancel()

        return results

    # ======================================================
    # Single pair execution (barrera de seguridad)
    # ======================================================

    async def _execute_pair(
        self,
        strategy:  PipelineStrategy,
        symbol:    str,
        timeframe: str,
        idx:       int,
        total:     int,
        mode:      PipelineMode,
    ) -> PairResult:
        """
        Barrera de seguridad alrededor de strategy.execute_pair.

        Las strategies capturan sus propias excepciones internamente.
        Este wrapper captura cualquier escape inesperado (bug de infra,
        error en el semáforo, etc.) y lo convierte en PairResult con error,
        garantizando que el worker pool nunca pierda un resultado.
        """
        try:
            return await strategy.execute_pair(
                symbol    = symbol,
                timeframe = timeframe,
                idx       = idx,
                total     = total,
                ctx       = self._ctx,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "execute_pair unhandled | exchange={} symbol={} tf={} "
                "error_type={} error={}",
                self._exchange_id, symbol, timeframe,
                type(exc).__name__, exc,
            )
            return PairResult(
                symbol     = symbol,
                timeframe  = timeframe,
                mode       = mode,
                error      = str(exc),
                error_type = type(exc).__name__,
            )
