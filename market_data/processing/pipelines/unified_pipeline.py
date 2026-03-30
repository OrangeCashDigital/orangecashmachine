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

from core.logging.setup import bind_pipeline

_log = bind_pipeline("pipeline")

from market_data.quality.pipeline import QualityPipeline
from market_data.storage.bronze.bronze_storage import BronzeStorage
from market_data.storage.silver.silver_storage import SilverStorage
from market_data.processing.strategies.base import (
    PairResult,
    PipelineContext,
    PipelineMode,
    PipelineStrategy,
    PipelineSummary,
)
from market_data.processing.strategies.backfill import BackfillStrategy
from market_data.processing.strategies.incremental import IncrementalStrategy
from market_data.processing.strategies.repair import RepairStrategy
from market_data.core.transformers.transformer import OHLCVTransformer
from services.exchange.ccxt_adapter import CCXTAdapter, ExchangeCircuitOpenError, get_breaker_state
from services.observability.metrics import FETCH_ABORTS_TOTAL
from services.state.cursor_store import (
    CursorStore,
    InMemoryCursorStore,
    build_cursor_store_from_config,
)

DEFAULT_MAX_CONCURRENCY: int = 6
PipelineModeStr = Literal["incremental", "backfill", "repair"]


class _ExchangeAbortError(Exception):
    """
    Señal interna: abortar todos los pares de este exchange.
    No es CancelledError — no mata el pipeline global.
    Seguro para uso futuro con múltiples exchanges en paralelo.
    """
    __slots__ = ("exchange_id",)

    def __init__(self, exchange_id: str) -> None:
        self.exchange_id = exchange_id
        super().__init__(f"Circuit open — aborting exchange={exchange_id}")


def _build_cursor_store_safe() -> CursorStore:
    try:
        store = build_cursor_store_from_config()
        if store.is_healthy():
            return store
        _log.warning("Redis no disponible — cursor store en memoria (fallback)")
        return InMemoryCursorStore()
    except Exception as exc:
        _log.bind(error=str(exc)).warning("CursorStore init failed — fallback")
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

        from market_data.ingestion.rest.ohlcv_fetcher import HistoricalFetcherAsync
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
        self._log = bind_pipeline("pipeline", exchange=self._exchange_id)

    # ======================================================
    # Public
    # ======================================================

    async def run(self, mode: PipelineModeStr = "incremental") -> PipelineSummary:
        pipeline_mode = PipelineMode(mode)
        strategy      = self._strategies[pipeline_mode]
        pairs         = [(s, tf) for s in self.symbols for tf in self.timeframes]
        total_pairs   = len(pairs)

        await self._ctx.fetcher.ensure_exchange()

        self._log.bind(
            mode=mode, market=self.market_type,
            symbols=len(self.symbols), timeframes=len(self.timeframes),
            pairs=total_pairs, concurrency=self.max_concurrency,
        ).info("UnifiedPipeline iniciando")

        pipeline_start = time.monotonic()
        results, degraded_exchanges = await self._run_worker_pool(strategy, pairs, pipeline_mode)
        duration_ms    = int((time.monotonic() - pipeline_start) * 1000)

        summary = PipelineSummary(
            results             = results,
            duration_ms         = duration_ms,
            mode                = pipeline_mode,
            degraded_exchanges  = degraded_exchanges,
        )
        summary.log(self._log)

        if summary.status == "degraded":
            self._log.bind(
                mode=mode, degraded_exchanges=summary.degraded_exchanges,
                failed=summary.failed, total=summary.total,
                transient=sum(1 for r in summary.results if r.is_transient_error),
            ).warning("Pipeline DEGRADED")
        elif summary.status == "failed":
            self._log.bind(
                mode=mode, failed=summary.failed, total=summary.total,
                transient=sum(1 for r in summary.results if r.is_transient_error),
            ).error("Pipeline FAILED")
        else:
            self._log.bind(
                mode=mode, rows=summary.total_rows, pairs=summary.total,
                throughput_rows_per_sec=summary.throughput_rows_per_sec,
                duration_ms=duration_ms,
            ).success("Pipeline OK")

        return summary

    # ======================================================
    # Worker pool (producer/consumer)
    # ======================================================

    async def _run_worker_pool(
        self,
        strategy:     PipelineStrategy,
        pairs:        List[tuple[str, str]],
        mode:         PipelineMode,
    ) -> tuple[List[PairResult], List[str]]:
        """
        Producer/consumer pool: evita crear todas las coroutines a la vez.

        El producer encola pares de a uno. Cada worker toma un par,
        lo procesa, y deposita el resultado. El número de workers activos
        nunca supera max_concurrency.
        """
        queue:        asyncio.Queue  = asyncio.Queue()
        results:      List[PairResult] = []
        degraded:     List[str]        = []
        total       = len(pairs)
        # Señal compartida: cuando un worker detecta circuit open, todos los
        # demás salen limpiamente sin cooldown duplicado ni reintentos inútiles.
        abort_event: asyncio.Event = asyncio.Event()

        # Encolar todos los trabajos
        for idx, (symbol, tf) in enumerate(pairs, 1):
            await queue.put((idx, symbol, tf))

        async def worker() -> None:
            while True:
                # Si el exchange fue abortado por otro worker, salir sin tomar más trabajo.
                if abort_event.is_set():
                    try:
                        queue.get_nowait()
                        queue.task_done()
                    except asyncio.QueueEmpty:
                        pass
                    return

                try:
                    item = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return

                try:
                    idx, symbol, tf = item
                    result = await self._execute_pair(
                        strategy, symbol, tf, idx, total, mode, abort_event
                    )
                    results.append(result)
                except _ExchangeAbortError as exc:
                    # Señalizar a todos los workers antes de drenar.
                    abort_event.set()
                    drained = 0
                    while not queue.empty():
                        try:
                            queue.get_nowait()
                            queue.task_done()
                            drained += 1
                        except asyncio.QueueEmpty:
                            break
                    if exc.exchange_id not in degraded:
                        degraded.append(exc.exchange_id)
                    self._log.bind(drained=drained, aborted_exchange=exc.exchange_id).warning("Exchange aborted — pares drenados")
                    return
                finally:
                    queue.task_done()

        workers = [
            asyncio.create_task(worker())
            for _ in range(self.max_concurrency)
        ]

        try:
            # queue.join() con timeout — evita deadlock si un worker muere
            # silenciosamente antes de llamar task_done().
            # Timeout = 1h, suficiente para cualquier pipeline histórico real.
            await asyncio.wait_for(queue.join(), timeout=3600)
        except asyncio.TimeoutError:
            self._log.bind(timeout_s=3600).error("Worker pool timed out — forzando shutdown")
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            raise RuntimeError(
                f"Worker pool timed out after 3600s | exchange={self._exchange_id}"
            )
        except asyncio.CancelledError:
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            self._log.bind(workers=len(workers)).warning("Pipeline cancelado — workers detenidos")
            raise
        finally:
            for w in workers:
                if not w.done():
                    w.cancel()

        return results, degraded

    # ======================================================
    # Single pair execution (barrera de seguridad)
    # ======================================================

    async def _execute_pair(
        self,
        strategy:    PipelineStrategy,
        symbol:      str,
        timeframe:   str,
        idx:         int,
        total:       int,
        mode:        PipelineMode,
        abort_event: asyncio.Event,
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
        except ExchangeCircuitOpenError as exc:
            # Si otro worker ya disparó el abort, salir sin duplicar cooldown.
            if abort_event.is_set():
                raise _ExchangeAbortError(self._exchange_id) from exc

            # Primer worker en detectar circuit open: cooldown coordinado.
            # Los demás workers detectarán abort_event y saltarán esto.
            _bs = get_breaker_state(self._exchange_id)
            _cooldown_s = max(1.0, _bs["cooldown_remaining_ms"] / 1000)
            self._log.bind(
                symbol=symbol, timeframe=timeframe,
                failures=_bs["fail_counter"], cooldown_s=round(_cooldown_s, 1),
            ).warning("Circuit open — sleeping cooldown")
            await asyncio.sleep(_cooldown_s)

            # Verificar si otro worker abortó durante el sleep.
            if abort_event.is_set():
                raise _ExchangeAbortError(self._exchange_id) from exc

            try:
                self._log.bind(symbol=symbol, timeframe=timeframe).info("Circuit cooldown retry")
                return await strategy.execute_pair(
                    symbol    = symbol,
                    timeframe = timeframe,
                    idx       = idx,
                    total     = total,
                    ctx       = self._ctx,
                )
            except ExchangeCircuitOpenError:
                # Breaker todavia abierto — abortar exchange, no el pipeline.
                _bs2 = get_breaker_state(self._exchange_id)
                FETCH_ABORTS_TOTAL.labels(exchange=self._exchange_id).inc()
                self._log.bind(
                    symbol=symbol, timeframe=timeframe,
                    failures=_bs2["fail_counter"],
                    cooldown_remaining_ms=_bs2["cooldown_remaining_ms"],
                ).warning("Circuit open after retry — aborting exchange")
                raise _ExchangeAbortError(self._exchange_id) from exc
        except Exception as exc:
            self._log.bind(
                symbol=symbol, timeframe=timeframe,
                error_type=type(exc).__name__, error=str(exc),
            ).error("execute_pair unhandled")
            return PairResult(
                symbol      = symbol,
                timeframe   = timeframe,
                mode        = mode,
                exchange_id = self._exchange_id,
                error       = str(exc),
                error_type  = type(exc).__name__,
            )
