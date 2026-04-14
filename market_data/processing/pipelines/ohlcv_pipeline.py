"""
market_data/processing/pipelines/ohlcv_pipeline.py
================================================

Pipeline de ingestion OHLCV (candles agregados por timeframe).
Dominio exclusivo: open, high, low, close, volume por timeframe fijo.

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

from core.logging import bind_pipeline

_log = bind_pipeline("pipeline")

from market_data.quality.pipeline import QualityPipeline
from market_data.storage.bronze.bronze_storage import BronzeStorage
from market_data.storage.storage_protocol import OHLCVStorage
from market_data.storage.iceberg.iceberg_storage import IcebergStorage
from market_data.processing.pipelines._worker_pool import run_worker_pool
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
from market_data.adapters.exchange.ccxt_adapter import CCXTAdapter, ExchangeCircuitOpenError, get_breaker_state
from market_data.observability.metrics import FETCH_ABORTS_TOTAL
from infra.state.cursor_store import (
    CursorStore,
    InMemoryCursorStore,
    build_cursor_store_from_env,
)


def _build_storage(
    exchange:     str,
    market_type:  str,
    redis_client=None,                   # no-op — Iceberg no usa Redis lock
    dry_run:      bool              = False,
    cursor_store: "CursorStore | None" = None,
) -> "OHLCVStorage":
    """
    Factory de storage OHLCV — IcebergStorage es el único backend.

    IcebergStorage es el único backend activo.
    """
    _log.bind(backend="iceberg", exchange=exchange, market_type=market_type).debug(
        "storage_factory | IcebergStorage"
    )
    return IcebergStorage(
        exchange     = exchange,
        market_type  = market_type,
        dry_run      = dry_run,
        cursor_store = cursor_store,  # L2 cache cross-process
    )


DEFAULT_MAX_CONCURRENCY: int  = 6
# Tiempo de stagger entre arranque de workers consecutivos (segundos).
# Previene thundering herd: N workers × WORKER_STAGGER_S = ventana de dispersión.
# Ejemplo: 20 workers × 0.05s = 1s total de dispersión — imperceptible al usuario.
# Configurable aquí para ajuste fino sin tocar lógica de negocio.
WORKER_STAGGER_S: float = 0.05
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
    # build_cursor_store_from_config() requiere AppConfig explícito y lanza
    # RuntimeError si se llama sin él — lo que causaba que el cursor store
    # cayera SIEMPRE a InMemoryCursorStore aunque Redis estuviera disponible,
    # haciendo que los cursores no persistieran entre runs.
    # build_cursor_store_from_env() resuelve config desde variables de entorno,
    # consistente con el resto del sistema (factories.py).
    try:
        store = build_cursor_store_from_env()
        if store.is_healthy():
            return store
        _log.warning("Redis no disponible — cursor store en memoria (fallback)")
        return InMemoryCursorStore()
    except Exception as exc:
        _log.bind(error=str(exc)).warning("CursorStore init failed — fallback")
        return InMemoryCursorStore()


class OHLCVPipeline:
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
        backfill_mode:      bool                  = True,
        market_type:        str                   = "spot",
        dry_run:            bool                  = False,
        auto_lookback_days: int                   = 3650,
    ) -> None:
        if not symbols:
            raise ValueError("symbols no puede estar vacio")
        if not timeframes:
            raise ValueError("timeframes no puede estar vacio")
        if not start_date:
            raise ValueError("start_date es obligatorio")
        # "auto" es el sentinel para lookback dinámico — se resuelve en el fetcher.
        # Una fecha ISO explícita actúa como floor; el fetcher la respeta en path C.
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
        silver  = _build_storage(
            exchange     = self._exchange_id,
            market_type  = self.market_type,
            redis_client = getattr(cursor, '_client', None),
            dry_run      = dry_run,
            cursor_store = cursor,  # L2 cache cross-process en get_last_timestamp
        )
        quality = QualityPipeline()

        from market_data.ingestion.rest.ohlcv_fetcher import HistoricalFetcherAsync
        # overlap NO se resuelve globalmente aquí.
        # Razón: un único max() sobre todos los timeframes produciría
        # solapamiento excesivo en timeframes largos (ej: overlap de 1m
        # aplicado a 1d = 15 días de reingesta innecesaria).
        # overlap_for_timeframe(tf, exchange) se invoca por par dentro de
        # _resolve_start_timestamp del fetcher, que ya recibe el exchange_id
        # via self._exchange (CCXTAdapter._exchange_id).
        # El fetcher usa DEFAULT_OVERLAP_BARS como base y lo ajusta por par.
        fetcher = HistoricalFetcherAsync(
            storage             = silver,
            transformer         = OHLCVTransformer(),
            exchange_client     = exchange_client,
            cursor_store        = cursor,
            backfill_mode       = self.backfill_mode,
            market_type         = market_type,
            config_start_date   = start_date,
            auto_lookback_days  = auto_lookback_days,
            # overlap_bars omitido: el fetcher resuelve por par via
            # overlap_for_timeframe(timeframe, exchange=exchange_id)
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
        ).info("OHLCVPipeline iniciando")

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

        # Validación de completitud post-pipeline (non-blocking)
        await self._run_completeness_check(summary)

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
        Delega al worker pool genérico con circuit breaker via on_abort.

        on_abort coordina el abort entre workers cuando _ExchangeAbortError
        se detecta: activa abort_event, drena la queue y registra el exchange
        degradado. El pool genérico gestiona el drenado y la cancelación.

        Ref: Stevens, "Unix Network Programming" — thundering herd prevention
        """
        total:    int       = len(pairs)
        degraded: List[str] = []

        # abort_event compartido: _execute_pair lo recibe para detectar
        # si otro worker ya abortó y saltar el cooldown duplicado.
        abort_event: asyncio.Event = asyncio.Event()

        items = [(idx, sym, tf) for idx, (sym, tf) in enumerate(pairs, 1)]

        async def _execute(item) -> PairResult:
            idx, symbol, tf = item
            return await self._execute_pair(
                strategy, symbol, tf, idx, total, mode, abort_event
            )

        def _on_abort(item, exc: Exception) -> bool:
            if not isinstance(exc, _ExchangeAbortError):
                return False
            abort_event.set()
            if exc.exchange_id not in degraded:
                degraded.append(exc.exchange_id)
                self._log.bind(aborted_exchange=exc.exchange_id).warning(
                    "Exchange aborted — circuit breaker"
                )
            return True

        results, _ = await run_worker_pool(
            items           = items,
            execute_fn      = _execute,
            max_concurrency = self.max_concurrency,
            exchange_id     = self._exchange_id,
            log             = self._log,
            on_abort        = _on_abort,
        )
        return results, degraded

    # ======================================================
    # Completeness check (post-pipeline)
    # ======================================================

    async def _run_completeness_check(self, summary: "PipelineSummary") -> None:
        """
        Valida cobertura temporal de las series escritas en este pipeline.

        Corre después de que el pipeline termina — nunca bloquea escrituras.
        Solo verifica series con rows > 0 (saltea skipped/errores).

        Estrategia
        ----------
        1. Leer datos desde storage (load_ohlcv con start=None → todo el dataset)
        2. Ejecutar scan_gaps sobre el DataFrame completo
        3. Loguear gaps por severidad — warning si high, info si medium/low
        4. SafeOps: cualquier excepción es capturada y logueada, nunca relanzada

        Nota: load_ohlcv está en el contrato OHLCVStorage — disponible en
        IcebergStorage sin necesidad de getattr.
        """
        from market_data.processing.utils.gap_utils import scan_gaps
        from market_data.quality.invariants.invariants import check_dataset_invariants

        written = [r for r in summary.results if r.success and r.rows > 0]
        if not written:
            return

        self._log.bind(mode="completeness", series=len(written)).debug(
            "Completeness check iniciando"
        )

        total_gaps   = 0
        total_high   = 0
        series_clean = 0

        for result in written:
            try:
                df = self._ctx.storage.load_ohlcv(
                    symbol=result.symbol, timeframe=result.timeframe,
                )
                if df is None or df.empty:
                    continue

                gaps = scan_gaps(df, result.timeframe)
                if not gaps:
                    series_clean += 1
                    continue

                high   = sum(1 for g in gaps if g.severity == "high")
                medium = sum(1 for g in gaps if g.severity == "medium")
                low    = len(gaps) - high - medium
                total_gaps += len(gaps)
                total_high += high

                _lvl = self._log.warning if high > 0 else self._log.info
                _lvl(
                    "Completeness gap detected | {}/{} exchange={} "
                    "gaps={} high={} medium={} low={}",
                    result.symbol, result.timeframe, self._exchange_id,
                    len(gaps), high, medium, low,
                )

                # Verificar invariantes formales del dataset completo.
                # Solo se ejecuta si load_ohlcv tuvo éxito — datos ya disponibles.
                try:
                    _manifest = {
                        "symbol":     result.symbol,
                        "timeframe":  result.timeframe,
                        "exchange":   self._exchange_id,
                        "version":    1,
                        "partitions": [{"min_ts": str(df["timestamp"].min()),
                                        "max_ts": str(df["timestamp"].max()),
                                        "rows":   len(df)}],
                    }
                    _inv = check_dataset_invariants(_manifest, check_lag=True)
                    if not _inv.ok:
                        for violation in _inv.violations:
                            self._log.bind(
                                symbol=result.symbol, timeframe=result.timeframe,
                            ).warning("Invariant violation | {}", violation)
                    for warning in _inv.warnings:
                        self._log.bind(
                            symbol=result.symbol, timeframe=result.timeframe,
                        ).info("Invariant warning | {}", warning)
                except Exception as _inv_exc:
                    self._log.bind(error=str(_inv_exc)).debug(
                        "Invariant check skipped (non-critical)"
                    )

            except Exception as exc:
                self._log.bind(
                    symbol=result.symbol, timeframe=result.timeframe,
                    error=str(exc),
                ).warning("Completeness check failed for series (non-critical)")

        self._log.bind(
            mode="completeness",
            series_checked=len(written),
            series_clean=series_clean,
            total_gaps=total_gaps,
            total_high=total_high,
            status="OK" if total_high == 0 else "GAPS_DETECTED",
        ).info("Completeness check finalizado")

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
