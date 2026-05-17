"""
market_data/application/pipelines/ohlcv_pipeline.py
================================================

Pipeline de ingestion OHLCV (candles agregados por timeframe).
Dominio exclusivo: open, high, low, close, volume por timeframe fijo.

Concurrencia
------------
Usa un producer/worker pool en lugar de asyncio.gather ilimitado.
Esto evita crear miles de coroutines simultáneas ("over-scheduling")
y da control real sobre el paralelismo.

Throttle mid-run
----------------
AdaptiveThrottle se inyecta opcionalmente al pipeline.
Cada par ejecutado alimenta el throttle inmediatamente via _feed_throttle,
en lugar de volcarlo todo en batch al final del run.

Esto cierra el feedback loop dentro del run, no solo entre runs.
self.max_concurrency se sincroniza con throttle.current tras cada par —
observable en logs y en get_throttle_state().

Limitación: asyncio.Semaphore es inmutable — el semáforo del run activo
no cambia mid-run. La concurrencia actualizada se aplica en el próximo run.
Para soporte dinámico dentro del run, run_worker_pool debería aceptar
una callable en lugar de un int.
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import TYPE_CHECKING, Any, List, Literal, Optional

if TYPE_CHECKING:
    from market_data.ports.outbound.exchange_client import ExchangeClientPort
    from market_data.ports.outbound.throttle import ThrottlePort

from ocm.observability import bind_pipeline

_log = bind_pipeline("pipeline")


def _build_kafka_publisher_safe():
    """
    Construye KafkaOHLCVPublisher respetando el feature flag KAFKA_ENABLED.

    Jerarquía de decisión (Fail-Fast → Fail-Soft):
      1. KAFKA_ENABLED != true  → retorna None inmediatamente (flag apagado).
         No se intenta conexión — safe para entornos sin broker.
      2. KAFKA_ENABLED == true  → construye KafkaOHLCVPublisher.
         Si el broker no está disponible → captura excepción → retorna None.

    SSOT del flag: ocm.config.env_vars.KAFKA_ENABLED — nunca string literal.
    El flag refleja integrations.kafka.enabled del YAML via Hydra.

    El producer NO se inicia aquí — start() es async y esta función es sync.
    KafkaOHLCVPublisher.publish_chunk() hace lazy-start en el primer uso
    (idempotente: KafkaProducerAdapter._started protege el doble arranque).
    """
    from ocm.config.env_vars import KAFKA_ENABLED as _KAFKA_ENABLED_VAR
    kafka_flag = os.environ.get(_KAFKA_ENABLED_VAR, "false").strip().lower()
    if kafka_flag not in ("1", "true", "yes"):
        _log.debug(
            "Kafka deshabilitado ({}={}) — modo degradado (Iceberg directo)",
            _KAFKA_ENABLED_VAR, kafka_flag,
        )
        return None
    try:
        from market_data.infrastructure.kafka.producer import KafkaProducerAdapter
        from market_data.infrastructure.kafka.ohlcv_publisher import KafkaOHLCVPublisher
        producer = KafkaProducerAdapter.from_env()
        return KafkaOHLCVPublisher(producer=producer)
    except Exception as exc:
        _log.warning(
            "KafkaOHLCVPublisher no disponible — modo degradado (sin Kafka)",
            error=str(exc),
        )
        return None

from market_data.application.pipelines._worker_pool import run_worker_pool
from market_data.domain.policies.base import (
    PairResult,
    PipelineContext,
    PipelineMode,
    PipelineStrategy,
    PipelineSummary,
)
from market_data.application.strategies.backfill    import BackfillStrategy
from market_data.application.strategies.incremental import IncrementalStrategy
from market_data.application.strategies.repair      import RepairStrategy
from market_data.ports.outbound.state import CursorStorePort
from market_data.ports.inbound.pipeline_trigger import PipelineTriggerPort
from market_data.ports.outbound.resilience import ExchangeCircuitOpenError
from ocm.runtime.state import (
    build_cursor_store_from_env,
    InMemoryCursorStore,
)
from ocm.runtime.state import build_gap_registry


# ==============================================================================
# Constantes
# ==============================================================================

DEFAULT_MAX_CONCURRENCY: int = 6

# Tiempo de stagger entre arranque de workers consecutivos (segundos).
# Previene thundering herd: N workers × WORKER_STAGGER_S = ventana de dispersión.
# Ejemplo: 20 workers × 0.05s = 1s total de dispersión — imperceptible al usuario.
WORKER_STAGGER_S: float = 0.05

PipelineModeStr = Literal["incremental", "backfill", "repair"]


# ==============================================================================
# Helpers internos
# ==============================================================================

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


def _build_cursor_store_safe() -> CursorStorePort:
    """
    Construye CursorStore desde variables de entorno con fallback seguro.

    build_cursor_store_from_env() resuelve config desde variables de entorno,
    consistente con el resto del sistema (factories.py).
    Si Redis no está disponible, usa InMemoryCursorStore como fallback.
    """
    try:
        store = build_cursor_store_from_env()
        if store.is_healthy():
            return store  # type: ignore[return-value]
        _log.warning("Redis no disponible — cursor store en memoria (fallback)")
        return InMemoryCursorStore()  # type: ignore[return-value]
    except Exception as exc:
        _log.bind(error=str(exc)).warning("CursorStore init failed — fallback")
        return InMemoryCursorStore()  # type: ignore[return-value]


def _classify_pair_error(result: PairResult) -> str:
    """
    Clasifica el tipo de error de un PairResult para el throttle adaptivo.

    Prioridad de clasificación:
      1. error_type (nombre de excepción) — más preciso que string matching
      2. error (mensaje)                  — fallback para excepciones sin type

    Tipos de retorno y peso en AdaptiveThrottle:
      "rate_limit" → peso ×2.0  (429 — penalización máxima)
      "timeout"    → peso ×1.0  (penalización media)
      "network"    → peso ×0.5  (glitch transitorio — penalización mínima)

    DRY: SSOT de clasificación para OHLCVPipeline.
    Para TradesPipeline/DerivativesPipeline ver _update_throttle_from_summary
    en batch_tasks.py (opera sobre PipelineSummary post-run, no por-par).

    Ref: AdaptiveThrottle._RATE_LIMIT_WEIGHT / _TIMEOUT_WEIGHT / _NETWORK_WEIGHT
    """
    error_type = (result.error_type or "").lower()
    error_msg  = str(result.error   or "").lower()

    if "ratelimit" in error_type or "429" in error_msg or "rate limit" in error_msg:
        return "rate_limit"
    if "timeout" in error_type or "timeout" in error_msg:
        return "timeout"
    return "network"


# ==============================================================================
# OHLCVPipeline
# ==============================================================================

class OHLCVPipeline(PipelineTriggerPort):
    """
    Pipeline unificado de ingestion OHLCV.

    Throttle mid-run
    ----------------
    Si se inyecta un AdaptiveThrottle, el pipeline actualiza su estado
    después de CADA par ejecutado — no en batch al final del run.
    Esto cierra el feedback loop dentro del run, no solo entre runs.

    self.max_concurrency se mantiene sincronizado con throttle.current.
    El semáforo del run activo no cambia (asyncio.Semaphore es inmutable),
    pero la concurrencia actualizada se aplica en el siguiente run y es
    observable en logs y en get_throttle_state().

    Uso
    ---
    pipeline = OHLCVPipeline(
        symbols         = ["BTC/USDT"],
        timeframes      = ["1h", "4h", "1d"],
        start_date      = "2024-01-01",
        exchange_client = adapter,
        market_type     = "spot",
        throttle        = throttle,   # opcional — habilita mid-run feedback
    )

    summary = await pipeline.run(mode="incremental")
    summary = await pipeline.run(mode="backfill")
    summary = await pipeline.run(mode="repair")
    """

    _log: Any  # bound en __init__ via bind_pipeline() — anotado para MyPy

    def __init__(
        self,
        symbols:            List[str],
        timeframes:         List[str],
        start_date:         str,
        exchange_client:    "ExchangeClientPort",
        fetcher:            object,   # HistoricalFetcherPort — inyectar desde factory
        metrics:            object,   # PipelineMetricsPort  — inyectar desde factory
        max_concurrency:    int                        = DEFAULT_MAX_CONCURRENCY,
        cursor_store:       Optional[CursorStorePort]  = None,
        backfill_mode:      bool                       = True,
        market_type:        str                        = "spot",
        dry_run:            bool                       = False,
        auto_lookback_days: int                        = 3650,
        throttle:           "Optional[ThrottlePort]" = None,
    ) -> None:
        # Fail-fast: dependencias de infraestructura obligatorias.
        # OHLCVPipeline no puede importar infrastructure/ ni adapters/ (DIP · BC-05).
        # Todas las implementaciones concretas vienen de ConcretePipelineFactory.
        if not symbols:
            raise ValueError("symbols no puede estar vacio")
        if not timeframes:
            raise ValueError("timeframes no puede estar vacio")
        if not start_date:
            raise ValueError("start_date es obligatorio")
        if exchange_client is None:
            raise TypeError("OHLCVPipeline: 'exchange_client' es obligatorio")
        if fetcher is None:
            raise TypeError(
                "OHLCVPipeline: 'fetcher' es obligatorio. "
                "Inyectar HistoricalFetcherAsync desde el composition root."
            )
        if metrics is None:
            raise TypeError(
                "OHLCVPipeline: 'metrics' es obligatorio. "
                "Inyectar PrometheusPipelineMetrics desde el composition root."
            )

        self.symbols         = symbols
        self.timeframes      = timeframes
        self.start_date      = start_date
        self.max_concurrency = max_concurrency
        self.market_type     = market_type.lower()
        self.backfill_mode   = backfill_mode
        self._exchange_id    = getattr(exchange_client, "_exchange_id", "unknown")
        self._throttle       = throttle

        # Dependencias inyectadas — sin resolución de infraestructura aquí (DIP).
        cursor     = cursor_store or _build_cursor_store_safe()
        # Late import — DIP: application no acopla quality/ en module-level.
        # BC-05: QualityPipeline es infrastructure interna de quality layer.
        from market_data.quality.pipeline import QualityPipeline  # noqa: PLC0415
        quality    = QualityPipeline()
        _publisher = _build_kafka_publisher_safe()

        self._ctx = PipelineContext(
            fetcher      = fetcher,
            cursor       = cursor,
            quality      = quality,
            exchange_id  = self._exchange_id,
            market_type  = self.market_type,
            start_date   = start_date,
            publisher    = _publisher,
            metrics      = metrics,
            gap_registry = build_gap_registry(),  # type: ignore[arg-type]
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
            mode             = mode,
            market           = self.market_type,
            symbols          = len(self.symbols),
            timeframes       = len(self.timeframes),
            pairs            = total_pairs,
            concurrency      = self.max_concurrency,
            throttle_enabled = self._throttle is not None,
        ).info("OHLCVPipeline iniciando")

        pipeline_start = time.monotonic()
        results, degraded_exchanges = await self._run_worker_pool(
            strategy, pairs, pipeline_mode
        )
        duration_ms = int((time.monotonic() - pipeline_start) * 1000)

        summary = PipelineSummary(
            results            = results,
            duration_ms        = duration_ms,
            mode               = pipeline_mode,
            degraded_exchanges = degraded_exchanges,
        )
        summary.log(self._log)

        if summary.status == "degraded":
            self._log.bind(
                mode               = mode,
                degraded_exchanges = summary.degraded_exchanges,
                failed             = summary.failed,
                total              = summary.total,
                transient          = sum(1 for r in summary.results if r.is_transient_error),
            ).warning("Pipeline DEGRADED")
        elif summary.status == "failed":
            self._log.bind(
                mode      = mode,
                failed    = summary.failed,
                total     = summary.total,
                transient = sum(1 for r in summary.results if r.is_transient_error),
            ).error("Pipeline FAILED")
        else:
            self._log.bind(
                mode                    = mode,
                rows                    = summary.total_rows,
                pairs                   = summary.total,
                throughput_rows_per_sec = summary.throughput_rows_per_sec,
                duration_ms             = duration_ms,
            ).success("Pipeline OK")

        # Validación de completitud post-pipeline (non-blocking)
        await self._run_completeness_check(summary)

        return summary

    # ======================================================
    # Throttle mid-run (feedback loop por par)
    # ======================================================

    def _feed_throttle(self, result: PairResult) -> None:
        """
        Registra el resultado de un par individual en el throttle adaptivo.

        Llamado después de CADA pair execution — no en batch al final del run.
        Esto da al throttle señales en tiempo real: el throttle ve el patrón
        temporal de presión del exchange dentro del run, no solo el resumen
        agregado post-run.

        self.max_concurrency se sincroniza con throttle.current tras cada
        registro — los logs y get_throttle_state() reflejan el estado real.

        SafeOps
        -------
        - No opera si throttle no fue inyectado (None).
        - Nunca lanza excepción: throttle es observabilidad, no control crítico.
          Un fallo aquí no debe interrumpir la ingestion de datos.
        """
        if self._throttle is None:
            return
        try:
            if result.error:
                self._throttle.record_error(
                    error_type = _classify_pair_error(result),
                    latency_ms = result.duration_ms,
                )
            else:
                self._throttle.record_success(latency_ms=result.duration_ms)
            # Sincronizar max_concurrency con el estado actualizado del throttle.
            # El semáforo del run activo es fijo (asyncio.Semaphore es inmutable).
            # El valor actualizado se aplica en el próximo run y es visible en
            # logs del pipeline y en get_throttle_state().
            self.max_concurrency = self._throttle.current
        except Exception as exc:  # pragma: no cover
            self._log.bind(error=str(exc)).debug("_feed_throttle failed (non-critical)")

    # ======================================================
    # Worker pool (producer/consumer)
    # ======================================================

    async def _run_worker_pool(
        self,
        strategy: PipelineStrategy,
        pairs:    List[tuple[str, str]],
        mode:     PipelineMode,
    ) -> tuple[List[PairResult], List[str]]:
        """
        Delega al worker pool genérico con circuit breaker via on_abort.

        on_abort coordina el abort entre workers cuando _ExchangeAbortError
        se detecta: activa abort_event, drena la queue y registra el exchange
        degradado. El pool genérico gestiona el drenado y la cancelación.

        Nota — concurrencia dinámica
        ----------------------------
        max_concurrency se pasa como int fijo al semáforo interno del pool.
        asyncio.Semaphore no soporta ajuste dinámico del límite.
        _feed_throttle actualiza self.max_concurrency mid-run, pero ese valor
        se aplica en el próximo run, no en el semáforo activo.
        Para soporte de ajuste dinámico dentro del run, run_worker_pool
        necesitaría aceptar Callable[[], int] en lugar de int.

        Ref: Stevens, "Unix Network Programming" — thundering herd prevention
        """
        total:    int       = len(pairs)
        degraded: List[str] = []

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
    # Completeness check (post-pipeline, non-blocking)
    # ======================================================

    async def _run_completeness_check(self, summary: "PipelineSummary") -> None:
        """
        Hook post-pipeline para verificación de completitud.

        Kappa architecture: el producer (OHLCVPipeline) no lee el lago.
        Gap detection e invariant checks son responsabilidad del consumer
        downstream (RepairStrategy) o de un Dagster asset de observabilidad
        que lee Iceberg directamente.

        SafeOps: nunca bloquea ni lanza — solo registra el evento.
        """
        written = [r for r in summary.results if r.success and r.rows > 0]
        if not written:
            return
        self._log.bind(
            mode           = "completeness",
            series_checked = len(written),
        ).debug("Completeness check omitido — responsabilidad del consumer Kappa")

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

        Throttle mid-run
        ----------------
        Cada PairResult que retorna alimenta el throttle via _feed_throttle
        antes de retornar. Las excepciones _ExchangeAbortError y CancelledError
        NO alimentan el throttle — el exchange está degradado o el pipeline
        cancelado; registrar esos eventos distorsionaría la señal.

        Las strategies capturan sus propias excepciones internamente.
        Este wrapper captura cualquier escape inesperado (bug de infra,
        error en el semáforo) y lo convierte en PairResult con error,
        garantizando que el worker pool nunca pierda un resultado.
        """
        # DIP·BC-05: métricas via puerto, nunca via infraestructura directa.
        # fail-soft: si metrics es None el pipeline opera en modo degradado.
        if self._ctx.metrics is not None:
            self._ctx.metrics.active_pairs_inc(self._exchange_id)
        try:
            result = await strategy.execute_pair(
                symbol    = symbol,
                timeframe = timeframe,
                idx       = idx,
                total     = total,
                ctx       = self._ctx,
            )
            self._feed_throttle(result)
            return result

        except asyncio.CancelledError:
            raise  # nunca alimentar throttle — pipeline cancelado externamente


        except ExchangeCircuitOpenError as exc:
            # Si otro worker ya disparó el abort, salir sin duplicar cooldown.
            if abort_event.is_set():
                raise _ExchangeAbortError(self._exchange_id) from exc

            # Primer worker en detectar circuit open: cooldown coordinado.
            # Los demás workers detectarán abort_event activo y saltarán esto.
            # DIP: exc transporta el estado del breaker — sin consultar el adapter.
            _cooldown = max(1.0, exc.cooldown_remaining_ms / 1000)
            self._log.bind(
                symbol     = symbol,
                timeframe  = timeframe,
                failures   = exc.fail_counter,
                cooldown_s = round(_cooldown, 1),
            ).warning("Circuit open — sleeping cooldown")
            await asyncio.sleep(_cooldown)

            # Verificar si otro worker abortó durante el sleep.
            if abort_event.is_set():
                raise _ExchangeAbortError(self._exchange_id) from exc

            try:
                self._log.bind(
                    symbol=symbol, timeframe=timeframe
                ).info("Circuit cooldown retry")
                result = await strategy.execute_pair(
                    symbol    = symbol,
                    timeframe = timeframe,
                    idx       = idx,
                    total     = total,
                    ctx       = self._ctx,
                )
                self._feed_throttle(result)
                return result
            except ExchangeCircuitOpenError as _inner_exc:  # DIP·BC-05: tipo desde port
                # Breaker todavía abierto tras cooldown — abortar exchange, no pipeline.
                # fail-soft: métricas opcionales; abort siempre se propaga.
                if self._ctx.metrics is not None:
                    self._ctx.metrics.fetch_aborts_inc(self._exchange_id)
                self._log.bind(
                    symbol                = symbol,
                    timeframe             = timeframe,
                    failures              = _inner_exc.fail_counter,
                    cooldown_remaining_ms = _inner_exc.cooldown_remaining_ms,
                ).warning("Circuit open after retry — aborting exchange")
                raise _ExchangeAbortError(self._exchange_id) from exc

        except Exception as exc:
            self._log.bind(
                symbol     = symbol,
                timeframe  = timeframe,
                error_type = type(exc).__name__,
                error      = str(exc),
            ).error("execute_pair unhandled")
            result = PairResult(
                symbol      = symbol,
                timeframe   = timeframe,
                mode        = mode,
                exchange_id = self._exchange_id,
                error       = str(exc),
                error_type  = type(exc).__name__,
            )
            self._feed_throttle(result)
            return result
        finally:
            # SafeOps — dec garantizado en todo path de salida:
            # éxito, CancelledError, AbortError, o excepción inesperada.
            # Sin finally, cualquier raise deja el gauge incrementado para siempre.
            try:
                if self._ctx.metrics is not None:
                    self._ctx.metrics.active_pairs_dec(self._exchange_id)
            except Exception:
                pass  # métricas son best-effort
