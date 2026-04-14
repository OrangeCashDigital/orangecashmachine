from __future__ import annotations

"""
market_data/orchestration/tasks/batch_tasks.py
===============================================

Prefect tasks de ejecución de pipelines de datos de mercado.

Responsabilidad
---------------
Cada task encapsula un pipeline de dominio específico y gestiona
su ciclo de vida: validación, ejecución, logging y errores.

Principios
----------
SOLID  – SRP: cada task tiene una única responsabilidad
KISS   – sin lógica innecesaria, flujo lineal y predecible
DRY    – validaciones centralizadas, sin repetición
SafeOps – tasks no fallan silenciosamente; skip explícito si desactivado

Referencias
-----------
Prefect task docs: https://docs.prefect.io/latest/develop/write-tasks
CCXT unified API:  https://docs.ccxt.com/#/?id=unified-api
"""

from collections.abc import Sequence

from prefect import task, get_run_logger

from core.config.schema import AppConfig, ExchangeConfig, PIPELINE_TASK_TIMEOUT
from market_data.orchestration.tasks.exchange_tasks import ExchangeProbe
from market_data.processing.pipelines.ohlcv_pipeline import OHLCVPipeline
from market_data.adapters.exchange.ccxt_adapter import (
    CCXTAdapter,
    get_or_create_throttle,
    get_throttle_state,
)

# ==========================================================
# Lifecycle helper
# ==========================================================

from contextlib import asynccontextmanager
from typing import AsyncIterator


@asynccontextmanager
async def managed_adapter(
    exchange_cfg,
    market_type: str,
    injected: "CCXTAdapter | None" = None,
) -> AsyncIterator[CCXTAdapter]:
    """
    Context manager para lifecycle de CCXTAdapter en tasks.

    Si el caller inyecta un adapter (ya conectado), lo usa directamente
    y NO lo cierra — el lifecycle es responsabilidad del flow.

    Si no hay adapter inyectado, crea uno y garantiza su cierre en finally.
    Este path es solo para resiliencia interna (tests, repair manual, retry
    isolation) — en producción el adapter siempre viene de ExchangeProbe.

    DRY   : un único punto de gestión de lifecycle — sin patrón _owns_client.
    SafeOps: cierre garantizado en finally cuando el adapter es propio.

    Ref: https://docs.prefect.io/latest/develop/write-tasks#task-retries
    """
    if injected is not None:
        yield injected
        return

    from loguru import logger as _loguru
    _loguru.warning(
        "managed_adapter fallback | exchange={} market_type={} — "
        "adapter created without prior ExchangeProbe validation",
        exchange_cfg.name.value,
        market_type,
    )
    adapter = CCXTAdapter(config=exchange_cfg, default_type=market_type)
    try:
        yield adapter
    finally:
        await adapter.close()


# ==========================================================
# Validaciones independientes y testeables (SafeOps)
# ==========================================================

def _validate_historical_inputs(
    exchange_cfg: ExchangeConfig,
    config: AppConfig,
) -> None:
    """Valida que haya símbolos y timeframes antes de ejecutar el pipeline OHLCV."""
    if not exchange_cfg.markets.spot_symbols:
        raise ValueError(
            f"Exchange '{exchange_cfg.name.value}' has no spot symbols configured."
        )
    if not config.pipeline.historical.timeframes:
        raise ValueError("Historical pipeline requires at least one timeframe.")


def _validate_derivatives_datasets(datasets: Sequence[str]) -> None:
    """Valida que se hayan especificado datasets antes de ejecutar derivados."""
    if not datasets:
        raise ValueError("Derivatives pipeline requires at least one dataset.")


# ==========================================================
# Internal helpers (DRY)
# ==========================================================

def _update_throttle_from_summary(throttle, summary) -> None:
    """
    Actualiza el throttle adaptivo según resultados del pipeline.

    Clasifica errores por tipo para ajustar la penalización de concurrencia:
      rate_limit → reducción agresiva (429 del exchange)
      timeout    → reducción moderada
      network    → reducción conservadora

    Ref: https://docs.ccxt.com/#/?id=error-handling
    """
    for r in summary.results:
        if r.error:
            error_str = str(r.error).lower()
            if "429" in error_str or "rate limit" in error_str:
                etype = "rate_limit"
            elif "timeout" in error_str:
                etype = "timeout"
            else:
                etype = "network"
            throttle.record_error(error_type=etype, latency_ms=r.duration_ms)
        else:
            throttle.record_success(latency_ms=r.duration_ms)


def _log_pipeline_metrics(summary, market_type: str, log) -> None:
    """
    Emite métricas por par/timeframe en formato estructurado.

    Símbolos de estado:
      ✓ → éxito
      ↷ → skipped (datos ya al día)
      ✗ → error
    """
    for r in sorted(summary.results, key=lambda x: (x.symbol, x.timeframe)):
        status = "✓" if r.success else ("↷" if r.skipped else "✗")
        log.info(
            "  %s %s/%s/%s | rows=%s duration=%sms error=%s",
            status,
            market_type,
            r.symbol,
            r.timeframe,
            r.rows,
            r.duration_ms,
            r.error or "-",
        )


def _raise_if_total_failure(
    summary,
    pipeline_name: str,
    exchange_name: str,
) -> None:
    """
    Lanza RuntimeError si el 100% de los símbolos fallaron.

    Fallos parciales son tolerados — el flow sigue con los pares restantes.
    Solo el fallo total es fatal para la task.

    Ref: Kumar (2025) — fail-soft pattern para pipelines distribuidos.
    """
    if summary.total > 0 and summary.failed == summary.total:
        raise RuntimeError(
            f"{pipeline_name} failed for all {summary.total} symbols"
            f" on '{exchange_name}'."
        )


# ==========================================================
# OHLCV Spot / Historical Pipeline
# ==========================================================

@task(
    name="historical_ohlcv_pipeline",
    retries=3,
    retry_delay_seconds=[30, 120, 300],
    timeout_seconds=PIPELINE_TASK_TIMEOUT,
    task_run_name="{exchange_cfg.name.value}-ohlcv",
    description="Ingests historical OHLCV spot data for a specific exchange.",
    tags=["ohlcv", "historical"],
)
async def run_historical_pipeline(
    config:          AppConfig,
    exchange_cfg:    ExchangeConfig,
    probe:           ExchangeProbe,
    exchange_client: "CCXTAdapter | None" = None,
) -> None:
    """
    Ejecuta el pipeline histórico OHLCV (spot) para un exchange.

    El adapter puede ser inyectado desde el flow (ya conectado, con
    load_markets ejecutado) o creado internamente como fallback.

    Dominio: OHLCV candles agregados por timeframe.
    No procesa trades ni derivados — ver TradesPipeline y DerivativesPipeline.

    SafeOps
    -------
    - Errores parciales loggeados — falla solo si el 100% de symbols fallan
    - Adapter externo nunca se cierra aquí — lifecycle del flow
    - Adapter interno siempre se cierra en finally via managed_adapter

    Ref: https://docs.prefect.io/latest/develop/write-tasks#task-retries
    """
    log = get_run_logger()
    _validate_historical_inputs(exchange_cfg, config)

    hist_cfg          = config.pipeline.historical
    exchange_name     = exchange_cfg.name.value
    spot_symbols      = exchange_cfg.markets.spot_symbols

    throttle = get_or_create_throttle(
        exchange_id       = exchange_name,
        market_type       = "spot",
        dataset           = "ohlcv",
        initial           = probe.max_concurrent,
        maximum           = probe.max_concurrent,
        latency_target_ms = probe.latency_ms or 500,
    )

    log.info(
        "Historical pipeline starting | exchange=%s market=spot"
        " symbols=%s timeframes=%s workers=%s",
        exchange_name,
        len(spot_symbols),
        len(hist_cfg.timeframes),
        throttle.current,
    )

    async with managed_adapter(exchange_cfg, "spot", injected=exchange_client) as client:
        pipeline = OHLCVPipeline(
            symbols            = spot_symbols,
            timeframes         = hist_cfg.timeframes,
            start_date         = hist_cfg.start_date,
            max_concurrency    = throttle.current,
            exchange_client    = client,
            backfill_mode      = hist_cfg.backfill_mode,
            market_type        = "spot",
            dry_run            = config.safety.dry_run,
            auto_lookback_days = getattr(hist_cfg, "auto_lookback_days", 1825),
        )
        pipeline_mode = "backfill" if hist_cfg.backfill_mode else "incremental"
        summary       = await pipeline.run(mode=pipeline_mode)

    _update_throttle_from_summary(throttle, summary)

    ts = get_throttle_state(exchange_name, market_type="spot", dataset="ohlcv")
    log.info(
        "Historical pipeline finished | exchange=%s ok=%s failed=%s skipped=%s rows=%s",
        exchange_name,
        summary.succeeded,
        summary.failed,
        summary.skipped,
        summary.total_rows,
    )
    log.info("── Pipeline metrics | exchange=%s ──────────────────────────", exchange_name)
    _log_pipeline_metrics(summary, "spot", log)
    log.info(
        "── Throttle state | key=%s concurrent=%s/%s error_rate=%.0f%% p95=%sms ──",
        ts["key"],
        ts["concurrent"],
        ts["maximum"],
        ts["error_rate"] * 100,
        int(ts["p95_ms"]),
    )

    _raise_if_total_failure(summary, "Historical pipeline", exchange_name)

    if summary.failed > 0:
        log.warning(
            "Historical pipeline partial failures | exchange=%s failed=%s/%s",
            exchange_name,
            summary.failed,
            summary.total,
        )


# ==========================================================
# OHLCV Futures Pipeline
# ==========================================================

@task(
    name="historical_futures_pipeline",
    retries=3,
    retry_delay_seconds=[30, 120, 300],
    timeout_seconds=PIPELINE_TASK_TIMEOUT,
    task_run_name="{exchange_cfg.name.value}-futures",
    description="Ingests historical OHLCV futures/perpetuals data.",
    tags=["ohlcv", "historical", "futures"],
)
async def run_futures_pipeline(
    config:          AppConfig,
    exchange_cfg:    ExchangeConfig,
    probe:           ExchangeProbe,
    exchange_client: "CCXTAdapter | None" = None,
) -> None:
    """
    Ejecuta el pipeline histórico OHLCV para futuros/perpetuos.

    Dominio: OHLCV candles de mercados swap/future — independiente de spot.
    Storage silver separado por market_type. Cursores independientes del spot.

    Independencia de dominio
    ------------------------
    Este pipeline NO depende del resultado de run_historical_pipeline.
    Si la API spot falla, futures puede ejecutar sin restricción.
    La dependencia es de recursos (rate limit del exchange), no de datos.

    SafeOps
    -------
    - Skip silencioso si futures no está habilitado en config
    - Solo falla si el 100% de símbolos fallan
    - Adapter siempre cerrado en finally via managed_adapter

    Ref: Popov (2025) — downstream stages independientes no deben bloquearse
         por fallos de upstream si no hay dependencia semántica de datos.
    """
    log = get_run_logger()

    if not exchange_cfg.has_futures:
        log.warning(
            "Futures pipeline skipped — futures not enabled | exchange=%s",
            exchange_cfg.name.value,
        )
        return

    futures_symbols     = exchange_cfg.markets.futures_symbols
    futures_market_type = exchange_cfg.markets.futures_default_type or "swap"
    hist_cfg            = config.pipeline.historical
    exchange_name       = exchange_cfg.name.value

    throttle = get_or_create_throttle(
        exchange_id       = exchange_name,
        market_type       = futures_market_type,
        dataset           = "ohlcv",
        initial           = probe.max_concurrent,
        maximum           = probe.max_concurrent,
        latency_target_ms = probe.latency_ms or 500,
    )

    log.info(
        "Futures pipeline starting | exchange=%s market=%s"
        " symbols=%s timeframes=%s workers=%s",
        exchange_name,
        futures_market_type,
        len(futures_symbols),
        len(hist_cfg.timeframes),
        throttle.current,
    )

    pipeline_mode = "backfill" if hist_cfg.backfill_mode else "incremental"
    async with managed_adapter(
        exchange_cfg, futures_market_type, injected=exchange_client
    ) as futures_client:
        pipeline = OHLCVPipeline(
            symbols         = futures_symbols,
            timeframes      = hist_cfg.timeframes,
            start_date      = hist_cfg.start_date,
            max_concurrency = throttle.current,
            exchange_client = futures_client,
            backfill_mode   = hist_cfg.backfill_mode,
            market_type     = futures_market_type,
            dry_run         = config.safety.dry_run,
        )
        summary = await pipeline.run(mode=pipeline_mode)

    _update_throttle_from_summary(throttle, summary)

    ts = get_throttle_state(
        exchange_name, market_type=futures_market_type, dataset="ohlcv"
    )
    log.info(
        "Futures pipeline finished | exchange=%s market=%s"
        " ok=%s failed=%s skipped=%s rows=%s",
        exchange_name,
        futures_market_type,
        summary.succeeded,
        summary.failed,
        summary.skipped,
        summary.total_rows,
    )
    log.info(
        "── Pipeline metrics | exchange=%s market=%s ──────────────────",
        exchange_name,
        futures_market_type,
    )
    _log_pipeline_metrics(summary, futures_market_type, log)
    log.info(
        "── Throttle state | key=%s concurrent=%s/%s error_rate=%.0f%% p95=%sms ──",
        ts["key"],
        ts["concurrent"],
        ts["maximum"],
        ts["error_rate"] * 100,
        int(ts["p95_ms"]),
    )

    _raise_if_total_failure(summary, "Futures pipeline", exchange_name)

    if summary.failed > 0:
        log.warning(
            "Futures pipeline partial failures | exchange=%s failed=%s/%s",
            exchange_name,
            summary.failed,
            summary.total,
        )


# ==========================================================
# Trades Pipeline (stub — SafeOps skip si desactivado)
# ==========================================================

@task(
    name="trades_pipeline",
    retries=2,
    retry_delay_seconds=[30, 120],
    timeout_seconds=PIPELINE_TASK_TIMEOUT,
    task_run_name="{exchange_cfg.name.value}-trades",
    description="Ingests tick-level trade data for a specific exchange.",
    tags=["trades"],
)
async def run_trades_pipeline(
    config:          AppConfig,
    exchange_cfg:    ExchangeConfig,
    probe:           ExchangeProbe,
    exchange_client: "CCXTAdapter | None" = None,
) -> None:
    """
    Pipeline de trades (tick data).

    Dominio distinto a OHLCV: sin timeframe, volumen masivo, append-only.
    Usa TradesPipeline — no comparte lógica con OHLCVPipeline.

    SafeOps
    -------
    - Skip silencioso si datasets.trades desactivado en config
    - Solo falla si el 100% de símbolos fallan
    - Adapter siempre cerrado en finally via managed_adapter

    Ref: https://docs.prefect.io/latest/develop/write-tasks\#task-retries
    """
    from market_data.processing.pipelines.trades_pipeline import TradesPipeline

    log = get_run_logger()
    exchange_name = exchange_cfg.name.value

    if not config.datasets.trades:
        log.warning(
            "Trades pipeline skipped — disabled in config | exchange=%s",
            exchange_name,
        )
        return

    spot_symbols = exchange_cfg.markets.spot_symbols
    if not spot_symbols:
        log.warning(
            "Trades pipeline skipped — no spot symbols configured | exchange=%s",
            exchange_name,
        )
        return

    throttle = get_or_create_throttle(
        exchange_id       = exchange_name,
        market_type       = "spot",
        dataset           = "trades",
        initial           = probe.max_concurrent,
        maximum           = probe.max_concurrent,
        latency_target_ms = probe.latency_ms or 500,
    )

    log.info(
        "Trades pipeline starting | exchange=%s symbols=%s workers=%s",
        exchange_name,
        len(spot_symbols),
        throttle.current,
    )

    async with managed_adapter(exchange_cfg, "spot", injected=exchange_client) as client:
        pipeline = TradesPipeline(
            symbols          = spot_symbols,
            exchange_client  = client,
            market_type      = "spot",
            dry_run          = config.safety.dry_run,
            max_concurrency  = throttle.current,
        )
        pipeline_mode = "backfill" if config.pipeline.historical.backfill_mode else "incremental"
        summary       = await pipeline.run(mode=pipeline_mode)

    log.info(
        "Trades pipeline finished | exchange=%s ok=%s failed=%s skipped=%s rows=%s",
        exchange_name,
        summary.succeeded,
        summary.failed,
        summary.skipped,
        summary.total_rows,
    )

    if summary.total > 0 and summary.failed == summary.total:
        raise RuntimeError(
            f"Trades pipeline failed for all {summary.total} symbols"
            f" on '{exchange_name}'."
        )

    if summary.failed > 0:
        log.warning(
            "Trades pipeline partial failures | exchange=%s failed=%s/%s",
            exchange_name,
            summary.failed,
            summary.total,
        )


# ==========================================================
# Derivatives Pipeline (stub — SafeOps skip si desactivado)
# ==========================================================

@task(
    name="derivatives_pipeline",
    retries=1,
    retry_delay_seconds=[60],
    timeout_seconds=PIPELINE_TASK_TIMEOUT,
    task_run_name="{exchange_cfg.name.value}-derivatives",
    description="Ingests derivative market metrics (funding_rate, open_interest).",
    tags=["derivatives"],
)
async def run_derivatives_pipeline(
    config:          AppConfig,
    exchange_cfg:    ExchangeConfig,
    probe:           ExchangeProbe,
    datasets:        list[str],
    exchange_client: "CCXTAdapter | None" = None,
) -> None:
    """
    Pipeline de derivados (funding_rate, open_interest).

    Dominio distinto a OHLCV: schema variable por métrica, sin timeframe fijo.
    Usa DerivativesPipeline — no comparte lógica con OHLCVPipeline.

    SafeOps
    -------
    - Skip silencioso si ningún dataset activo en config
    - Valida datasets antes de construir el pipeline (fail-fast)
    - Solo falla si el 100% de pares (dataset × símbolo) fallan
    - Adapter siempre cerrado en finally via managed_adapter

    Ref: https://docs.prefect.io/latest/develop/write-tasks\#task-retries
    """
    from market_data.processing.pipelines.derivatives_pipeline import DerivativesPipeline

    log = get_run_logger()
    _validate_derivatives_datasets(datasets)

    exchange_name      = exchange_cfg.name.value
    active_derivatives = [d for d in datasets if d in (config.datasets.active_derivative_datasets or [])]

    if not active_derivatives:
        log.warning(
            "Derivatives pipeline skipped — no active derivative datasets"
            " | exchange=%s requested=%s",
            exchange_name,
            datasets,
        )
        return

    futures_symbols     = exchange_cfg.markets.futures_symbols
    futures_market_type = exchange_cfg.markets.futures_default_type or "swap"

    if not futures_symbols:
        log.warning(
            "Derivatives pipeline skipped — no futures symbols configured"
            " | exchange=%s",
            exchange_name,
        )
        return

    throttle = get_or_create_throttle(
        exchange_id       = exchange_name,
        market_type       = futures_market_type,
        dataset           = "derivatives",
        initial           = probe.max_concurrent,
        maximum           = probe.max_concurrent,
        latency_target_ms = probe.latency_ms or 500,
    )

    log.info(
        "Derivatives pipeline starting | exchange=%s market=%s"
        " datasets=%s symbols=%s workers=%s",
        exchange_name,
        futures_market_type,
        active_derivatives,
        len(futures_symbols),
        throttle.current,
    )

    async with managed_adapter(
        exchange_cfg, futures_market_type, injected=exchange_client
    ) as client:
        pipeline = DerivativesPipeline(
            symbols          = futures_symbols,
            datasets         = active_derivatives,
            exchange_client  = client,
            market_type      = futures_market_type,
            dry_run          = config.safety.dry_run,
            max_concurrency  = throttle.current,
        )
        pipeline_mode = "backfill" if config.pipeline.historical.backfill_mode else "incremental"
        summary       = await pipeline.run(mode=pipeline_mode)

    log.info(
        "Derivatives pipeline finished | exchange=%s ok=%s failed=%s"
        " skipped=%s rows=%s",
        exchange_name,
        summary.succeeded,
        summary.failed,
        summary.skipped,
        summary.total_rows,
    )

    if summary.total > 0 and summary.failed == summary.total:
        raise RuntimeError(
            f"Derivatives pipeline failed for all {summary.total} pairs"
            f" on '{exchange_name}'."
        )

    if summary.failed > 0:
        log.warning(
            "Derivatives pipeline partial failures | exchange=%s failed=%s/%s",
            exchange_name,
            summary.failed,
            summary.total,
        )


# ==========================================================
# Repair Pipeline (OHLCV gap healing)
# ==========================================================

@task(
    name="repair_pipeline",
    retries=2,
    retry_delay_seconds=[60, 300],
    timeout_seconds=PIPELINE_TASK_TIMEOUT * 4,
    task_run_name="{exchange_cfg.name.value}-repair",
    description="Repairs OHLCV gaps for a specific exchange and market type.",
    tags=["ohlcv", "repair"],
)
async def run_repair_pipeline(
    config:          AppConfig,
    exchange_cfg:    ExchangeConfig,
    probe:           ExchangeProbe,
    market_type:     str               = "spot",
    exchange_client: "CCXTAdapter | None" = None,
) -> None:
    """
    Detecta y rellena gaps en series OHLCV existentes.

    Dependencia semántica
    ---------------------
    Repair opera sobre datos ya escritos por run_historical_pipeline.
    Debe ejecutar DESPUÉS de que haya al menos partial success en spot.
    Si spot falló completamente, no hay datos que reparar — skip seguro.

    Esta dependencia es semántica (necesita datos existentes), no de
    disponibilidad de API. Es la única dependencia real en el grafo.

    Ref: Etikyala (2023) — "dependencies satisfied, not previous stage complete"

    SafeOps
    -------
    - Skip si no hay símbolos configurados para el market_type
    - Solo falla si el 100% de símbolos fallan
    - Adapter siempre cerrado en finally via managed_adapter
    """
    log           = get_run_logger()
    exchange_name = exchange_cfg.name.value
    hist_cfg      = config.pipeline.historical

    symbols = (
        exchange_cfg.markets.spot_symbols
        if market_type == "spot"
        else exchange_cfg.markets.futures_symbols
    )

    if not symbols:
        log.warning(
            "Repair skip — no symbols configured | exchange=%s market=%s",
            exchange_name,
            market_type,
        )
        return

    if not hist_cfg.timeframes:
        raise ValueError("Repair pipeline requires at least one timeframe.")

    throttle = get_or_create_throttle(
        exchange_id       = exchange_name,
        market_type       = market_type,
        dataset           = "ohlcv",
        initial           = probe.max_concurrent,
        maximum           = probe.max_concurrent,
        latency_target_ms = probe.latency_ms or 500,
    )

    log.info(
        "Repair pipeline starting | exchange=%s market=%s"
        " symbols=%s timeframes=%s workers=%s",
        exchange_name,
        market_type,
        len(symbols),
        len(hist_cfg.timeframes),
        throttle.current,
    )

    async with managed_adapter(exchange_cfg, market_type, injected=exchange_client) as client:
        pipeline = OHLCVPipeline(
            symbols            = symbols,
            timeframes         = hist_cfg.timeframes,
            start_date         = hist_cfg.start_date,
            max_concurrency    = throttle.current,
            exchange_client    = client,
            market_type        = market_type,
            backfill_mode      = hist_cfg.backfill_mode,
            dry_run            = config.safety.dry_run,
            auto_lookback_days = getattr(hist_cfg, "auto_lookback_days", 1825),
        )
        summary = await pipeline.run(mode="repair")

    _update_throttle_from_summary(throttle, summary)

    ts = get_throttle_state(exchange_name, market_type=market_type, dataset="ohlcv")
    log.info(
        "Repair pipeline finished | exchange=%s market=%s"
        " ok=%s failed=%s gaps_found=%s gaps_healed=%s rows=%s",
        exchange_name,
        market_type,
        summary.succeeded,
        summary.failed,
        summary.total_gaps_found,
        summary.total_gaps_healed,
        summary.total_rows,
    )
    log.info(
        "── Pipeline metrics | exchange=%s market=%s ──────────────────",
        exchange_name,
        market_type,
    )
    _log_pipeline_metrics(summary, market_type, log)
    log.info(
        "── Throttle state | key=%s concurrent=%s/%s error_rate=%.0f%% p95=%sms ──",
        ts["key"],
        ts["concurrent"],
        ts["maximum"],
        ts["error_rate"] * 100,
        int(ts["p95_ms"]),
    )

    _raise_if_total_failure(summary, "Repair", exchange_name)
