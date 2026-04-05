from __future__ import annotations

"""
market_data/orchestration/tasks/batch_tasks.py
===============================================

Prefect tasks de ejecución de pipelines de datos de mercado.

Responsabilidad
---------------
Cada task encapsula un pipeline específico (historical, trades, derivatives)
y gestiona su ciclo de vida: validación, ejecución, logging y errores.

Principios
----------
SOLID  – SRP: cada task tiene una sola responsabilidad
KISS   – sin lógica innecesaria, flujo lineal y predecible
DRY    – validaciones centralizadas, sin repetición
SafeOps – placeholders no fallan en producción si están desactivados por config
"""

from typing import List, Sequence

from prefect import task, get_run_logger

from core.config.schema import AppConfig, ExchangeConfig, PIPELINE_TASK_TIMEOUT
from market_data.orchestration.tasks.exchange_tasks import ExchangeProbe
from market_data.processing.pipelines.unified_pipeline import UnifiedPipeline
from market_data.adapters.exchange.ccxt_adapter import CCXTAdapter, get_or_create_throttle, get_throttle_state

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

    Si no hay adapter inyectado, crea uno, lo conecta y garantiza
    su cierre en finally — el lifecycle es responsabilidad de esta task.

    DRY: elimina el patrón _owns_client duplicado en 3 tasks.
    SafeOps: cierre garantizado en finally cuando _owns_client=True.
    """
    if injected is not None:
        yield injected
        return

    # Fallback: adapter creado sin ExchangeProbe previo.
    # En flows de produccion, el adapter SIEMPRE debe venir de
    # validate_exchange_connection. Este path es solo para resilencia
    # interna de tasks (tests, reparacion manual, retry isolation).
    import logging as _logging
    _logging.getLogger(__name__).warning(
        "managed_adapter fallback | exchange=%s market_type=%s — "        "adapter created without prior ExchangeProbe validation",
        exchange_cfg.name.value, market_type,
    )
    adapter = CCXTAdapter(config=exchange_cfg, default_type=market_type)
    try:
        yield adapter
    finally:
        await adapter.close()



# ==========================================================
# Validaciones independientes y testeables (SafeOps)
# ==========================================================

def _validate_historical_inputs(exchange_cfg: ExchangeConfig, config: AppConfig) -> None:
    """Valida que haya símbolos y timeframes antes de ejecutar el pipeline histórico."""
    if not exchange_cfg.markets.spot_symbols:
        raise ValueError(f"Exchange '{exchange_cfg.name.value}' has no spot symbols configured.")
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
    """Actualiza el throttle adaptivo según resultados del pipeline."""
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
    """Emite métricas por par/timeframe en formato estructurado."""
    for r in sorted(summary.results, key=lambda x: (x.symbol, x.timeframe)):
        status = "✓" if r.success else ("↷" if r.skipped else "✗")
        log.info(
            "  %s %s/%s/%s | rows=%s duration=%sms error=%s",
            status, market_type, r.symbol, r.timeframe,
            r.rows, r.duration_ms, r.error or "-",
        )


def _raise_if_total_failure(summary, pipeline_name: str, exchange_name: str) -> None:
    """Lanza RuntimeError si el 100% de los símbolos fallaron."""
    if summary.total > 0 and summary.failed == summary.total:
        raise RuntimeError(
            f"{pipeline_name} failed for all {summary.total} symbols on '{exchange_name}'."
        )


# ==========================================================
# Historical Pipeline
# ==========================================================

@task(
    name="historical_ohlcv_pipeline",
    retries=3,
    retry_delay_seconds=[30, 120, 300],
    timeout_seconds=PIPELINE_TASK_TIMEOUT,
    task_run_name="{exchange_cfg.name.value}-ohlcv",
    description="Ingests historical OHLCV data for a specific exchange.",
    tags=["ohlcv", "historical"],
)
async def run_historical_pipeline(
    config: AppConfig,
    exchange_cfg: ExchangeConfig,
    probe: ExchangeProbe,
    exchange_client: "CCXTAdapter | None" = None,
) -> None:
    """
    Ejecuta el pipeline histórico (OHLCV) para un exchange.

    El adapter puede ser inyectado desde el flow (ya conectado, load_markets
    ya ejecutado) o creado internamente como fallback. En el segundo caso,
    esta task es responsable de cerrarlo.

    SafeOps
    -------
    - Errores parciales loggeados — solo falla si el 100% de symbols fallan
    - Adapter externo nunca se cierra aquí — lifecycle del flow
    - Adapter interno siempre se cierra en finally
    """
    log = get_run_logger()
    _validate_historical_inputs(exchange_cfg, config)

    hist_cfg        = config.pipeline.historical
    exchange_name   = exchange_cfg.name.value
    max_concurrency = probe.max_concurrent
    spot_symbols    = exchange_cfg.markets.spot_symbols

    # Throttle adaptivo: usa probe.max_concurrent como initial/maximum.
    # El throttle ajusta concurrencia en tiempo real según error rate.
    throttle = get_or_create_throttle(
        exchange_id       = exchange_name,
        market_type       = "spot",
        dataset           = "ohlcv",
        initial           = max_concurrency,
        maximum           = max_concurrency,
        latency_target_ms = probe.latency_ms or 500,
    )
    effective_concurrency = throttle.current

    log.info(
        "Historical pipeline starting | exchange=%s market=spot symbols=%s timeframes=%s workers=%s",
        exchange_name,
        len(spot_symbols),
        len(hist_cfg.timeframes),
        effective_concurrency,
    )

    async with managed_adapter(exchange_cfg, "spot", injected=exchange_client) as client:
        pipeline = UnifiedPipeline(
            symbols         = spot_symbols,
            timeframes      = hist_cfg.timeframes,
            start_date      = hist_cfg.start_date,
            max_concurrency = effective_concurrency,
            exchange_client = client,
            backfill_mode   = hist_cfg.backfill_mode,
            market_type     = "spot",
        )
        summary = await pipeline.run(mode="incremental")

    _update_throttle_from_summary(throttle, summary)

    ts = get_throttle_state(exchange_name, market_type="spot", dataset="ohlcv")
    log.info(
        "Historical pipeline finished | exchange=%s ok=%s failed=%s skipped=%s rows=%s",
        exchange_name, summary.succeeded, summary.failed, summary.skipped, summary.total_rows,
    )

    log.info("── Pipeline metrics | exchange=%s ──────────────────────────", exchange_name)
    _log_pipeline_metrics(summary, "spot", log)
    log.info(
        "── Throttle state | key=%s concurrent=%s/%s error_rate=%.0f%% p95=%sms ──",
        ts["key"], ts["concurrent"], ts["maximum"], ts["error_rate"] * 100, int(ts["p95_ms"]),
    )

    _raise_if_total_failure(summary, "Historical pipeline", exchange_name)

    if summary.failed > 0:
        log.warning(
            "Historical pipeline partial failures | exchange=%s failed=%s/%s",
            exchange_name, summary.failed, summary.total,
        )


# ==========================================================
# Futures Pipeline
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
    exchange_client: 'CCXTAdapter | None' = None,
) -> None:
    """
    Ejecuta el pipeline histórico OHLCV para futuros/perpetuos.

    Crea su propio CCXTAdapter con defaultType configurado.
    Storage silver separado por market_type (swap).
    Cursores independientes del spot.

    SafeOps
    -------
    - Skip silencioso si futures no está habilitado en config
    - Solo falla si el 100% de símbolos fallan
    - Adapter siempre cerrado en finally
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

    # Throttle compartido con spot del mismo exchange — si spot ya redujo
    # concurrencia por errores, futures hereda ese estado de presión.
    throttle = get_or_create_throttle(
        exchange_id       = exchange_name,
        market_type       = futures_market_type,
        dataset           = "ohlcv",
        initial           = probe.max_concurrent,
        maximum           = probe.max_concurrent,
        latency_target_ms = probe.latency_ms or 500,
    )
    effective_concurrency = throttle.current

    log.info(
        "Futures pipeline starting | exchange=%s market=%s symbols=%s timeframes=%s workers=%s",
        exchange_name, futures_market_type,
        len(futures_symbols), len(hist_cfg.timeframes), effective_concurrency,
    )

    async def _run_pipeline(client) -> None:
        nonlocal summary
        pipeline = UnifiedPipeline(
            symbols         = futures_symbols,
            timeframes      = hist_cfg.timeframes,
            start_date      = hist_cfg.start_date,
            max_concurrency = effective_concurrency,
            exchange_client = client,
            backfill_mode   = hist_cfg.backfill_mode,
            market_type     = futures_market_type,
        )
        pipeline_mode = "backfill" if hist_cfg.backfill_mode else "incremental"
        summary = await pipeline.run(mode=pipeline_mode)

    summary = None  # will be set inside _run_pipeline
    # Invariante de aislamiento: nunca reutilizar un adapter inyectado
    # si está cerrado o en un loop diferente (retry de Prefect).
    # En ese caso, crear uno fresco con owned lifecycle.
    _use_injected = (
        exchange_client is not None
        and not exchange_client._closed
        and await exchange_client.is_healthy()
    )
    if _use_injected:
        await _run_pipeline(exchange_client)
    else:
        if exchange_client is not None:
            log.warning(
                "Futures adapter unhealthy or closed — creating fresh adapter | exchange=%s",
                exchange_name,
            )
        async with managed_adapter(exchange_cfg, futures_market_type) as futures_client:
            await _run_pipeline(futures_client)

    _update_throttle_from_summary(throttle, summary)

    ts = get_throttle_state(exchange_name, market_type=futures_market_type, dataset="ohlcv")
    log.info(
        "Futures pipeline finished | exchange=%s market=%s ok=%s failed=%s skipped=%s rows=%s",
        exchange_name, futures_market_type,
        summary.succeeded, summary.failed, summary.skipped, summary.total_rows,
    )

    log.info("── Pipeline metrics | exchange=%s market=%s ──────────────────", exchange_name, futures_market_type)
    _log_pipeline_metrics(summary, futures_market_type, log)
    log.info(
        "── Throttle state | key=%s concurrent=%s/%s error_rate=%.0f%% p95=%sms ──",
        ts["key"], ts["concurrent"], ts["maximum"], ts["error_rate"] * 100, int(ts["p95_ms"]),
    )

    _raise_if_total_failure(summary, "Futures pipeline", exchange_name)


# ==========================================================
# Trades Pipeline
# ==========================================================

@task(
    name="trades_pipeline",
    retries=2,
    retry_delay_seconds=[30, 120],
    timeout_seconds=PIPELINE_TASK_TIMEOUT,
    task_run_name="{exchange_cfg.name.value}-trades",
    description="Trades pipeline — skipped silently if disabled in config.",
    tags=["trades"],
)
async def run_trades_pipeline(
    config: AppConfig,
    exchange_cfg: ExchangeConfig,
    probe: ExchangeProbe,
    dataset: str = "trades",
) -> None:
    """
    Pipeline de trades.

    SafeOps: si datasets.trades está desactivado en config, retorna
    silenciosamente sin error.
    """
    log = get_run_logger()

    if not config.datasets.trades:
        log.warning(
            "Trades pipeline skipped — disabled in config | exchange=%s dataset=%s",
            exchange_cfg.name.value, dataset,
        )
        return

    # TODO: implementar ingestión de trades
    log.error(
        "Trades pipeline not implemented | exchange=%s dataset=%s",
        exchange_cfg.name.value, dataset,
    )
    raise NotImplementedError(
        f"Trades pipeline not implemented for '{exchange_cfg.name.value}'. "
        "Set 'datasets.trades: false' in settings.yaml to suppress this error."
    )


# ==========================================================
# Derivatives Pipeline
# ==========================================================

@task(
    name="derivatives_pipeline",
    retries=1,
    retry_delay_seconds=[60],
    timeout_seconds=PIPELINE_TASK_TIMEOUT,
    task_run_name="{exchange_cfg.name.value}-derivatives",
    description="Derivatives pipeline — skipped silently if disabled in config.",
    tags=["derivatives"],
)
async def run_derivatives_pipeline(
    config: AppConfig,
    exchange_cfg: ExchangeConfig,
    probe: ExchangeProbe,
    datasets: List[str],
) -> None:
    """
    Pipeline de derivados (funding_rate, open_interest, liquidations, etc.).

    SafeOps: si ningún dataset de derivados está activo en config, retorna
    silenciosamente sin error.
    """
    log = get_run_logger()
    _validate_derivatives_datasets(datasets)

    active_derivatives = config.datasets.active_derivative_datasets
    if not active_derivatives:
        log.warning(
            "Derivatives pipeline skipped — no derivative datasets active in config | exchange=%s",
            exchange_cfg.name.value,
        )
        return

    # TODO: implementar ingestión de derivados
    log.error(
        "Derivatives pipeline not implemented | exchange=%s datasets=%s",
        exchange_cfg.name.value, datasets,
    )
    raise NotImplementedError(
        f"Derivatives pipeline not implemented for '{exchange_cfg.name.value}'. "
        f"Disable {datasets} in settings.yaml to suppress this error."
    )


# ==========================================================
# Backfill Pipeline Task
# ==========================================================

@task(
    name="backfill_pipeline",
    retries=2,
    retry_delay_seconds=[60, 300],
    timeout_seconds=PIPELINE_TASK_TIMEOUT * 4,
    task_run_name="{exchange_cfg.name.value}-backfill",
    description="Backfills complete historical OHLCV data to exchange origin.",
    tags=["ohlcv", "backfill"],
)
async def run_repair_pipeline(
    config:          AppConfig,
    exchange_cfg:    ExchangeConfig,
    probe:           ExchangeProbe,
    market_type:     str = "spot",
    exchange_client: "CCXTAdapter | None" = None,
) -> None:
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
            exchange_name, market_type,
        )
        return

    log.info(
        "Repair pipeline starting | exchange=%s market=%s symbols=%s timeframes=%s",
        exchange_name, market_type, len(symbols), len(hist_cfg.timeframes),
    )

    async with managed_adapter(exchange_cfg, market_type, injected=exchange_client) as client:
        pipeline = UnifiedPipeline(
            symbols         = symbols,
            timeframes      = hist_cfg.timeframes,
            start_date      = hist_cfg.start_date,
            max_concurrency = probe.max_concurrent,
            exchange_client = client,
            market_type     = market_type,
            backfill_mode   = hist_cfg.backfill_mode,
        )
        summary = await pipeline.run(mode="repair")

    log.info(
        "Repair pipeline finished | exchange=%s market=%s ok=%s failed=%s "
        "gaps_found=%s gaps_healed=%s rows=%s",
        exchange_name, market_type,
        summary.succeeded, summary.failed,
        summary.total_gaps_found, summary.total_gaps_healed,
        summary.total_rows,
    )

    _raise_if_total_failure(summary, "Repair", exchange_name)
