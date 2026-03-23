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
from market_data.batch.pipelines.historical_pipeline import HistoricalPipelineAsync


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

    log.info(
        "Historical pipeline starting | exchange=%s market=spot symbols=%s timeframes=%s workers=%s",
        exchange_name,
        len(spot_symbols),
        len(hist_cfg.timeframes),
        max_concurrency,
    )

    # Adapter inyectado → el flow gestiona su ciclo de vida
    # Adapter interno  → esta task lo crea y lo cierra
    from services.exchange.ccxt_adapter import CCXTAdapter
    _owns_client = exchange_client is None
    if _owns_client:
        exchange_client = CCXTAdapter(config=exchange_cfg, default_type=None)

    try:
        pipeline = HistoricalPipelineAsync(
            symbols           = spot_symbols,
            timeframes        = hist_cfg.timeframes,
            start_date        = hist_cfg.start_date,
            max_concurrency   = max_concurrency,
            exchange_client   = exchange_client,
            backfill_mode = hist_cfg.backfill_mode,
            market_type       = "spot",
        )
        summary = await pipeline.run()
    finally:
        if _owns_client:
            await exchange_client.close()

    log.info(
        "Historical pipeline finished | exchange=%s ok=%s failed=%s skipped=%s rows=%s",
        exchange_name,
        summary.succeeded,
        summary.failed,
        summary.skipped,
        summary.total_rows,
    )

    if summary.total > 0 and summary.failed == summary.total:
        raise RuntimeError(
            f"Historical pipeline failed for all {summary.total} symbols on '{exchange_name}'."
        )

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
    config:       AppConfig,
    exchange_cfg: ExchangeConfig,
    probe:        ExchangeProbe,
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
    from services.exchange.ccxt_adapter import CCXTAdapter

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

    log.info(
        "Futures pipeline starting | exchange=%s market=%s symbols=%s timeframes=%s workers=%s",
        exchange_name, futures_market_type,
        len(futures_symbols), len(hist_cfg.timeframes), probe.max_concurrent,
    )

    futures_client = CCXTAdapter(config=exchange_cfg, default_type=futures_market_type)

    try:
        pipeline = HistoricalPipelineAsync(
            symbols           = futures_symbols,
            timeframes        = hist_cfg.timeframes,
            start_date        = hist_cfg.start_date,
            max_concurrency   = probe.max_concurrent,
            exchange_client   = futures_client,
            backfill_mode = hist_cfg.backfill_mode,
            market_type       = futures_market_type,
        )
        summary = await pipeline.run()
    finally:
        await futures_client.close()

    log.info(
        "Futures pipeline finished | exchange=%s market=%s ok=%s failed=%s skipped=%s rows=%s",
        exchange_name, futures_market_type,
        summary.succeeded, summary.failed, summary.skipped, summary.total_rows,
    )

    if summary.total > 0 and summary.failed == summary.total:
        raise RuntimeError(
            f"Futures pipeline failed for all {summary.total} symbols on '{exchange_name}'."
        )


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
    silenciosamente sin error. Esto evita fallos en producción por
    pipelines aún no implementados.

    Para activar: set 'datasets.trades: true' en settings.yaml
    e implementar la lógica de ingestión.
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
    silenciosamente sin error. Esto evita fallos en producción por
    pipelines aún no implementados.

    Para activar: habilitar al menos un dataset de derivados en settings.yaml
    e implementar la lógica de ingestión.
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
async def run_backfill_pipeline(
    config:          AppConfig,
    exchange_cfg:    ExchangeConfig,
    probe:           ExchangeProbe,
    market_type:     str = "spot",
    exchange_client=None,
) -> None:
    from market_data.batch.pipelines.unified_pipeline import UnifiedPipeline
    from services.exchange.ccxt_adapter import CCXTAdapter

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
            "Backfill skip — no symbols configured | exchange=%s market=%s",
            exchange_name, market_type,
        )
        return

    log.info(
        "Backfill pipeline starting | exchange=%s market=%s symbols=%s timeframes=%s",
        exchange_name, market_type, len(symbols), len(hist_cfg.timeframes),
    )

    _owns_client = exchange_client is None
    if _owns_client:
        exchange_client = CCXTAdapter(config=exchange_cfg, default_type=market_type)

    try:
        pipeline = UnifiedPipeline(
            symbols           = symbols,
            timeframes        = hist_cfg.timeframes,
            start_date        = hist_cfg.start_date,
            max_concurrency   = probe.max_concurrent,
            exchange_client   = exchange_client,
            market_type       = market_type,
            backfill_mode = True,  # backfill siempre descarga desde el origen
        )
        summary = await pipeline.run(mode="backfill")
    finally:
        if _owns_client:
            await exchange_client.close()

    log.info(
        "Backfill pipeline finished | exchange=%s market=%s ok=%s failed=%s rows=%s",
        exchange_name, market_type,
        summary.succeeded, summary.failed, summary.total_rows,
    )

    if summary.total > 0 and summary.failed == summary.total:
        raise RuntimeError(
            f"Backfill failed for all {summary.total} symbols on '{exchange_name}'."
        )


# ==========================================================
# Repair Pipeline Task
# ==========================================================

@task(
    name="repair_pipeline",
    retries=1,
    retry_delay_seconds=[120],
    timeout_seconds=PIPELINE_TASK_TIMEOUT * 2,
    task_run_name="{exchange_cfg.name.value}-repair",
    description="Detects and repairs temporal gaps in Silver OHLCV data.",
    tags=["ohlcv", "repair"],
)
async def run_repair_pipeline(
    config:          AppConfig,
    exchange_cfg:    ExchangeConfig,
    probe:           ExchangeProbe,
    market_type:     str = "spot",
    exchange_client=None,
) -> None:
    from market_data.batch.pipelines.unified_pipeline import UnifiedPipeline
    from services.exchange.ccxt_adapter import CCXTAdapter

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

    _owns_client = exchange_client is None
    if _owns_client:
        exchange_client = CCXTAdapter(config=exchange_cfg, default_type=market_type)

    try:
        pipeline = UnifiedPipeline(
            symbols           = symbols,
            timeframes        = hist_cfg.timeframes,
            start_date        = hist_cfg.start_date,
            max_concurrency   = probe.max_concurrent,
            exchange_client   = exchange_client,
            market_type       = market_type,
            backfill_mode = hist_cfg.backfill_mode,
        )
        summary = await pipeline.run(mode="repair")
    finally:
        if _owns_client:
            await exchange_client.close()

    log.info(
        "Repair pipeline finished | exchange=%s market=%s ok=%s failed=%s "
        "gaps_found=%s gaps_healed=%s rows=%s",
        exchange_name, market_type,
        summary.succeeded, summary.failed,
        summary.total_gaps_found, summary.total_gaps_healed,
        summary.total_rows,
    )

    if summary.total > 0 and summary.failed == summary.total:
        raise RuntimeError(
            f"Repair failed for all {summary.total} symbols on '{exchange_name}'."
        )
