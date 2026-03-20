from __future__ import annotations
from typing import List, Sequence

from prefect import task, get_run_logger

from core.config.schema import AppConfig, ExchangeConfig, PIPELINE_TASK_TIMEOUT
from market_data.orchestration.tasks.exchange_tasks import ExchangeProbe
from market_data.batch.pipelines.historical_pipeline import HistoricalPipelineAsync

# -----------------------------------------------------------------------------
# Validaciones independientes y testeables (SafeOps)
# -----------------------------------------------------------------------------

def _validate_historical_inputs(exchange_cfg: ExchangeConfig, config: AppConfig) -> None:
    """Valida que haya símbolos y timeframes antes de ejecutar historical pipeline."""
    if not exchange_cfg.all_symbols:
        raise ValueError(f"Exchange '{exchange_cfg.name.value}' has no symbols configured.")
    if not config.pipeline.historical.timeframes:
        raise ValueError("Historical pipeline requires at least one timeframe.")


def _validate_derivatives_datasets(datasets: Sequence[str]) -> None:
    """Valida que se hayan seleccionado datasets para derivados."""
    if not datasets:
        raise ValueError("Derivatives pipeline requires at least one dataset.")


# -----------------------------------------------------------------------------
# Historical Pipeline
# -----------------------------------------------------------------------------

@task(
    name="historical_ohlcv_pipeline",
    retries=3,
    retry_delay_seconds=[30, 120, 300],
    timeout_seconds=PIPELINE_TASK_TIMEOUT,
    task_run_name="{exchange_cfg.name.value}-ohlcv",
    description="Ingests historical OHLCV data for a specific exchange.",
    tags=["ohlcv"],
)
async def run_historical_pipeline(
    config: AppConfig,
    exchange_cfg: ExchangeConfig,
    probe: ExchangeProbe,
    exchange_client: "CCXTAdapter | None" = None,
) -> None:
    """
    Ejecuta el pipeline histórico (OHLCV) de manera asíncrona para un exchange.
    SafeOps: errores parciales loggeados, raise solo si todos fallan.
    """
    log = get_run_logger()
    _validate_historical_inputs(exchange_cfg, config)

    hist_cfg = config.pipeline.historical
    exchange_name = exchange_cfg.name.value
    max_concurrency = probe.max_concurrent

    log.info(
        "Historical pipeline starting | exchange=%s symbols=%s timeframes=%s workers=%s",
        exchange_name, len(exchange_cfg.all_symbols), len(hist_cfg.timeframes), max_concurrency,
    )

    # Usar adapter inyectado (ya conectado, load_markets() ya hecho)
    # o crear uno nuevo como fallback para compatibilidad
    from services.exchange.ccxt_adapter import CCXTAdapter
    _owns_client = exchange_client is None
    if _owns_client:
        exchange_client = CCXTAdapter(config=exchange_cfg)

    try:
        pipeline = HistoricalPipelineAsync(
            symbols=exchange_cfg.all_symbols,
            timeframes=hist_cfg.timeframes,
            start_date=hist_cfg.start_date,
            max_concurrency=max_concurrency,
            exchange_client=exchange_client,
        )
        summary = await pipeline.run()
    finally:
        if _owns_client:
            await exchange_client.close()

    log.info(
        "Historical pipeline finished | exchange=%s ok=%s failed=%s skipped=%s rows=%s",
        exchange_name, summary.succeeded, summary.failed, summary.skipped, summary.total_rows,
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


# -----------------------------------------------------------------------------
# Trades Pipeline (SafeOps Placeholder)
# -----------------------------------------------------------------------------

@task(
    name="trades_pipeline",
    retries=3,
    retry_delay_seconds=[30, 120, 300],
    timeout_seconds=PIPELINE_TASK_TIMEOUT,
    task_run_name="{exchange_cfg.name.value}-trades",
    description="[NOT IMPLEMENTED] Trades dataset currently disabled.",
)
async def run_trades_pipeline(
    config: AppConfig,
    exchange_cfg: ExchangeConfig,
    probe: ExchangeProbe,
) -> None:
    """
    Placeholder para pipeline de trades. SafeOps: solo raise NotImplementedError.
    """
    log = get_run_logger()
    log.error("Trades pipeline not implemented | exchange=%s", exchange_cfg.name.value)
    raise NotImplementedError("Disable 'datasets.trades' in settings.yaml.")


# -----------------------------------------------------------------------------
# Derivatives Pipeline (SafeOps Placeholder)
# -----------------------------------------------------------------------------

@task(
    name="derivatives_pipeline",
    retries=0,
    timeout_seconds=PIPELINE_TASK_TIMEOUT,
    task_run_name="{exchange_cfg.name.value}-derivatives",
    description="[NOT IMPLEMENTED] Derivative datasets currently disabled.",
)
async def run_derivatives_pipeline(
    config: AppConfig,
    exchange_cfg: ExchangeConfig,
    probe: ExchangeProbe,
    datasets: List[str],
) -> None:
    """
    Placeholder para pipeline de derivados. SafeOps: valida datasets y raise NotImplementedError.
    """
    log = get_run_logger()
    _validate_derivatives_datasets(datasets)
    log.error(
        "Derivatives pipeline not implemented | exchange=%s datasets=%s",
        exchange_cfg.name.value, datasets,
    )
    raise NotImplementedError(f"Disable {datasets} in settings.yaml.")