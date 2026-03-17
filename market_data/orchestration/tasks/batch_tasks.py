"""
orchestration/tasks/batch_tasks.py
====================================
Prefect tasks que ejecutan pipelines batch del dominio.

Responsabilidad única: instanciar pipelines y delegar ejecución.

Principios: SOLID, DRY, KISS, SafeOps
"""
from __future__ import annotations

from typing import List, Sequence

from prefect import task, get_run_logger

from core.config.schema import AppConfig, ExchangeConfig, PIPELINE_TASK_TIMEOUT
from market_data.orchestration.tasks.exchange_tasks import ExchangeProbe
from market_data.batch.flows.historical_pipeline import HistoricalPipelineAsync


def _validate_historical_inputs(exchange_cfg: ExchangeConfig, config: AppConfig) -> None:
    if not exchange_cfg.all_symbols:
        raise ValueError(f"Exchange '{exchange_cfg.name.value}' has no symbols configured.")
    if not config.pipeline.historical.timeframes:
        raise ValueError("Historical pipeline requires at least one timeframe.")


def _validate_derivatives_datasets(datasets: Sequence[str]) -> None:
    if not datasets:
        raise ValueError("Derivatives pipeline requires at least one dataset.")


@task(
    name="historical_ohlcv_pipeline",
    retries=3,
    retry_delay_seconds=[30, 120, 300],
    timeout_seconds=PIPELINE_TASK_TIMEOUT,
    task_run_name="{exchange_cfg.name.value}-ohlcv",
    description="Ingests historical OHLCV data for a specific exchange.",
)
async def run_historical_pipeline(
    config:       AppConfig,
    exchange_cfg: ExchangeConfig,
    probe:        ExchangeProbe,
) -> None:
    log = get_run_logger()
    _validate_historical_inputs(exchange_cfg, config)

    hist          = config.pipeline.historical
    exchange_name = exchange_cfg.name.value
    max_concurrency = probe.max_concurrent

    log.info(
        "Historical pipeline starting | exchange=%s symbols=%s timeframes=%s workers=%s",
        exchange_name, len(exchange_cfg.all_symbols), len(hist.timeframes), max_concurrency,
    )

    from services.exchange.ccxt_adapter import CCXTAdapter

    exchange_client = CCXTAdapter(config=exchange_cfg)

    pipeline = HistoricalPipelineAsync(
        symbols        =exchange_cfg.all_symbols,
        timeframes     =hist.timeframes,
        start_date     =hist.start_date,
        max_concurrency=max_concurrency,
        exchange_client=exchange_client,
    )
    summary = await pipeline.run()

    log.info(
        "Historical pipeline finished | exchange=%s ok=%s failed=%s skipped=%s rows=%s",
        exchange_name, summary.succeeded, summary.failed, summary.skipped, summary.total_rows,
    )

    if summary.total > 0 and summary.failed == summary.total:
        raise RuntimeError(
            f"Historical pipeline failed for all {summary.total} pairs on '{exchange_name}'."
        )

    if summary.failed > 0:
        log.warning(
            "Historical pipeline partial failures | exchange=%s failed=%s/%s",
            exchange_name, summary.failed, summary.total,
        )


@task(
    name="trades_pipeline",
    retries=3,
    retry_delay_seconds=[30, 120, 300],
    timeout_seconds=PIPELINE_TASK_TIMEOUT,
    task_run_name="{exchange_cfg.name.value}-trades",
    description="[NOT IMPLEMENTED] Disable 'datasets.trades' in settings.yaml.",
)
async def run_trades_pipeline(
    config:       AppConfig,
    exchange_cfg: ExchangeConfig,
    probe:        ExchangeProbe,
) -> None:
    log = get_run_logger()
    log.error("Trades pipeline not implemented | exchange=%s", exchange_cfg.name.value)
    raise NotImplementedError("Disable 'datasets.trades' in settings.yaml.")


@task(
    name="derivatives_pipeline",
    retries=0,
    timeout_seconds=PIPELINE_TASK_TIMEOUT,
    task_run_name="{exchange_cfg.name.value}-derivatives",
    description="[NOT IMPLEMENTED] Disable derivative datasets in settings.yaml.",
)
async def run_derivatives_pipeline(
    config:       AppConfig,
    exchange_cfg: ExchangeConfig,
    probe:        ExchangeProbe,
    datasets:     List[str],
) -> None:
    log = get_run_logger()
    _validate_derivatives_datasets(datasets)
    log.error("Derivatives pipeline not implemented | exchange=%s datasets=%s",
              exchange_cfg.name.value, datasets)
    raise NotImplementedError(f"Disable {datasets} in settings.yaml.")
