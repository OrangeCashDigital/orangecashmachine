"""
orchestration/tasks/batch_tasks.py
==================================

Prefect tasks que ejecutan pipelines batch del dominio.

Responsabilidad
---------------
Instanciar pipelines y delegar ejecución.

Principios
----------
SOLID
    SRP – cada task ejecuta un pipeline específico

DRY
    Configuración extraída una sola vez

KISS
    Sin abstracciones innecesarias

SafeOps
    validación temprana
    logs estructurados
    fallos explícitos
"""

from __future__ import annotations

from typing import List, Sequence

from prefect import task, get_run_logger

from core.config.schema import AppConfig, PIPELINE_TASK_TIMEOUT
from market_data.batch.flows.historical_pipeline import HistoricalPipelineAsync


# ==========================================================
# Helpers
# ==========================================================

def _validate_historical_config(config: AppConfig) -> None:
    """
    Valida configuración mínima para pipeline histórico.
    """

    hist = config.pipeline.historical

    if not hist.symbols:
        raise ValueError("Historical pipeline requires at least one symbol")

    if not hist.timeframes:
        raise ValueError("Historical pipeline requires at least one timeframe")


def _validate_derivatives_datasets(datasets: Sequence[str]) -> None:
    """
    Valida lista de datasets solicitados.
    """

    if not datasets:
        raise ValueError(
            "Derivatives pipeline requires at least one dataset"
        )


# ==========================================================
# Task: Historical OHLCV
# ==========================================================

@task(
    name="historical_ohlcv_pipeline",
    retries=3,
    retry_delay_seconds=[30, 120, 300],
    timeout_seconds=PIPELINE_TASK_TIMEOUT,
    description="Ingests historical OHLCV data for configured symbols.",
)
async def run_historical_pipeline(config: AppConfig) -> None:
    """
    Ejecuta el pipeline histórico OHLCV.

    Idempotencia
    ------------
    HistoricalStorage deduplica por timestamp.

    Política de fallo
    -----------------
    Solo falla si TODOS los pares fallan.
    """

    log = get_run_logger()

    _validate_historical_config(config)

    hist = config.pipeline.historical

    log.info(
        "Starting historical pipeline | symbols={} timeframes={} workers={}",
        len(hist.symbols),
        len(hist.timeframes),
        config.max_concurrent,
    )

    pipeline = HistoricalPipelineAsync(
        symbols=hist.symbols,
        timeframes=hist.timeframes,
        start_date=hist.start_date,
        max_workers=config.max_concurrent,
    )

    summary = await pipeline.run()

    log.info(
        "Historical pipeline finished | ok={} failed={} skipped={} rows={}",
        summary.succeeded,
        summary.failed,
        summary.skipped,
        summary.total_rows,
    )

    if summary.total > 0 and summary.failed == summary.total:

        raise RuntimeError(
            f"Historical pipeline failed for all {summary.total} pairs. "
            "Possible causes: exchange downtime, credentials error, "
            "or network failure."
        )

    if summary.failed > 0:

        log.warning(
            "Historical pipeline partial failures | failed={}/{}",
            summary.failed,
            summary.total,
        )


# ==========================================================
# Task: Trades (NOT IMPLEMENTED)
# ==========================================================

@task(
    name="trades_pipeline",
    retries=3,
    retry_delay_seconds=[30, 120, 300],
    timeout_seconds=PIPELINE_TASK_TIMEOUT,
    description="[NOT IMPLEMENTED] Trade-level ingestion pipeline.",
)
async def run_trades_pipeline(config: AppConfig) -> None:
    """
    Pipeline de ingestión de trades.

    Estado
    ------
    PENDIENTE DE IMPLEMENTACIÓN.
    """

    log = get_run_logger()

    log.error(
        "Trades pipeline not implemented. Disable 'datasets.trades' in settings.yaml."
    )

    raise NotImplementedError(
        "TradesPipelineAsync is not implemented. "
        "Disable 'datasets.trades' in settings.yaml."
    )


# ==========================================================
# Task: Derivatives (NOT IMPLEMENTED)
# ==========================================================

@task(
    name="derivatives_pipeline",
    retries=0,
    timeout_seconds=PIPELINE_TASK_TIMEOUT,
    description="[NOT IMPLEMENTED] Derivatives data ingestion pipeline.",
)
async def run_derivatives_pipeline(
    config: AppConfig,
    datasets: List[str],
) -> None:
    """
    Pipeline de ingestión de datos derivados.

    Datasets posibles
    -----------------
    funding_rate
    open_interest
    liquidations
    mark_price
    index_price
    """

    log = get_run_logger()

    _validate_derivatives_datasets(datasets)

    log.error(
        "Derivatives pipeline not implemented | datasets={}",
        datasets,
    )

    raise NotImplementedError(
        f"DerivativesPipelineAsync not implemented. "
        f"Requested datasets: {datasets}. "
        "Disable these datasets in settings.yaml."
    )