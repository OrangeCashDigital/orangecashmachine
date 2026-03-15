"""
orchestration/flows/batch_flow.py
==================================

Responsabilidad única
---------------------
Definir el flow principal de ingestión batch y el helper
de agregación de resultados.

No contiene lógica de negocio ni accede directamente
a exchanges o storage.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from prefect import flow, get_run_logger

from market_data.orchestration.config import AppConfig, CONFIG_PATH
from market_data.orchestration.tasks.config_tasks import load_and_validate_config
from market_data.orchestration.tasks.exchange_tasks import validate_exchange_connection
from market_data.orchestration.tasks.batch_tasks import (
    run_historical_pipeline,
    run_trades_pipeline,
    run_derivatives_pipeline,
)


# ==========================================================
# Result Aggregator
# ==========================================================

async def _collect_task_results(futures: list) -> None:
    """
    Espera todas las tasks y agrega resultados.

    return_exceptions=True garantiza que el fallo de un pipeline
    no cancele los demás (SafeOps: fallos parciales aislados).

    Relanza solo si el 100% de las tasks fallaron.
    """
    log = get_run_logger()

    outcomes = await asyncio.gather(*futures, return_exceptions=True)
    failures  = [r for r in outcomes if isinstance(r, BaseException)]
    successes = len(outcomes) - len(failures)

    for exc in failures:
        log.error("Pipeline task failed | error=%s", exc)

    if failures and successes == 0:
        raise RuntimeError(
            f"All {len(failures)} pipeline(s) failed. "
            "Review individual task logs for details."
        )

    if failures:
        log.warning(
            "Flow completed with partial failures | ok=%s failed=%s",
            successes, len(failures),
        )


# ==========================================================
# Batch Flow
# ==========================================================

@flow(
    name="market_data_ingestion",
    log_prints=True,
    retries=1,
    retry_delay_seconds=60,
)
async def market_data_flow(config_path: Path = CONFIG_PATH) -> None:
    """
    Orquestador principal de ingestión de market data.

    Secuencia
    ---------
    1. Cargar y validar config     – falla rápido si el YAML es inválido
    2. Validar exchange            – falla rápido si no hay conectividad
    3. Lanzar pipelines en paralelo con fallos parciales aislados

    Parameters
    ----------
    config_path : Path
        Ruta al settings.yaml. Sobreescribible para testing
        o deployments en distintos entornos sin tocar código.
    """
    log = get_run_logger()

    config: AppConfig = load_and_validate_config(config_path)
    await validate_exchange_connection(config)

    if not config.datasets.any_active:
        log.warning("No pipelines enabled in configuration. Flow exiting.")
        return

    log.info(
        "Launching pipelines | ohlcv=%s trades=%s derivatives=%s",
        config.datasets.ohlcv,
        config.datasets.trades,
        bool(config.datasets.active_derivative_datasets),
    )

    futures = []

    if config.datasets.ohlcv:
        futures.append(run_historical_pipeline.submit(config))

    if config.datasets.trades:
        futures.append(run_trades_pipeline.submit(config))

    derivative_datasets = config.datasets.active_derivative_datasets
    if derivative_datasets:
        futures.append(run_derivatives_pipeline.submit(config, derivative_datasets))

    await _collect_task_results(futures)

    log.info("Market data flow completed successfully")
