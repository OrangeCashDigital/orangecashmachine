"""
orchestration/flows/batch_flow.py
====================================
Orquestador principal de ingestión batch multi-exchange.

Principios: SOLID, DRY, KISS, SafeOps
"""
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import List

from prefect import flow, get_run_logger

from core.config.schema import AppConfig, CONFIG_PATH
from market_data.orchestration.tasks.config_tasks import load_and_validate_config
from market_data.orchestration.tasks.exchange_tasks import (
    ExchangeProbe,
    validate_exchange_connection,
)
from market_data.orchestration.tasks.batch_tasks import (
    run_historical_pipeline,
    run_trades_pipeline,
    run_derivatives_pipeline,
)


async def _collect_task_results(futures: list) -> None:
    log = get_run_logger()
    outcomes  = await asyncio.gather(*futures, return_exceptions=True)
    failures  = [r for r in outcomes if isinstance(r, BaseException)]
    successes = len(outcomes) - len(failures)

    for exc in failures:
        log.error("Pipeline task failed | error=%s", exc)

    if failures and successes == 0:
        raise RuntimeError(f"All {len(failures)} pipeline(s) failed.")

    if failures:
        log.warning("Flow completed with partial failures | ok=%s failed=%s", successes, len(failures))


def _launch_exchange_pipelines(config: AppConfig, probe: ExchangeProbe) -> list:
    log = get_run_logger()

    exc_cfg = config.get_exchange(probe.exchange)
    if exc_cfg is None:
        log.warning("Exchange not found in config | exchange=%s", probe.exchange)
        return []

    requested = set(d for d in ("ohlcv","trades","orderbook","funding_rate","open_interest","liquidations","mark_price","index_price") if getattr(config.datasets, d, False))
    supported = set(probe.supported_datasets)
    active    = requested & supported
    skipped   = requested - supported

    if skipped:
        log.warning("Datasets skipped (unsupported) | exchange=%s skipped=%s",
                    probe.exchange, sorted(skipped))

    log.info("Launching pipelines | exchange=%s datasets=%s max_concurrent=%s",
             probe.exchange, sorted(active), probe.max_concurrent)

    futures = []

    if "ohlcv" in active:
        futures.append(run_historical_pipeline.submit(config, exc_cfg, probe))

    if "trades" in active:
        futures.append(run_trades_pipeline.submit(config, exc_cfg, probe))

    derivative_datasets = [
        d for d in config.datasets.active_derivative_datasets if d in active
    ]
    if derivative_datasets:
        futures.append(run_derivatives_pipeline.submit(config, exc_cfg, probe, derivative_datasets))

    return futures


@flow(
    name="market_data_ingestion",
    log_prints=True,
    retries=1,
    retry_delay_seconds=60,
)
async def market_data_flow(config_path: Path = CONFIG_PATH) -> None:
    log = get_run_logger()

    # 1. Config
    config: AppConfig = load_and_validate_config(config_path)

    if not config.datasets.any_active:
        log.warning("No datasets active. Flow exiting.")
        return

    log.info("Flow starting | exchanges=%s datasets=%s",
             config.exchange_names, [d for d in ("ohlcv","trades","orderbook","funding_rate","open_interest","liquidations","mark_price","index_price") if getattr(config.datasets, d, False)])

    # 2. Validar exchanges en paralelo
    probes: List[ExchangeProbe] = []
    for exc_cfg in config.enabled_exchanges:
        try:
            probe = await validate_exchange_connection.fn(exc_cfg)
            probes.append(probe)
        except Exception as exc:
            import traceback
            log.error("Exchange validation failed | exchange=%s error=%s\n%s",
                      exc_cfg.name.value, exc, traceback.format_exc())

    if not probes:
        raise RuntimeError("All exchange validations failed.")

    log.info("Exchanges validated | ok=%s/%s", len(probes), len(config.enabled_exchanges))

    # 3. Lanzar pipelines por exchange
    all_futures = []
    for probe in probes:
        all_futures.extend(_launch_exchange_pipelines(config, probe))

    if not all_futures:
        log.warning("No pipelines launched.")
        return

    # 4. Agregar resultados
    await _collect_task_results(all_futures)

    log.info("Market data flow completed successfully")
