# market_data/orchestration/flows/batch_flow.py
# ============================================================
"""
Flujo principal de ingestión de datos de mercado (OrangeCashMachine)

Responsabilidad:
- Cargar y validar configuración
- Validar exchanges
- Lanzar pipelines (historical, trades, derivatives)
- Consolidar resultados

Principios aplicados:
- SOLID (SRP, OCP)
- KISS
- DRY
- SafeOps (errores parciales no detienen todo)
"""
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import List, Set, Tuple

from prefect import flow, get_run_logger

from core.config.schema import AppConfig, CONFIG_PATH
from market_data.orchestration.tasks.config_tasks import load_and_validate_config_task
from market_data.orchestration.tasks.exchange_tasks import ExchangeProbe, validate_exchange_connection
from market_data.orchestration.tasks.batch_tasks import (
    run_historical_pipeline,
    run_trades_pipeline,
    run_derivatives_pipeline,
)

# ==========================================================
# Dataset registry
# ==========================================================

_CORE_DATASETS = ("ohlcv", "trades", "orderbook")
_DERIVATIVE_DATASETS = ("funding_rate", "open_interest", "liquidations", "mark_price", "index_price")
_ALL_DATASETS = (*_CORE_DATASETS, *_DERIVATIVE_DATASETS)

# ==========================================================
# Helpers
# ==========================================================

def filter_active_datasets(requested: Set[str], probe: ExchangeProbe) -> Tuple[Set[str], Set[str]]:
    """Filtra datasets activos según soporte del exchange. Devuelve (activos, omitidos)"""
    supported = set(probe.supported_datasets)
    active = requested & supported
    skipped = requested - supported
    return active, skipped

async def gather_task_results(futures: list) -> None:
    """
    Consolida resultados de pipelines ejecutadas en paralelo.

    SafeOps:
      - Todas fallan → error fatal
      - Algunas fallan → warning
    """
    log = get_run_logger()
    results = await asyncio.gather(*futures, return_exceptions=True)
    failures = [r for r in results if isinstance(r, BaseException)]
    successes = len(results) - len(failures)

    for exc in failures:
        log.error("Pipeline task failed | error=%s", exc)

    if failures and successes == 0:
        raise RuntimeError(f"All {len(failures)} pipeline(s) failed.")

    if failures:
        log.warning("Flow completed with partial failures | ok=%s failed=%s", successes, len(failures))

async def validate_exchanges_parallel(config: AppConfig) -> List[ExchangeProbe]:
    """Valida exchanges en paralelo y retorna probes válidos"""
    log = get_run_logger()
    futures = [validate_exchange_connection(cfg) for cfg in config.exchanges]
    results = await asyncio.gather(*futures, return_exceptions=True)

    probes: List[ExchangeProbe] = []
    for cfg, result in zip(config.exchanges, results):
        if isinstance(result, Exception):
            log.error("Exchange validation failed | exchange=%s error=%s", cfg.name.value, result)
        else:
            probes.append(result)

    if not probes:
        raise RuntimeError("All exchange validations failed.")
    log.info("Exchanges validated | ok=%s/%s", len(probes), len(config.exchanges))
    return probes

def launch_pipelines_for_exchange(config: AppConfig, probe: ExchangeProbe, requested: Set[str]) -> list:
    """Lanza pipelines según datasets activos de un exchange validado"""
    log = get_run_logger()
    exc_cfg = config.get_exchange(probe.exchange)
    if exc_cfg is None:
        log.warning("Exchange not found in config | exchange=%s", probe.exchange)
        return []

    active, skipped = filter_active_datasets(requested, probe)
    if skipped:
        log.warning("Datasets skipped (unsupported) | exchange=%s skipped=%s", probe.exchange, sorted(skipped))

    log.info(
        "Launching pipelines | exchange=%s datasets=%s max_concurrent=%s rate_limit_ms=%s",
        probe.exchange,
        sorted(active),
        probe.max_concurrent,
        probe.rate_limit_ms,
    )

    futures: list = []

    # Spot pipelines
    if getattr(probe, "supports_spot", True):
        if "ohlcv" in active:
            futures.append(run_historical_pipeline.submit(config, exc_cfg, probe))
        if "trades" in active:
            futures.append(run_trades_pipeline.submit(config, exc_cfg, probe))
    elif set(_CORE_DATASETS) & active:
        log.warning("Spot datasets requested but exchange has no spot support | exchange=%s", probe.exchange)

    # Derivative pipelines
    if getattr(probe, "supports_futures", True):
        derivative_active = [d for d in _DERIVATIVE_DATASETS if d in active]
        if derivative_active:
            futures.append(run_derivatives_pipeline.submit(config, exc_cfg, probe, derivative_active))
    elif set(_DERIVATIVE_DATASETS) & active:
        log.warning("Derivative datasets requested but exchange has no futures support | exchange=%s", probe.exchange)

    return futures

# ==========================================================
# Prefect Flow
# ==========================================================

@flow(
    name="market_data_ingestion",
    log_prints=True,
    retries=0,  # reintentos globales deshabilitados; se usan retries individuales en tasks
)
async def market_data_flow(config_path: Path = CONFIG_PATH) -> None:
    """
    Flow principal de ingestión de datos de mercado

    Flujo:
      1. Carga de configuración
      2. Validación de exchanges
      3. Lanzamiento de pipelines según datasets activos
      4. Consolidación de resultados
    """
    log = get_run_logger()

    # 1. Cargar configuración
    config: AppConfig = await load_and_validate_config_task(config_path)

    if not config.datasets.any_active:
        log.warning("No datasets active. Flow exiting.")
        return

    requested: Set[str] = {ds for ds in _ALL_DATASETS if getattr(config.datasets, ds, False)}
    log.info("Flow starting | exchanges=%s datasets=%s", config.exchange_names, sorted(requested))

    # 2. Validar exchanges
    probes = await validate_exchanges_parallel(config)

    # 3. Lanzar pipelines
    all_futures: list = []
    for probe in probes:
        all_futures.extend(launch_pipelines_for_exchange(config, probe, requested))

    if not all_futures:
        log.warning("No pipelines launched.")
        return

    # 4. Consolidar resultados
    await gather_task_results(all_futures)
    log.info("Market data flow completed successfully")