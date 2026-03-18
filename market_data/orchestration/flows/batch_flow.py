"""
market_data/orchestration/flows/batch_flow.py
=============================================

Flow principal de ingestión de datos de mercado — OrangeCashMachine.

Responsabilidad
---------------
Orquestación pura: cargar config, validar exchanges, lanzar pipelines
y consolidar resultados. Sin lógica de negocio.

Separación de capas (principio clave)
--------------------------------------
• Prefect resuelve futures y maneja concurrencia/scheduling.
• El dominio (pipelines, fetchers, probes) recibe SIEMPRE valores
  reales — nunca PrefectFuture. Esto garantiza testeabilidad,
  portabilidad y desacoplamiento total de Prefect.

Principios
----------
SOLID  – SRP: este módulo solo orquesta
KISS   – flujo lineal en 4 pasos, helpers pequeños
DRY    – lógica de datasets y pipelines centralizada
SafeOps – fallos parciales loggeados, nunca colapso total
"""
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import List, Set, Tuple

from prefect import flow, get_run_logger

from core.config.schema import AppConfig, CONFIG_PATH
from core.config.loader import load_and_validate_config_task
from market_data.orchestration.tasks.exchange_tasks import (
    ExchangeProbe,
    validate_exchange_connection,
)
from market_data.orchestration.tasks.batch_tasks import (
    run_historical_pipeline,
    run_trades_pipeline,
    run_derivatives_pipeline,
)


# ==========================================================
# Dataset registry
# ==========================================================

_CORE_DATASETS: Tuple[str, ...] = ("ohlcv", "trades", "orderbook")
_DERIVATIVE_DATASETS: Tuple[str, ...] = (
    "funding_rate", "open_interest", "liquidations",
    "mark_price",   "index_price",
)
_ALL_DATASETS: Tuple[str, ...] = (*_CORE_DATASETS, *_DERIVATIVE_DATASETS)

# Datasets que implican mercado spot
_SPOT_DATASETS: frozenset[str] = frozenset(_CORE_DATASETS)

# Datasets que implican mercado de derivados
_DERIV_DATASETS: frozenset[str] = frozenset(_DERIVATIVE_DATASETS)


# ==========================================================
# Helpers de dominio — sin Prefect, testeables de forma aislada
# ==========================================================

def _filter_active_datasets(
    requested: Set[str],
    probe:     ExchangeProbe,
) -> Tuple[Set[str], Set[str]]:
    """
    Intersecta datasets solicitados con los soportados por el exchange.

    Returns
    -------
    active  : datasets a procesar
    skipped : datasets solicitados pero no disponibles
    """
    supported = set(probe.supported_datasets)
    return requested & supported, requested - supported


def _supports_spot(probe: ExchangeProbe) -> bool:
    """True si el exchange tiene mercado spot activo."""
    return "spot" in probe.available_markets


def _supports_futures(probe: ExchangeProbe) -> bool:
    """True si el exchange tiene mercados de derivados activos."""
    return bool({"swap", "future"} & set(probe.available_markets))


def _launch_pipelines_for_exchange(
    config:    AppConfig,
    probe:     ExchangeProbe,
    requested: Set[str],
    log,
) -> list:
    """
    Lanza tasks Prefect según los datasets activos del exchange.

    Recibe un ExchangeProbe resuelto — nunca un PrefectFuture.
    Retorna lista de PrefectFutures de las tasks lanzadas.

    Lógica de mercado
    -----------------
    • Spot (ohlcv, trades)    → solo si exchange tiene mercado spot
    • Derivatives             → solo si exchange tiene swap/future
    • Datasets sin soporte    → warning y omisión
    """
    exc_cfg = config.get_exchange(probe.exchange)
    if exc_cfg is None:
        log.warning("Exchange config not found | exchange=%s", probe.exchange)
        return []

    active, skipped = _filter_active_datasets(requested, probe)

    if skipped:
        log.warning(
            "Datasets skipped (unsupported by exchange) | exchange=%s skipped=%s",
            probe.exchange, sorted(skipped),
        )

    if not active:
        log.warning("No active datasets for exchange | exchange=%s", probe.exchange)
        return []

    log.info(
        "Launching pipelines | exchange=%s datasets=%s max_concurrent=%s rate_limit_ms=%s",
        probe.exchange, sorted(active), probe.max_concurrent, probe.rate_limit_ms,
    )

    futures: list = []
    has_spot    = _supports_spot(probe)
    has_futures = _supports_futures(probe)

    # --- Spot pipelines ---
    spot_active = _SPOT_DATASETS & active
    if spot_active and not has_spot:
        log.warning(
            "Spot datasets requested but no spot market | exchange=%s datasets=%s",
            probe.exchange, sorted(spot_active),
        )
    else:
        if "ohlcv" in active:
            futures.append(run_historical_pipeline.submit(config, exc_cfg, probe))
        if "trades" in active:
            futures.append(run_trades_pipeline.submit(config, exc_cfg, probe))

    # --- Derivative pipelines ---
    deriv_active = [d for d in _DERIVATIVE_DATASETS if d in active]
    if deriv_active and not has_futures:
        log.warning(
            "Derivative datasets requested but no futures market | exchange=%s datasets=%s",
            probe.exchange, deriv_active,
        )
    elif deriv_active:
        futures.append(run_derivatives_pipeline.submit(config, exc_cfg, probe, deriv_active))

    return futures


async def _consolidate_results(futures: list, log) -> None:
    """
    Resuelve todos los futures de pipelines y consolida resultados.

    SafeOps
    -------
    • Todas fallan  → RuntimeError (flow falla)
    • Algunas fallan → warning, flow continúa
    • Ninguna falla  → éxito silencioso
    """
    results   = await asyncio.gather(*futures, return_exceptions=True)
    failures  = [r for r in results if isinstance(r, BaseException)]
    successes = len(results) - len(failures)

    for exc in failures:
        log.error("Pipeline task failed | error=%s", exc)

    if failures and successes == 0:
        raise RuntimeError(
            f"All {len(failures)} pipeline(s) failed. Check logs for details."
        )
    if failures:
        log.warning(
            "Flow completed with partial failures | ok=%s failed=%s",
            successes, len(failures),
        )


async def _validate_exchanges(config: AppConfig, log) -> List[ExchangeProbe]:
    """
    Valida todos los exchanges en paralelo.

    Resuelve los futures de Prefect aquí — el resto del flow
    recibe únicamente ExchangeProbe reales.

    SafeOps: exchanges que fallen se loggean y se omiten.
    Raises RuntimeError si todos fallan.
    """
    # Lanzar validaciones en paralelo
    validation_futures = [
        validate_exchange_connection(cfg)
        for cfg in config.exchanges
    ]

    # Resolver futures → valores reales
    results = await asyncio.gather(*validation_futures, return_exceptions=True)

    probes: List[ExchangeProbe] = []
    for cfg, result in zip(config.exchanges, results):
        if isinstance(result, Exception):
            log.error(
                "Exchange validation failed | exchange=%s error=%s",
                cfg.name.value, result,
            )
        else:
            probes.append(result)

    if not probes:
        raise RuntimeError(
            "All exchange validations failed. Cannot proceed with pipelines."
        )

    log.info("Exchanges validated | ok=%s/%s", len(probes), len(config.exchanges))
    return probes


# ==========================================================
# Prefect Flow
# ==========================================================

@flow(
    name="market_data_ingestion",
    log_prints=True,
    retries=0,  # retries por task, no por flow global
)
async def market_data_flow(config_path: Path = CONFIG_PATH) -> None:
    """
    Flow principal de ingestión de datos de mercado.

    Pasos
    -----
    1. Cargar y validar configuración
    2. Validar exchanges en paralelo → resolver a ExchangeProbes reales
    3. Lanzar pipelines por exchange (dominio recibe valores, no futures)
    4. Resolver futures de pipelines y consolidar resultados
    """
    log = get_run_logger()

    # 1. Configuración
    config: AppConfig = await load_and_validate_config_task(config_path)

    if not config.datasets.any_active:
        log.warning("No active datasets configured. Flow exiting early.")
        return

    requested: Set[str] = {
        ds for ds in _ALL_DATASETS
        if getattr(config.datasets, ds, False)
    }

    log.info(
        "Flow starting | exchanges=%s datasets=%s",
        config.exchange_names, sorted(requested),
    )

    # 2. Validar exchanges → ExchangeProbes reales (futures resueltos aquí)
    probes = await _validate_exchanges(config, log)

    # 3. Lanzar pipelines — dominio recibe ExchangeProbe, nunca PrefectFuture
    pipeline_futures: list = []
    for probe in probes:
        pipeline_futures.extend(
            _launch_pipelines_for_exchange(config, probe, requested, log)
        )

    if not pipeline_futures:
        log.warning("No pipelines were launched. Check datasets and exchange capabilities.")
        return

    # 4. Consolidar resultados
    await _consolidate_results(pipeline_futures, log)

    log.info("Market data flow completed successfully.")
