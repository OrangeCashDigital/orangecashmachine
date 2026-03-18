from __future__ import annotations
import asyncio
from typing import List, Set, Tuple

from prefect import flow, get_run_logger

from core.config.schema import AppConfig
from core.config.loader import load_config
from market_data.orchestration.tasks.exchange_tasks import ExchangeProbe, validate_exchange_connection
from market_data.orchestration.tasks.batch_tasks import (
    run_historical_pipeline,
    run_trades_pipeline,
    run_derivatives_pipeline,
)

# ==================================================================
# Helpers de dominio (sin Prefect, totalmente testeables)
# ==================================================================

def _filter_active_datasets(requested: Set[str], probe: ExchangeProbe) -> Tuple[Set[str], Set[str]]:
    """Intersecta datasets solicitados con los soportados por el exchange."""
    supported = set(probe.supported_datasets)
    return requested & supported, requested - supported

def _supports_spot(probe: ExchangeProbe) -> bool:
    return "spot" in probe.available_markets

def _supports_futures(probe: ExchangeProbe) -> bool:
    return bool({"swap", "future"} & set(probe.available_markets))

def _launch_spot_pipelines(config: AppConfig, exc_cfg, probe: ExchangeProbe, active: Set[str], log) -> List[asyncio.Future]:
    """Lanza pipelines de mercado spot (ohlcv, trades, orderbook) si el exchange los soporta."""
    spot_requested = active & {"ohlcv", "trades", "orderbook"}
    if not spot_requested:
        return []
    if not _supports_spot(probe):
        log.warning(
            "Spot datasets requested but no spot market | exchange=%s datasets=%s",
            probe.exchange, sorted(spot_requested)
        )
        return []

    futures: List[asyncio.Future] = []
    if "ohlcv" in spot_requested:
        futures.append(run_historical_pipeline(config, exc_cfg, probe))
    if "trades" in spot_requested:
        futures.append(run_trades_pipeline(config, exc_cfg, probe))
    if "orderbook" in spot_requested:
        # Asumiendo que orderbook usa mismo task que trades u otra task específica
        futures.append(run_trades_pipeline(config, exc_cfg, probe, dataset="orderbook"))

    return futures

def _launch_derivative_pipelines(config: AppConfig, exc_cfg, probe: ExchangeProbe, active: Set[str], log) -> List[asyncio.Future]:
    """Lanza pipelines de derivados (funding_rate, open_interest, liquidations, mark_price, index_price) si soportados."""
    deriv_requested = active & {"funding_rate", "open_interest", "liquidations", "mark_price", "index_price"}
    if not deriv_requested:
        return []
    if not _supports_futures(probe):
        log.warning(
            "Derivative datasets requested but no futures market | exchange=%s datasets=%s",
            probe.exchange, sorted(deriv_requested)
        )
        return []
    return [run_derivatives_pipeline(config, exc_cfg, probe, deriv_requested)]

def _launch_pipelines_for_exchange(config: AppConfig, probe: ExchangeProbe, requested: Set[str], log) -> List[asyncio.Future]:
    """Orquesta pipelines para un exchange concreto (spot + derivatives)."""
    exc_cfg = config.get_exchange(probe.exchange)
    if exc_cfg is None:
        log.warning("Exchange config not found | exchange=%s", probe.exchange)
        return []

    active, skipped = _filter_active_datasets(requested, probe)
    if skipped:
        log.warning("Skipped datasets (unsupported) | exchange=%s skipped=%s", probe.exchange, sorted(skipped))
    if not active:
        log.warning("No active datasets for exchange | exchange=%s", probe.exchange)
        return []

    log.info("Launching pipelines | exchange=%s datasets=%s", probe.exchange, sorted(active))

    return [
        *_launch_spot_pipelines(config, exc_cfg, probe, active, log),
        *_launch_derivative_pipelines(config, exc_cfg, probe, active, log),
    ]

async def _validate_exchanges(config: AppConfig, log) -> List[ExchangeProbe]:
    """Valida exchanges en paralelo y retorna ExchangeProbes reales. SafeOps: errores loggeados, omite fallidos."""
    futures = [validate_exchange_connection(exc) for exc in config.exchanges]
    results = await asyncio.gather(*futures, return_exceptions=True)

    probes: List[ExchangeProbe] = []
    for exc, res in zip(config.exchanges, results):
        if isinstance(res, Exception):
            log.error("Exchange validation failed | exchange=%s error=%s", exc.name.value, res)
        else:
            probes.append(res)

    if not probes:
        raise RuntimeError("All exchange validations failed. Cannot proceed.")

    log.info("Exchanges validated | ok=%s/%s", len(probes), len(config.exchanges))
    return probes

async def _consolidate_results(futures: list, log) -> None:
    """Espera todas las futures y consolida resultados, loggeando fallos parciales."""
    results = await asyncio.gather(*futures, return_exceptions=True)
    failures = [r for r in results if isinstance(r, Exception)]
    successes = len(results) - len(failures)

    for f in failures:
        log.error("Pipeline task failed | error=%s", f)

    if failures and successes == 0:
        raise RuntimeError(f"All {len(failures)} pipelines failed")
    if failures:
        log.warning("Flow completed with partial failures | ok=%s failed=%s", successes, len(failures))

# ==================================================================
# Prefect Flow
# ==================================================================

@flow(name="market_data_ingestion", log_prints=True, retries=0)
async def market_data_flow(config: AppConfig | None = None):
    """
    Flow principal de ingestión de datos de mercado.

    1. Cargar y validar configuración
    2. Validar exchanges en paralelo → ExchangeProbes reales
    3. Lanzar pipelines por exchange
    4. Consolidar resultados
    """
    log = get_run_logger()

    # Cargar configuración si no se inyecta
    if config is None:
        config = load_config()

    if not config.datasets.any_active:
        log.warning("No active datasets configured. Exiting flow.")
        return

    requested: Set[str] = set(config.datasets.active_datasets)
    log.info("Flow starting | exchanges=%s datasets=%s", config.exchange_names, sorted(requested))

    # 1. Validar exchanges → ExchangeProbes reales
    probes = await _validate_exchanges(config, log)

    # 2. Lanzar pipelines
    pipeline_futures: List[asyncio.Future] = []
    for probe in probes:
        pipeline_futures.extend(_launch_pipelines_for_exchange(config, probe, requested, log))

    if not pipeline_futures:
        log.warning("No pipelines launched. Check config and exchange capabilities.")
        return

    # 3. Consolidar resultados
    await _consolidate_results(pipeline_futures, log)
    log.info("Market data flow completed successfully")