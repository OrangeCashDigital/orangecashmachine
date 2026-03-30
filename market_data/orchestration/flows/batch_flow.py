"""
market_data/orchestration/flows/batch_flow.py
=============================================

Flow principal de ingestión de datos de mercado.

Responsabilidad
---------------
Orquestar la validación de exchanges y el lanzamiento de pipelines
de datos (OHLCV, trades, derivados) de forma paralela y resiliente.

Este módulo NO ejecuta lógica de negocio directamente.
Eso es responsabilidad de los tasks y pipelines.

Principios
----------
SOLID  – SRP: el flow orquesta, los tasks ejecutan
KISS   – flujo lineal y predecible
DRY    – helpers desacoplados y testeables
SafeOps – adapters siempre cerrados en finally, fallos parciales tolerados

Métricas
--------
push_metrics en el finally garantiza que las métricas
lleguen al Pushgateway en producción (Prefect Worker), donde entrypoint.py
no se ejecuta. Job por exchange evita colisiones last-write-wins.
"""

from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, TYPE_CHECKING

from prefect import flow, get_run_logger

from core.config.schema import AppConfig
from core.config.loader import load_config
from market_data.orchestration.tasks.exchange_tasks import ExchangeProbe, validate_exchange_connection
if TYPE_CHECKING:
    from market_data.adapters.exchange.ccxt_adapter import CCXTAdapter
from market_data.orchestration.tasks.batch_tasks import (
    run_historical_pipeline,
    run_futures_pipeline,
    run_trades_pipeline,
    run_derivatives_pipeline,
)
from infra.observability.server import push_metrics

_PUSHGATEWAY = os.getenv("PUSHGATEWAY_URL", "localhost:9091")

# ==================================================================
# Helpers de dominio (desacoplados y testeables)
# ==================================================================

def _filter_active_datasets(
    requested: Set[str],
    probe: ExchangeProbe,
) -> Tuple[Set[str], Set[str]]:
    supported = set(probe.supported_datasets)
    return requested & supported, requested - supported


def _supports_market_type(probe: ExchangeProbe, market_type: str) -> bool:
    return market_type in probe.available_markets


def _launch_spot_pipelines(
    config: AppConfig,
    exc_cfg,
    probe: ExchangeProbe,
    active: Set[str],
    log,
    adapter: "CCXTAdapter | None" = None,
) -> List[asyncio.Future]:
    spot_requested = active & {"ohlcv", "trades", "orderbook"}
    if not spot_requested:
        return []

    if not _supports_market_type(probe, "spot"):
        log.warning(
            "Spot datasets requested but no spot market | exchange=%s datasets=%s",
            probe.exchange, sorted(spot_requested),
        )
        return []

    futures: List[asyncio.Future] = []
    if "ohlcv" in spot_requested:
        futures.append(run_historical_pipeline(config, exc_cfg, probe, exchange_client=adapter))
    if "trades" in spot_requested:
        futures.append(run_trades_pipeline(config, exc_cfg, probe, dataset="trades"))
    if "orderbook" in spot_requested:
        futures.append(run_trades_pipeline(config, exc_cfg, probe, dataset="orderbook"))

    return futures


def _launch_futures_pipelines(
    config:  AppConfig,
    exc_cfg,
    probe:   ExchangeProbe,
    active:  Set[str],
    log,
) -> List[asyncio.Future]:
    if "ohlcv" not in active:
        return []
    if not exc_cfg.has_futures:
        return []
    has_swap   = _supports_market_type(probe, "swap")
    has_future = _supports_market_type(probe, "future")
    if not has_swap and not has_future:
        log.warning(
            "Futures configured but exchange has no swap/future market | exchange=%s",
            probe.exchange,
        )
        return []
    log.info(
        "Launching futures pipeline | exchange=%s symbols=%s market=%s",
        probe.exchange,
        exc_cfg.markets.futures_symbols,
        exc_cfg.markets.futures_default_type or "swap",
    )
    return [run_futures_pipeline(config, exc_cfg, probe)]


def _launch_derivative_pipelines(
    config: AppConfig,
    exc_cfg,
    probe: ExchangeProbe,
    active: Set[str],
    log,
) -> List[asyncio.Future]:
    derivative_datasets = {
        "funding_rate", "open_interest", "liquidations", "mark_price", "index_price"
    }
    deriv_requested = active & derivative_datasets
    if not deriv_requested:
        return []

    if not _supports_market_type(probe, "swap") and not _supports_market_type(probe, "future"):
        log.warning(
            "Derivative datasets requested but no futures market | exchange=%s datasets=%s",
            probe.exchange, sorted(deriv_requested),
        )
        return []

    return [run_derivatives_pipeline(config, exc_cfg, probe, list(deriv_requested))]


def _launch_pipelines_for_exchange(
    config: AppConfig,
    probe: ExchangeProbe,
    requested: Set[str],
    log,
    adapter: "CCXTAdapter | None" = None,
) -> List[asyncio.Future]:
    exc_cfg = config.get_exchange(probe.exchange)
    if exc_cfg is None:
        log.warning("Exchange config not found | exchange=%s", probe.exchange)
        return []

    active, skipped = _filter_active_datasets(requested, probe)
    if skipped:
        log.warning(
            "Skipped datasets (unsupported) | exchange=%s skipped=%s",
            probe.exchange, sorted(skipped),
        )
    if not active:
        log.warning("No active datasets for exchange | exchange=%s", probe.exchange)
        return []

    log.info("Launching pipelines | exchange=%s datasets=%s", probe.exchange, sorted(active))
    return [
        *_launch_spot_pipelines(config, exc_cfg, probe, active, log, adapter=adapter),
        *_launch_futures_pipelines(config, exc_cfg, probe, active, log),
        *_launch_derivative_pipelines(config, exc_cfg, probe, active, log),
    ]


async def _validate_exchanges(
    config: AppConfig,
    log,
) -> Tuple[List[ExchangeProbe], Dict[str, CCXTAdapter]]:
    futures = [validate_exchange_connection(exc) for exc in config.exchanges]
    results = await asyncio.gather(*futures, return_exceptions=True)

    probes:   List[ExchangeProbe]    = []
    adapters: Dict[str, CCXTAdapter] = {}

    for exc, res in zip(config.exchanges, results):
        if isinstance(res, Exception):
            log.error(
                "Exchange validation failed | exchange=%s error=%s",
                exc.name.value, res,
            )
        else:
            probe, adapter = res
            probes.append(probe)
            adapters[probe.exchange] = adapter

    if not probes:
        raise RuntimeError("All exchange validations failed. Cannot proceed.")

    log.info("Exchanges validated | ok=%s/%s", len(probes), len(config.exchanges))
    return probes, adapters


async def _consolidate_results(
    futures: List[asyncio.Future],
    log,
) -> tuple[int, int]:
    """Consolida resultados de pipeline tasks. Retorna (ok, failed)."""
    results  = await asyncio.gather(*futures, return_exceptions=True)
    failures = [r for r in results if isinstance(r, Exception)]
    ok       = len(results) - len(failures)

    for f in failures:
        log.error("Pipeline task failed | error=%s", f)

    if failures:
        log.warning(
            "Flow completed with partial failures | ok=%s failed=%s",
            ok, len(failures),
        )

    return ok, len(failures)


# ==================================================================
# Prefect Flow
# ==================================================================

@flow(
    name="market_data_ingestion",
    description="Ingesta de datos de mercado: OHLCV histórico por exchange y timeframe.",
    log_prints=True,
    retries=0,
)
async def market_data_flow(
    env: Optional[str] = None,
    config_dir: Optional[str] = None,
) -> None:
    """
    Flow principal de ingestión de datos de mercado.

    Flujo
    -----
    1. Resolver configuración portable (parámetros > env vars > defaults)
    2. Validar exchanges en paralelo → ExchangeProbes reales
    3. Lanzar pipelines por exchange (adapter inyectado)
    4. Consolidar resultados
    5. Push métricas por exchange (evita colisiones en Pushgateway)
    6. Delete métricas (evita stale metrics en próximo run fallido)
    7. Cerrar adapters — lifecycle garantizado en finally
    """
    log = get_run_logger()

    # env y config_dir vienen desde quien dispara el flow (RunConfig o CLI).
    # El flow de Prefect no lee el entorno directamente — recibe valores resueltos.
    resolved_env = env or "production"
    resolved_dir = Path(config_dir) if config_dir else Path("/app/config")

    log.info("Flow starting | env=%s config_dir=%s", resolved_env, resolved_dir)

    config = load_config(env=resolved_env, path=resolved_dir)

    if not config.datasets.any_active:
        log.warning("No active datasets configured. Exiting flow.")
        return

    requested: Set[str] = set(config.datasets.active_datasets)
    log.info(
        "Datasets requested | exchanges=%s datasets=%s",
        config.exchange_names, sorted(requested),
    )

    probes, adapters = await _validate_exchanges(config, log)

    pipeline_futures: List[asyncio.Future] = []
    for probe in probes:
        adapter = adapters.get(probe.exchange)
        pipeline_futures.extend(
            _launch_pipelines_for_exchange(config, probe, requested, log, adapter=adapter)
        )

    if not pipeline_futures:
        log.warning("No pipelines launched. Check config and exchange capabilities.")
        return

    flow_start = time.monotonic()
    ok = failed = 0
    try:
        ok, failed = await _consolidate_results(pipeline_futures, log)
        if failed > 0 and ok == 0:
            raise RuntimeError(f"All {failed} pipelines failed — aborting flow.")
    finally:
        # ── Cerrar adapters ───────────────────────────────────────────────────
        for name, adapter in adapters.items():
            try:
                await adapter.close()
            except Exception as exc:
                log.warning("Adapter close failed | exchange=%s error=%s", name, exc)

        # ── Push métricas por exchange ────────────────────────────────────────
        for probe in probes:
            push_metrics(exchange=probe.exchange, gateway=_PUSHGATEWAY)
            log.info("Metrics pushed | exchange=%s", probe.exchange)

        # ── Flow summary ──────────────────────────────────────────────────────
        duration_s  = time.monotonic() - flow_start
        flow_status = "OK" if failed == 0 else ("PARTIAL" if ok > 0 else "FAILED")
        log.info(
            "══ FLOW SUMMARY | %s | env=%s exchanges=%s/%s "
            "pipelines=%s/%s duration=%.1fs ══",
            flow_status, resolved_env,
            len(probes), len(config.exchanges),
            ok, len(pipeline_futures), duration_s,
        )

    if failed == 0:
        log.info("Market data flow completed successfully.")
