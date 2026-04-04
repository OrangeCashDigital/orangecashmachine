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
    run_repair_pipeline,
)
from infra.observability.server import push_metrics
from market_data.safety import guard_context
from market_data.safety.execution_guard import ExecutionGuard
from market_data.safety.environment_validator import EnvironmentValidator, EnvironmentMismatchError

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
) -> List[asyncio.Future]:
    # No se pasa adapter — cada task crea el suyo con lifecycle aislado.
    # El markets cache en CCXTAdapter (TTL=60s) absorbe el costo de reconexión.
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
        futures.append(run_historical_pipeline(config, exc_cfg, probe))
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
) -> List[asyncio.Future]:
    # Invariante de aislamiento: no se pasa adapter entre flow y tasks.
    # Cada task gestiona su propio lifecycle de conexión.
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
        *_launch_spot_pipelines(config, exc_cfg, probe, active, log),
        *_launch_futures_pipelines(config, exc_cfg, probe, active, log),
        *_launch_derivative_pipelines(config, exc_cfg, probe, active, log),
        *([run_repair_pipeline(config, exc_cfg, probe, market_type="spot")]
          if "ohlcv" in active else []),
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
    """
    Consolida resultados de pipeline tasks. Retorna (ok, failed).

    Guard integration
    -----------------
    Los errores que llegan aquí ya sobrevivieron todos los retries de Prefect
    — son fatales por definición. El guard los cuenta para activar el kill
    switch si se supera max_errors consecutivos en futuras ejecuciones.

    SafeOps: guard_context.get_guard() retorna None si no hay guard activo
    (ej: ejecución directa desde Prefect Server sin entrypoint local).
    """
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

    # Notificar al guard — solo si hay uno activo en este proceso
    guard = guard_context.get_guard()
    if guard is not None:
        if failures:
            guard.record_error(reason=f"{len(failures)}_pipeline_tasks_failed")
            if guard.should_stop():
                log.critical(
                    "execution_guard_triggered | reason=%s",
                    guard._stop_reason,
                )
        else:
            guard.record_success()

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

    # ── Guard + Validator ─────────────────────────────────────────────────────
    # Si ya hay un guard activo (inyectado por entrypoint.py en local),
    # lo reutilizamos. Si no (Prefect directo en producción), lo creamos aquí.
    # Esto garantiza que _consolidate_results siempre encuentre un guard.
    if guard_context.get_guard() is None:
        _guard = ExecutionGuard(
            max_errors    = getattr(getattr(config, "pipeline", None), "max_consecutive_errors", 10),
            max_runtime_s = 0,  # sin límite de tiempo en producción — Prefect gestiona timeouts
        )
        _guard.start()
        guard_context.set_guard(_guard)
        log.info("ExecutionGuard initialized | source=flow (production mode)")
    else:
        log.debug("ExecutionGuard reused | source=entrypoint (local mode)")

    # EnvironmentValidator: checks locales antes de tocar red o storage.
    # SafeOps: falla rápido con mensaje claro, no a mitad del pipeline.
    try:
        from core.config.runtime import RunConfig
        _run_cfg = RunConfig.from_env()
        EnvironmentValidator().check(config, _run_cfg)
    except EnvironmentMismatchError as exc:
        log.critical("Environment validation failed — aborting | reason=%s", exc)
        raise
    except Exception as exc:
        # RunConfig puede fallar en entornos atípicos — log + continuar (no fatal)
        log.warning("EnvironmentValidator skipped | error=%s", exc)

    if not config.datasets.any_active:
        log.warning("No active datasets configured. Exiting flow.")
        return

    requested: Set[str] = set(config.datasets.active_datasets)
    log.info(
        "Datasets requested | exchanges=%s datasets=%s",
        config.exchange_names, sorted(requested),
    )

    probes, adapters = await _validate_exchanges(config, log)

    # Cerrar adapters de validación antes de lanzar pipelines.
    # Invariante: adapters no cruzan boundaries de task — cada task
    # crea el suyo. El markets cache (TTL=60s) absorbe el costo.
    for name, adapter in adapters.items():
        try:
            await adapter.close()
        except Exception as exc:
            log.warning("Adapter close (post-validation) failed | exchange=%s error=%s", name, exc)
    adapters = {}  # ya no se necesitan

    # Fase 1: spot + repair (paralelo entre exchanges, secuencial vs futures)
    # Fase 2: futures (solo cuando fase 1 completa)
    # Evita contención de rate limits cuando spot y futures comparten exchange.
    spot_futures: List[asyncio.Future] = []
    for probe in probes:
        spot_futures.extend(
            _launch_spot_and_repair(config, probe, requested, log)
        )

    futures_futures: List[asyncio.Future] = []
    for probe in probes:
        futures_futures.extend(
            _launch_futures_only(config, probe, requested, log)
        )

    pipeline_futures: List[asyncio.Future] = spot_futures + futures_futures

    if not pipeline_futures:
        log.warning("No pipelines launched. Check config and exchange capabilities.")
        return

    flow_start = time.monotonic()
    ok = failed = 0
    try:
        # Stages con dependencias explícitas.
        # requires_success=True: el stage solo corre si el anterior no falló 100%.
        # Extensible: añadir fases con sus propias reglas de dependencia.
        stages = [
            {"name": "spot+repair", "tasks": spot_futures,    "requires_success": False},
            {"name": "futures",     "tasks": futures_futures, "requires_success": True},
        ]
        for stage in stages:
            if stage["requires_success"] and failed > 0 and ok == 0:
                log.warning(
                    "Stage skipped — previous stage failed completely | stage=%s",
                    stage["name"],
                )
                break
            if not stage["tasks"]:
                continue
            log.info("Stage: %s | tasks=%s", stage["name"], len(stage["tasks"]))
            ok_s, fail_s = await _consolidate_results(stage["tasks"], log)
            ok     += ok_s
            failed += fail_s
        if failed > 0 and ok == 0:
            raise RuntimeError(f"All {failed} pipelines failed — aborting flow.")
    finally:
        # ── Guard teardown (solo si fue creado por el flow, no por entrypoint) ──
        _flow_guard = guard_context.get_guard()
        if _flow_guard is not None:
            _flow_guard.stop()
            guard_context.set_guard(None)

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
