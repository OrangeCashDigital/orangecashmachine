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
import time
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional, Set, Tuple, TYPE_CHECKING

from prefect import flow, get_run_logger

from core.config.schema import AppConfig
from core.config.hydra_loader import load_appconfig_standalone
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

from core.config.env_vars import PUSHGATEWAY_URL as _PUSHGATEWAY_URL
_PUSHGATEWAY = _PUSHGATEWAY_URL  # SSoT: env_vars.py; runtime lo resuelve desde RunConfig

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


class ExchangeTasks(NamedTuple):
    """Par de listas de futures separadas por fase para el stage system del flow."""
    spot: List[asyncio.Future]     # fase 1: spot + repair
    futures: List[asyncio.Future]  # fase 2: futures + derivatives


def _launch_pipelines_for_exchange(
    config: AppConfig,
    probe: ExchangeProbe,
    requested: Set[str],
    log,
) -> ExchangeTasks:
    # Invariante de aislamiento: no se pasa adapter entre flow y tasks.
    # Cada task gestiona su propio lifecycle de conexión.
    exc_cfg = config.get_exchange(probe.exchange)
    if exc_cfg is None:
        log.warning("Exchange config not found | exchange=%s", probe.exchange)
        return ExchangeTasks([], [])

    active, skipped = _filter_active_datasets(requested, probe)
    if skipped:
        log.warning(
            "Skipped datasets (unsupported) | exchange=%s skipped=%s",
            probe.exchange, sorted(skipped),
        )
    if not active:
        log.warning("No active datasets for exchange | exchange=%s", probe.exchange)
        return ExchangeTasks([], [])

    log.info("Launching pipelines | exchange=%s datasets=%s", probe.exchange, sorted(active))
    spot_tasks: List[asyncio.Future] = [
        *_launch_spot_pipelines(config, exc_cfg, probe, active, log),
        *([run_repair_pipeline(config, exc_cfg, probe, market_type="spot")]
          if "ohlcv" in active else []),
    ]
    futures_tasks: List[asyncio.Future] = [
        *_launch_futures_pipelines(config, exc_cfg, probe, active, log),
        *_launch_derivative_pipelines(config, exc_cfg, probe, active, log),
    ]
    return ExchangeTasks(spot=spot_tasks, futures=futures_tasks)


async def _validate_exchanges(
    config: AppConfig,
    log,
) -> List[ExchangeProbe]:
    # Adapters de validacion se crean y cierran aqui — no cruzan boundaries.
    # Cada task downstream crea el suyo propio.
    # Invariante: lifecycle completo dentro de esta funcion.
    futures = [validate_exchange_connection(exc) for exc in config.exchanges]
    results = await asyncio.gather(*futures, return_exceptions=True)

    probes: List[ExchangeProbe] = []

    for exc, res in zip(config.exchanges, results):
        if isinstance(res, Exception):
            log.error(
                "Exchange validation failed | exchange=%s error=%s",
                exc.name.value, res,
            )
        else:
            probe, adapter = res
            probes.append(probe)
            # Cerrar adapter aqui — su unico proposito era validacion.
            # Markets cache (TTL=60s) absorbe el costo en reconexion.
            try:
                await adapter.close()
            except Exception as close_exc:
                log.warning(
                    "Adapter close (post-validation) failed | exchange=%s error=%s",
                    probe.exchange, close_exc,
                )

    if not probes:
        raise RuntimeError("All exchange validations failed. Cannot proceed.")

    log.info("Exchanges validated | ok=%s/%s", len(probes), len(config.exchanges))
    return probes


# Bounded concurrency: máximo de pipelines simultáneos por stage.
# Evita saturar rate limits cuando hay múltiples exchanges en paralelo.
# Valor 4: conservador para la config actual (3 exchanges × 1 símbolo).
# Ajustar si se escala a más exchanges o símbolos.
_PIPELINE_SEMAPHORE = asyncio.Semaphore(4)


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

    Bounded concurrency: _PIPELINE_SEMAPHORE limita pipelines simultáneos.
    Evita saturar rate limits y conexiones con múltiples exchanges en paralelo.
    """
    async def _guarded(task):
        async with _PIPELINE_SEMAPHORE:
            return await task

    results  = await asyncio.gather(*(_guarded(f) for f in futures), return_exceptions=True)
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

    config = load_appconfig_standalone(env=resolved_env, config_dir=resolved_dir)

    # ── Guard + Validator ─────────────────────────────────────────────────────
    # Si ya hay un guard activo (inyectado por entrypoint.py en local),
    # lo reutilizamos. Si no (Prefect directo en producción), lo creamos aquí.
    # Esto garantiza que _consolidate_results siempre encuentre un guard.
    if guard_context.get_guard() is None:
        _guard = ExecutionGuard(
            max_errors    = getattr(getattr(config, "pipeline", None), "max_consecutive_errors", 10),
            max_runtime_s = None,  # None = sin límite — Prefect Worker gestiona el timeout externamente
        )
        _guard.start()
        guard_context.set_guard(_guard)
        log.info("ExecutionGuard initialized | source=flow (production mode)")
    else:
        log.debug("ExecutionGuard reused | source=entrypoint (local mode)")

    # EnvironmentValidator ya ejecutado en run_application() antes de llamar al flow.
    # En modo Prefect standalone (sin entrypoint), el guard lo cubre en su contexto.

    if not config.datasets.any_active:
        log.warning("No active datasets configured. Exiting flow.")
        return

    requested: Set[str] = set(config.datasets.active_datasets)
    log.info(
        "Datasets requested | exchanges=%s datasets=%s",
        config.exchange_names, sorted(requested),
    )

    probes = await _validate_exchanges(config, log)


    # SSoT: _launch_pipelines_for_exchange centraliza filtrado, validación de
    # capabilities y separación de stages. El flow solo acumula y ejecuta.
    spot_futures: List[asyncio.Future] = []
    futures_futures: List[asyncio.Future] = []

    for probe in probes:
        tasks = _launch_pipelines_for_exchange(config, probe, requested, log)
        spot_futures.extend(tasks.spot)
        futures_futures.extend(tasks.futures)

    pipeline_futures: List[asyncio.Future] = spot_futures + futures_futures

    if not pipeline_futures:
        log.warning("No pipelines launched. Check config and exchange capabilities.")
        return

    flow_start = time.monotonic()
    ok = failed = 0
    ok_prev = fail_prev = 0  # resultado del stage inmediatamente anterior
    try:
        # Stages con dependencias explícitas.
        # requires_success=True: el stage solo corre si el anterior no falló 100%.
        # ok_prev/fail_prev reflejan el stage anterior, no el acumulado global,
        # para evitar que fallos de fases posteriores bloqueen stages siguientes.
        # Extensible: añadir fases con sus propias reglas de dependencia.
        stages = [
            {"name": "spot+repair", "tasks": spot_futures,    "requires_success": False},
            {"name": "futures",     "tasks": futures_futures, "requires_success": True},
        ]
        for stage in stages:
            if stage["requires_success"] and fail_prev > 0 and ok_prev == 0:
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
            ok_prev, fail_prev = ok_s, fail_s  # snapshot para el stage siguiente
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
