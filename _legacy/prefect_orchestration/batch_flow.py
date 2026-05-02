"""
ocm_platform/control_plane/orchestration/flows/batch_flow.py
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
import os
import re

import asyncio
from dataclasses import dataclass
import time
from pathlib import Path
from typing import List, NamedTuple, Optional, Set, Tuple

from prefect import flow, get_run_logger
from prefect.runtime import flow_run as _prefect_flow_run

from ocm_platform.config.schema import AppConfig
from ocm_platform.runtime.context import RuntimeContext
from ocm_platform.control_plane.orchestration.tasks.exchange_tasks import (
    ExchangeProbe,
    validate_exchange_connection,
)

from ocm_platform.control_plane.orchestration.tasks.batch_tasks import (
    run_historical_pipeline,
    run_futures_pipeline,
    run_trades_pipeline,
    run_derivatives_pipeline,
    run_repair_pipeline,
)
from market_data.ports.observability import MetricsPusherPort
from ocm_platform.runtime import guard_context
from ocm_platform.runtime.guard import ExecutionGuard

from ocm_platform.config.env_vars import PUSHGATEWAY_URL as _PUSHGATEWAY_URL

# Leer el valor de la variable de entorno, no el nombre de la constante.
# RunConfig.from_env() ya normaliza http(s):// → host:port.


class _NoopMetricsPusher:
    """Sentinel no-op — activo cuando pusher=None llega al flow.

    Inyectado desde el composition root cuando métricas están
    deshabilitadas. SafeOps: nunca lanza.
    """
    def push(self, labels=None) -> None:
        pass  # intencional
_PUSHGATEWAY: str = re.sub(
    r'^https?://', '',
    os.getenv(_PUSHGATEWAY_URL, 'localhost:9091'),
)

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
    # El adapter del probe (ya conectado y con markets cacheados) se inyecta
    # en los tasks via exchange_client. El flow cierra el adapter en finally.
    spot_requested = active & {"ohlcv", "trades", "orderbook"}
    if not spot_requested:
        return []

    if not _supports_market_type(probe, "spot"):
        log.warning(
            "Spot datasets requested but no spot market | exchange=%s datasets=%s",
            probe.exchange,
            sorted(spot_requested),
        )
        return []

    futures: List[asyncio.Future] = []
    if "ohlcv" in spot_requested:
        futures.append(run_historical_pipeline(config, exc_cfg, probe,
                                               exchange_client=probe.adapter))
    if "trades" in spot_requested:
        # TradesPipeline: tick data — dominio propio, adapter inyectado desde probe.
        # Orderbook es un dominio distinto (L2 streaming) — no se mapea a TradesPipeline.
        futures.append(run_trades_pipeline(config, exc_cfg, probe,
                                           exchange_client=probe.adapter))

    return futures


def _launch_futures_pipelines(
    config: AppConfig,
    exc_cfg,
    probe: ExchangeProbe,
    active: Set[str],
    log,
) -> List[asyncio.Future]:
    if "ohlcv" not in active:
        return []
    if not exc_cfg.has_futures:
        return []
    has_swap = _supports_market_type(probe, "swap")
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
        "funding_rate",
        "open_interest",
        "liquidations",
        "mark_price",
        "index_price",
    }
    deriv_requested = active & derivative_datasets
    if not deriv_requested:
        return []

    if not _supports_market_type(probe, "swap") and not _supports_market_type(
        probe, "future"
    ):
        log.warning(
            "Derivative datasets requested but no futures market | exchange=%s datasets=%s",
            probe.exchange,
            sorted(deriv_requested),
        )
        return []

    return [run_derivatives_pipeline(config, exc_cfg, probe, list(deriv_requested))]


class ExchangeTasks(NamedTuple):
    """
    Listas de futures separadas por nodo del grafo de dependencias.

    Nodos activos
    -------------
    spot            : OHLCV spot — sin dependencias, siempre ejecuta
    futures         : OHLCV swap/future + derivatives — independiente de spot
    repair          : gap healing — PARTIAL_SUCCESS(spot)

    Nodos OCP (listos para activar — stub [] hasta implementar task)
    ----------------------------------------------------------------
    backfill        : descarga histórica full — sin dependencias
    reconciliation  : reconcilia Silver vs Bronze — PARTIAL_SUCCESS(spot)
    validation      : valida integridad post-ingestion — PARTIAL_SUCCESS(spot|futures)
    feature_eng     : computa features desde Silver — PARTIAL_SUCCESS(spot)

    Principio OCP
    -------------
    Añadir nodo N:
      1. Campo nuevo aquí (List[asyncio.Future], default=[])
      2. _launch_pipelines_for_exchange: popula el campo si el task existe
      3. market_data_flow: _run_stage("N", tasks.N, log) + .merge()
    Cero cambios en _StageResult, _run_stage, _consolidate_results.

    Ref: Etikyala (2023), Kumar (2025) — dependency-satisfaction execution model
    """

    spot:           List[asyncio.Future]  # nodo 1: spot (independiente)
    futures:        List[asyncio.Future]  # nodo 2: futures + derivatives (independiente)
    repair:         List[asyncio.Future]  # nodo 3: PARTIAL_SUCCESS(spot)
    backfill:       List[asyncio.Future]  # nodo 4: sin dependencias (stub [])
    reconciliation: List[asyncio.Future]  # nodo 5: PARTIAL_SUCCESS(spot) (stub [])
    validation:     List[asyncio.Future]  # nodo 6: PARTIAL_SUCCESS(spot|futures) (stub [])
    feature_eng:    List[asyncio.Future]  # nodo 7: PARTIAL_SUCCESS(spot) (stub [])


def _launch_pipelines_for_exchange(
    config: AppConfig,
    probe: ExchangeProbe,
    requested: Set[str],
    log,
) -> ExchangeTasks:
    # El adapter del probe se pasa via exchange_client a los tasks relevantes.
    # Cada task gestiona su propio lifecycle de conexión.
    exc_cfg = config.get_exchange(probe.exchange)
    if exc_cfg is None:
        log.warning("Exchange config not found | exchange=%s", probe.exchange)
        return ExchangeTasks(
            spot=[], futures=[], repair=[],
            backfill=[], reconciliation=[], validation=[], feature_eng=[],
        )

    active, skipped = _filter_active_datasets(requested, probe)
    if skipped:
        log.warning(
            "Skipped datasets (unsupported) | exchange=%s skipped=%s",
            probe.exchange,
            sorted(skipped),
        )
    if not active:
        log.warning("No active datasets for exchange | exchange=%s", probe.exchange)
        return ExchangeTasks(
            spot=[], futures=[], repair=[],
            backfill=[], reconciliation=[], validation=[], feature_eng=[],
        )

    log.info(
        "Launching pipelines | exchange=%s datasets=%s", probe.exchange, sorted(active)
    )
    spot_tasks: List[asyncio.Future] = _launch_spot_pipelines(
        config, exc_cfg, probe, active, log
    )
    # repair se separa de spot: tiene dependencia semántica propia (PARTIAL_SUCCESS).
    # El flow decide si ejecutar repair basándose en el resultado de spot,
    # no en si spot fue lanzado. Ver grafo de dependencias en market_data_flow.
    repair_tasks: List[asyncio.Future] = (
        [run_repair_pipeline(
            config, exc_cfg, probe,
            market_type="spot",
            exchange_client=probe.adapter,
        )]
        if "ohlcv" in active
        else []
    )
    futures_tasks: List[asyncio.Future] = [
        *_launch_futures_pipelines(config, exc_cfg, probe, active, log),
        *_launch_derivative_pipelines(config, exc_cfg, probe, active, log),
    ]
    return ExchangeTasks(
        spot           = spot_tasks,
        futures        = futures_tasks,
        repair         = repair_tasks,
        backfill       = [],   # stub — activar cuando BackfillTask exista
        reconciliation = [],   # stub — activar cuando ReconciliationTask exista
        validation     = [],   # stub — activar cuando ValidationTask exista
        feature_eng    = [],   # stub — activar cuando FeatureEngTask exista
    )


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
                exc.name.value,
                res,
            )
        else:
            probe, adapter = res
            probe.adapter = adapter  # lifecycle: flow cierra en finally
            probes.append(probe)

    if not probes:
        raise RuntimeError("All exchange validations failed. Cannot proceed.")

    log.info("Exchanges validated | ok=%s/%s", len(probes), len(config.exchanges))
    return probes


# Bounded concurrency: máximo de pipelines simultáneos por stage.
# Evita saturar rate limits cuando hay múltiples exchanges en paralelo.
# Valor 4: conservador para la config actual (3 exchanges × 1 símbolo).
# Ajustar si se escala a más exchanges o símbolos.
# ══════════════════════════════════════════════════════════════════════════
# Domain model — StageResult
# ══════════════════════════════════════════════════════════════════════════
@dataclass
class _StageResult:
    """
    Modelo de dominio para el resultado de un nodo del grafo de ejecución.

    Principios
    ----------
    SRP  — encapsula estado (ok/failed/executed) sin mezclar lógica de orchestration.
    OCP  — añadir nodo N = crear otro _StageResult, sin tocar lógica central.
    DIP  — el grafo depende de esta abstracción, no de ints primitivos.

    Semántica de `executed`
    -----------------------
    False → nodo NO lanzado (futures vacío — ningún par configurado para este stage).
    True  → nodo lanzado, independientemente del ratio ok/failed.

    Distinguir "no ejecutado" de "ejecutado con cero resultados" es crítico para:
      - métricas (no contaminar dashboards con nodos vacíos como "all_failed")
      - logs (SKIPPED vs ALL_FAILED comunican causas distintas)
      - dependencias entre nodos (repair solo depende de executed + has_success)

    Métodos públicos
    ----------------
    has_success() — True si el nodo fue lanzado y al menos 1 pipeline completó OK.
                    Semántica PARTIAL_SUCCESS: repair corre aunque haya fallos
                    siempre que haya algún éxito.
    status        — str canónico para logs/métricas (SSOT — un único punto de verdad).
    merge()       — acumulación SSOT: un único punto de suma al final del grafo.
                    OCP: añadir nodo = añadir .merge() a la cadena.
    """
    ok:       int  = 0
    failed:   int  = 0
    executed: bool = False   # False = nodo no lanzado (futures vacío)

    def has_success(self) -> bool:
        """True si el nodo fue lanzado y al menos 1 pipeline completó sin error."""
        return self.executed and self.ok > 0

    @property
    def status(self) -> str:
        """Estado canónico del nodo — SSOT para logs y métricas.

        SKIPPED    — nodo no lanzado (sin pares configurados para este stage).
        ALL_OK     — todos los pipelines del nodo completaron sin error.
        PARTIAL    — mezcla de éxitos y fallos (Fail-Soft: el flow continúa).
        ALL_FAILED — todos los pipelines fallaron (candidato a Fail-Fast).
        """
        if not self.executed:
            return "SKIPPED"
        if self.failed == 0:
            return "ALL_OK"
        if self.ok > 0:
            return "PARTIAL"
        return "ALL_FAILED"

    def merge(self, other: "_StageResult") -> "_StageResult":
        """Suma dos resultados de nodo — SSOT de agregación.

        executed se propaga con OR: si cualquiera de los dos nodos fue lanzado,
        el resultado agregado refleja que hubo actividad real.
        """
        return _StageResult(
            ok       = self.ok     + other.ok,
            failed   = self.failed + other.failed,
            executed = self.executed or other.executed,
        )


async def _run_stage(
    name:    str,
    futures: "list[asyncio.Future]",
    log,
) -> "_StageResult":
    """
    Ejecuta un nodo del grafo y retorna su StageResult.

    Encapsula el patrón repetido en los 3 nodos:
      log.info → _consolidate_results → StageResult

    KISS — elimina duplicación sin sacrificar claridad.
    SafeOps — si futures está vacío, retorna StageResult vacío
              sin tocar _consolidate_results (never call with empty list).

    Returns
    -------
    _StageResult con ok/failed del nodo. Si futures está vacío, (0, 0).
    """
    if not futures:
        # executed=False (default) — nodo no lanzado, distinto de "lanzado sin resultados"
        return _StageResult(executed=False)
    log.info("Graph node: %s | tasks=%s | status=launching", name, len(futures))
    ok, failed = await _consolidate_results(futures, log)
    result = _StageResult(ok=ok, failed=failed, executed=True)
    log.info("Graph node: %s | status=%s | ok=%s | failed=%s", name, result.status, ok, failed)
    return result


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

    results = await asyncio.gather(
        *(_guarded(f) for f in futures), return_exceptions=True
    )
    failures = [r for r in results if isinstance(r, Exception)]
    ok = len(results) - len(failures)

    for f in failures:
        log.error("Pipeline task failed | error=%s", f)

    if failures:
        log.warning(
            "Flow completed with partial failures | ok=%s failed=%s",
            ok,
            len(failures),
        )

    # Notificar al guard — solo si hay uno activo en este proceso
    guard = guard_context.get_guard()
    if guard is not None:
        if failures:
            guard.record_error(reason=f"{len(failures)}_pipeline_tasks_failed")
            if guard.should_stop():
                log.critical(
                    "execution_guard_triggered | reason=%s",
                    guard.stop_reason,
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
    # Protección contra acumulación de backlog.
    # 55 s < 60 s (intervalo del schedule) → Prefect cancela el run
    # antes del siguiente tick. Sin esto, runs lentos se acumulan en cola.
    timeout_seconds=55,
)
async def market_data_flow(
    runtime_context: Optional[RuntimeContext] = None,
    pusher: Optional[MetricsPusherPort] = None,
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
    # Binding explícito OCM run_id ↔ Prefect flow run ID para trazabilidad cruzada.
    # Permite correlacionar registros de run_registry, Prometheus y Prefect UI
    # con un único identificador en cualquier sistema de observabilidad.
    _prefect_id = str(_prefect_flow_run.id) if _prefect_flow_run.id else "no-prefect-context"
    # Support invocation with a serialized context (dict) coming from Prefect
    if isinstance(runtime_context, dict):
        runtime_context = RuntimeContext.from_dict(runtime_context)  # type: ignore[assignment]

    # El flow recibe un RuntimeContext resuelto por el entrypoint/runner.
    if runtime_context is None:
        raise RuntimeError(
            "market_data_flow requires a resolved RuntimeContext provided by the entrypoint."
        )
    config = runtime_context.app_config
    env = runtime_context.environment
    config_dir = Path("config").resolve()
    log.info(
        "flow_starting | env=%s config_dir=%s ocm_run_id=%s prefect_run_id=%s",
        env,
        config_dir,
        runtime_context.run_id,
        _prefect_id,
    )
    # ── Guard + Validator ─────────────────────────────────────────────────────
    # Si ya hay un guard activo (inyectado por entrypoint.py en local),
    # lo reutilizamos. Si no (Prefect directo en producción), lo creamos aquí.
    # Esto garantiza que _consolidate_results siempre encuentre un guard.
    if guard_context.get_guard() is None:
        _guard = ExecutionGuard(
            max_errors=getattr(
                getattr(config, "pipeline", None), "max_consecutive_errors", 10
            ),
            max_runtime_s=None,  # None = sin límite — Prefect Worker gestiona el timeout externamente
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
        config.exchange_names,
        sorted(requested),
    )

    # Validación de exchanges: si falla, abortar gracefully y registrar.
    try:
        probes = await _validate_exchanges(config, log)
    except Exception as exc:
        log.error("exchange_validation_failed | error={}", exc)
        return

    # SSoT: _launch_pipelines_for_exchange centraliza filtrado, validación de
    # capabilities y separación de stages. El flow solo acumula y ejecuta.
    spot_futures:           List[asyncio.Future] = []
    futures_futures:        List[asyncio.Future] = []
    repair_futures:         List[asyncio.Future] = []
    backfill_futures:       List[asyncio.Future] = []
    reconciliation_futures: List[asyncio.Future] = []
    validation_futures:     List[asyncio.Future] = []
    feature_eng_futures:    List[asyncio.Future] = []

    for probe in probes:
        tasks = _launch_pipelines_for_exchange(config, probe, requested, log)
        spot_futures.extend(tasks.spot)
        futures_futures.extend(tasks.futures)
        repair_futures.extend(tasks.repair)
        backfill_futures.extend(tasks.backfill)
        reconciliation_futures.extend(tasks.reconciliation)
        validation_futures.extend(tasks.validation)
        feature_eng_futures.extend(tasks.feature_eng)

    # Total para el summary — incluye todos los nodos del grafo
    pipeline_futures: List[asyncio.Future] = (
        spot_futures + futures_futures + repair_futures
        + backfill_futures + reconciliation_futures
        + validation_futures + feature_eng_futures
    )

    if not pipeline_futures:
        log.warning("No pipelines launched. Check config and exchange capabilities.")
        return

    flow_start = time.monotonic()
    try:
        # ── Grafo de dependencias explícito ─────────────────────────────────
        #
        # Diseño basado en dependency-satisfaction (no en orden secuencial):
        #
        #   spot    ──────────────────────────────────────────► (siempre)
        #   futures ──────────────────────────────────────────► (siempre, independiente)
        #   repair  ── depende de spot con PARTIAL_SUCCESS ───► (si spot_result.has_success() > 0)
        #
        # spot y futures son semánticamente independientes: usan APIs distintas
        # (spot vs swap), storage separado y cursores independientes.
        # Un fallo de spot NO debe bloquear futures.
        #
        # repair tiene dependencia semántica real: opera sobre datos escritos
        # por spot. Si spot falló completamente (spot_result.has_success() == 0), no hay datos
        # que reparar — skip seguro y explícito.
        #
        # Ref: Etikyala (2023) — "dependencies satisfied, not previous stage complete"
        # Ref: Kumar (2025)    — fail-soft: aislar fallos por dominio
        # Ref: Navarro (2025)  — pipelines lineales crean bottlenecks innecesarios
        #
        # Extensión futura: añadir nodo al grafo sin tocar nodos existentes.

        # ── Nodo 1: spot — sin dependencias, siempre ejecuta ────────────────
        spot_result = await _run_stage("spot", spot_futures, log)

        # ── Nodo 2: futures — sin dependencias, independiente de spot ────────
        # Razón: API swap != API spot; fallo de spot no implica fallo de futures.
        futures_result = await _run_stage("futures", futures_futures, log)

        # ── Nodo 3: repair — dependencia PARTIAL_SUCCESS(spot) ───────────────
        # Solo ejecuta si spot escribió al menos 1 serie exitosa.
        # Si spot falló completamente, no hay datos que reparar — skip explícito.
        repair_result = _StageResult()
        if repair_futures:
            if not spot_result.has_success() and spot_futures:
                log.warning(
                    "Graph node: repair skipped"
                    " — spot failed completely, no data to repair"
                    " | spot_ok=%s spot_failed=%s",
                    spot_result.ok,
                    spot_result.failed,
                )
            else:
                repair_result = await _run_stage("repair", repair_futures, log)

        # ── Nodo 4: backfill — sin dependencias (paralelo a spot/futures) ────
        # Descarga histórica completa de gaps largos.
        # Independiente de spot: usa cursores propios, storage propio.
        # Stub []: activa sin tocar nodos existentes cuando BackfillTask exista.
        backfill_result = await _run_stage("backfill", backfill_futures, log)

        # ── Nodo 5: reconciliation — PARTIAL_SUCCESS(spot) ───────────────────
        # Reconcilia Silver vs Bronze: detecta registros huérfanos.
        # Dependencia semántica: necesita datos escritos por spot.
        # Stub []: activa sin tocar nodos existentes cuando ReconciliationTask exista.
        reconciliation_result = _StageResult()
        if reconciliation_futures:
            if not spot_result.has_success() and spot_futures:
                log.warning(
                    "Graph node: reconciliation skipped"
                    " — spot failed completely, no data to reconcile"
                    " | spot_ok=%s spot_failed=%s",
                    spot_result.ok,
                    spot_result.failed,
                )
            else:
                reconciliation_result = await _run_stage(
                    "reconciliation", reconciliation_futures, log
                )

        # ── Nodo 6: validation — PARTIAL_SUCCESS(spot OR futures) ───────────
        # Valida integridad de Silver post-ingestion (schema, gaps, freshness).
        # Depende de que haya escrito algo — spot o futures.
        # Stub []: activa sin tocar nodos existentes cuando ValidationTask exista.
        validation_result = _StageResult()
        any_data_written = spot_result.has_success() or futures_result.has_success()
        if validation_futures:
            if not any_data_written:
                log.warning(
                    "Graph node: validation skipped"
                    " — no data written by spot or futures"
                )
            else:
                validation_result = await _run_stage(
                    "validation", validation_futures, log
                )

        # ── Nodo 7: feature_eng — PARTIAL_SUCCESS(spot) ─────────────────────
        # Computa features desde Silver (EMA, RSI, vol) hacia feature store.
        # Dependencia semántica: necesita Silver escrito por spot.
        # Stub []: activa sin tocar nodos existentes cuando FeatureEngTask exista.
        feature_eng_result = _StageResult()
        if feature_eng_futures:
            if not spot_result.has_success() and spot_futures:
                log.warning(
                    "Graph node: feature_eng skipped"
                    " — spot failed completely, no features to compute"
                    " | spot_ok=%s spot_failed=%s",
                    spot_result.ok,
                    spot_result.failed,
                )
            else:
                feature_eng_result = await _run_stage(
                    "feature_eng", feature_eng_futures, log
                )

        # ── Agregación final — SSOT + OCP ────────────────────────────────────
        # OCP: añadir nodo N = _StageResult() + _run_stage() + .merge()
        # SSOT: un único punto de suma — sin acumuladores dispersos.
        final_result = (
            spot_result
            .merge(futures_result)
            .merge(repair_result)
            .merge(backfill_result)
            .merge(reconciliation_result)
            .merge(validation_result)
            .merge(feature_eng_result)
        )
        ok     = final_result.ok
        failed = final_result.failed

        if failed > 0 and ok == 0:
            raise RuntimeError(f"All {failed} pipelines failed — aborting flow.")
    finally:
        # ── Guard teardown (solo si fue creado por el flow, no por entrypoint) ──
        _flow_guard = guard_context.get_guard()
        if _flow_guard is not None:
            _flow_guard.stop()
            guard_context.set_guard(None)

        # ── Cerrar adapters de validación ────────────────────────────────────
        for probe in probes:
            if probe.adapter is not None:
                try:
                    await probe.adapter.close()
                except Exception as _close_exc:
                    pass  # best-effort — no ocultar errores del flow
                finally:
                    probe.adapter = None

        # ── Push métricas por exchange ────────────────────────────────────────
        _pusher: MetricsPusherPort = pusher if pusher is not None else _NoopMetricsPusher()
        if config.observability.metrics.enabled:
            for probe in probes:
                _pusher.push({"exchange": probe.exchange, "gateway": _PUSHGATEWAY})
                log.info("Metrics pushed | exchange=%s", probe.exchange)
        else:
            log.debug("metrics_push_skipped", reason="metrics.enabled=false")

        # ── Flow summary ──────────────────────────────────────────────────────
        duration_s = time.monotonic() - flow_start
        flow_status = "OK" if failed == 0 else ("PARTIAL" if ok > 0 else "FAILED")
        log.info(
            "══ FLOW SUMMARY | %s | env=%s exchanges=%s/%s "
            "pipelines=%s/%s duration=%.1fs ══",
            flow_status,
            env,
            len(probes),
            len(config.exchanges),
            ok,
            len(pipeline_futures),
            duration_s,
        )

    if failed == 0:
        log.info("Market data flow completed successfully.")
