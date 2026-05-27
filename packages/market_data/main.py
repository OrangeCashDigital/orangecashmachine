# -*- coding: utf-8 -*-
"""
market_data/main.py
====================

Entrypoint del market-data-service como proceso independiente.

Responsabilidad
---------------
Exponer market_data como microservicio autónomo:
  - Pipeline de ingesta corriendo en background loop (asyncio)
  - HTTP API mínima: /health, /ready, /ohlcv/{exchange}/{symbol}/{timeframe}

Arquitectura
------------
  ┌─────────────────────────────────────────┐
  │         market-data-service :8001        │
  │                                          │
  │  FastAPI ─── lifespan ──► ingestion loop │
  │                │                         │
  │          GET /health                     │
  │          GET /ready                      │
  │          GET /ohlcv/{exch}/{sym}/{tf}    │
  └─────────────────────────────────────────┘

Principios: SRP · KISS · SafeOps · DIP
No importa nada de trading/ ni portfolio/ — boundary explícito.

Uso
---
    python -m market_data.main
    python market_data/main.py
    uvicorn market_data.main:app --host 0.0.0.0 --port 8001
"""

from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from ocm.config import env_vars
from ocm.observability import bind_pipeline, bootstrap_logging, configure_logging
from ocm.runtime import guard_context
from ocm.runtime.context import RuntimeContext
from ocm.runtime.guard import ExecutionGuard, ExecutionStoppedError


def build_context() -> RuntimeContext:
    """
    Construye RuntimeContext standalone — sin Hydra, sin CLI.

    Usa load_appconfig_standalone() (mismo path que OCMResource en Dagster).
    SSOT: RunConfig.from_env() resuelve OCM_ENV, OCM_DEBUG, run_id.

    Fail-Fast: lanza ConfigurationError si AppConfig es inválido.
    """
    from datetime import datetime, timezone

    from ocm.config.hydra_loader import load_appconfig_standalone
    from ocm.runtime.run_config import RunConfig

    run_cfg = RunConfig.from_env()
    app_cfg = load_appconfig_standalone(env=run_cfg.env)
    return RuntimeContext(
        app_config=app_cfg,
        run_config=run_cfg,
        started_at=datetime.now(timezone.utc),
    )


_log = bind_pipeline("market_data.main")

# ---------------------------------------------------------------------------
# Configuración del servicio — leída de env vars con defaults sensatos
# ---------------------------------------------------------------------------

_HOST: str = os.getenv(env_vars.MARKET_DATA_HOST, "0.0.0.0")  # nosec B104 — bind a todas las interfaces intencional en contenedor Docker
_PORT: int = int(os.getenv(env_vars.MARKET_DATA_PORT, "8001"))
_INGESTION_INTERVAL_S: float = float(os.getenv(env_vars.INGESTION_INTERVAL_S, "300"))  # 5 min entre runs
_SERVICE_NAME: str = "market-data-service"
_VERSION: str = "1.0.0"


# ---------------------------------------------------------------------------
# Estado global del servicio — mutable solo desde el lifespan
# ---------------------------------------------------------------------------


if TYPE_CHECKING:  # pragma: no cover
    from market_data.ports.outbound.storage_factory import StorageFactoryPort


class _ServiceState:
    """Estado mutable del proceso. Un único instancia por proceso (singleton)."""

    guard: Optional[ExecutionGuard] = None
    ctx: Optional[RuntimeContext] = None
    storage_factory: Optional["StorageFactoryPort"] = None  # inyectado en lifespan
    started: float = 0.0
    healthy: bool = False
    last_run: Optional[float] = None
    last_result: str = "pending"


_state = _ServiceState()


# ---------------------------------------------------------------------------
# Ingestion loop — corre en background sin bloquear FastAPI
# ---------------------------------------------------------------------------


async def _ingestion_loop(ctx: RuntimeContext, guard: ExecutionGuard) -> None:
    """
    Loop de ingestión: ejecuta market_data_flow repetidamente.

    Diseño
    ------
    - Cada iteración lanza el flow completo (backfill + incremental + repair).
    - Espera _INGESTION_INTERVAL_S entre runs.
    - guard.should_stop() se verifica antes de cada run y en cada sleep.
    - CancelledError re-lanzado — permite shutdown limpio vía lifespan.

    SafeOps
    -------
    - Excepciones del flow se capturan — el loop nunca muere por 1 run fallido.
    - guard.record_error() activa kill switch si hay errores consecutivos.
    """
    log = _log.bind(component="ingestion_loop")
    log.info("ingestion_loop_started", interval_s=_INGESTION_INTERVAL_S)

    # Imports lazy — application layer depende de infra, no al revés (DIP)
    from market_data.application.use_cases.pipeline_orchestrator import (
        PipelineOrchestrator,
        PipelineRequest,
    )

    orchestrator = PipelineOrchestrator()
    app_cfg = ctx.app_config

    while not guard.should_stop():
        run_start = time.monotonic()
        log.info("ingestion_run_starting")

        try:
            guard.check()

            # Ejecutar pipeline por cada exchange configurado
            for exc_name in app_cfg.exchange_names:
                exc_cfg = app_cfg.get_exchange(exc_name)
                if exc_cfg is None:
                    continue

                for market_type, symbols in [
                    ("spot", getattr(exc_cfg.markets, "spot_symbols", [])),
                    ("futures", getattr(exc_cfg.markets, "futures_symbols", [])),
                ]:
                    if not symbols:
                        continue

                    request = PipelineRequest(
                        exchange=exc_name,
                        market_type=market_type,
                        pipeline="ohlcv",
                        mode="incremental",
                        credentials=exc_cfg.ccxt_credentials(),
                        resilience=exc_cfg.resilience,
                        symbols=symbols,
                        timeframes=app_cfg.pipeline.historical.timeframes,
                        start_date=app_cfg.pipeline.historical.start_date,
                        auto_lookback_days=app_cfg.pipeline.historical.auto_lookback_days,
                        run_id=ctx.run_id,
                        dry_run=app_cfg.safety.dry_run,
                    )
                    await orchestrator.run(request)

            guard.record_success()
            _state.last_result = "success"
            log.info(
                "ingestion_run_completed",
                duration_s=round(time.monotonic() - run_start, 1),
            )

        except ExecutionStoppedError:
            log.warning("ingestion_stopped_by_guard")
            break

        except asyncio.CancelledError:
            raise  # shutdown limpio — no capturar

        except Exception as exc:
            guard.record_error(reason=str(exc))
            _state.last_result = f"error:{type(exc).__name__}"
            log.opt(exception=True).error(
                "ingestion_run_failed",
                error_type=type(exc).__name__,
                error=str(exc),
            )

        _state.last_run = time.monotonic()

        # Sleep interruptible — cancela inmediatamente en shutdown
        try:
            await asyncio.sleep(_INGESTION_INTERVAL_S)
        except asyncio.CancelledError:
            raise

    log.info("ingestion_loop_stopped", reason=guard.stop_reason)


async def _bronze_writer_loop() -> None:
    """
    Loop permanente del KafkaBronzeWriter — Kappa stream processor.

    Responsabilidad: consumir ohlcv.raw → escribir a Bronze Iceberg.
    Corre en paralelo al _ingestion_loop — son dos tareas independientes.

    Ciclo de vida:
      start() → run() [loop poll/process/commit] → stop() en CancelledError

    SafeOps:
      - start() falla → log error + return (no muere el proceso)
      - Errores en run() → KafkaBronzeWriter los captura internamente
      - CancelledError → stop() + return (shutdown limpio)

    Degraded mode:
      Si Kafka no está disponible (broker down), start() falla y el loop
      retorna inmediatamente. El _ingestion_loop sigue corriendo en modo
      degradado (escribe directo a Iceberg via ctx.kafka_producer=None path).
    """
    log = _log.bind(component="bronze_writer_loop")
    log.info("bronze_writer_loop_starting")

    # Imports lazy — infra no se importa en module level (DIP · startup cost)
    try:
        from market_data.infrastructure.kafka.bronze_writer import KafkaBronzeWriter
        from market_data.infrastructure.kafka.consumer import KafkaConsumerAdapter
        from market_data.infrastructure.kafka.producer import KafkaProducerAdapter
        from market_data.infrastructure.storage.bronze.bronze_storage import (
            BronzeStorage,
        )
    except ImportError as exc:
        log.error("bronze_writer_loop_import_error", error=str(exc))
        return

    # Construir dependencias — BronzeStorage sin exchange fijo:
    # el exchange se extrae del EventPayload en cada mensaje (SSOT del wire format).
    bronze = BronzeStorage()  # exchange=None → se setea por mensaje
    consumer = KafkaConsumerAdapter.for_bronze_writer()

    # DLQ producer — opcional, SafeOps: None = mensajes malos solo se loguean
    try:
        dlq_producer = KafkaProducerAdapter.from_env()
    except Exception as exc:
        log.warning("bronze_writer_dlq_producer_failed", error=str(exc))
        dlq_producer = None

    writer = KafkaBronzeWriter(
        consumer=consumer,
        bronze_storage=bronze,
        dlq_producer=dlq_producer,  # type: ignore[arg-type]  # KafkaProducerAdapter implementa KafkaProducerPort
    )

    try:
        await writer.start()
        log.info("bronze_writer_loop_started — consuming ohlcv.raw → Bronze")
        await writer.run()
    except asyncio.CancelledError:
        log.info("bronze_writer_loop_cancelled — stopping")
        await writer.stop()
    except Exception as exc:
        log.error("bronze_writer_loop_error", error=str(exc))
        try:
            await writer.stop()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Lifespan — startup / shutdown del servicio
# ---------------------------------------------------------------------------


@asynccontextmanager
async def _lifespan(app: FastAPI):
    """
    Ciclo de vida de FastAPI.

    Startup
    -------
    1. Bootstrap logging
    2. Construir RuntimeContext (config + run_id)
    3. Inicializar ExecutionGuard
    4. Lanzar ingestion loop como background task

    Shutdown
    --------
    1. Señalizar guard → stop
    2. Cancelar background task
    3. Esperar cleanup
    """
    # ── Startup ──────────────────────────────────────────────────────────────
    # _state: objeto mutable de proceso — no necesita `global` porque no
    # reasignamos la variable, solo mutamos sus atributos.
    _log.info("service_starting", service=_SERVICE_NAME, version=_VERSION)

    ctx = build_context()
    bootstrap_logging(debug=ctx.debug, env=ctx.environment)
    configure_logging(
        cfg=ctx.app_config.observability.logging,
        env=ctx.environment,
        debug=ctx.debug,
    )

    guard = ExecutionGuard(
        max_errors=ctx.app_config.pipeline.max_consecutive_errors,
        max_runtime_s=None,  # microservicio: sin límite — corre indefinidamente
    )
    guard.start()
    guard_context.set_guard(guard)

    # ── Wiring de adaptadores outbound (composition root — DIP) ─────────────
    # StorageFactoryPort: una factory cacheada por (exchange, market_type).
    # Ningún handler instancia adaptadores — todos consumen el port (DIP).
    from market_data.adapters.outbound.storage.iceberg_factory import (
        IcebergStorageFactory,
    )

    _state.storage_factory = IcebergStorageFactory()

    _state.guard = guard
    _state.ctx = ctx
    _state.started = time.monotonic()
    _state.healthy = True

    ingestion_task = asyncio.create_task(
        _ingestion_loop(ctx, guard),
        name="market_data_ingestion",
    )

    # Kappa: KafkaBronzeWriter corre en paralelo — consume ohlcv.raw → Bronze.
    # Independiente del ingestion_loop — son bounded contexts distintos.
    # SafeOps: si Kafka no está disponible, el loop retorna sin matar el proceso.
    bronze_writer_task = asyncio.create_task(
        _bronze_writer_loop(),
        name="kafka_bronze_writer",
    )

    # Kappa: FeedOrchestrator — WS feeds → Kafka trades.raw.
    # Fail-Soft: build_feed_orchestrator retorna None si feeds.yaml tiene
    # ingestion_mode=rest o no hay feeds habilitados. En ese caso no se lanza
    # ninguna task y el sistema opera en modo REST polling solamente.
    feed_orchestrator_task = None
    try:
        from market_data.infrastructure.bootstrap.composition_root import (
            CompositionRoot,
        )

        _feed_orch = CompositionRoot.build_feed_orchestrator(ctx.app_config)
        if _feed_orch is not None:
            feed_orchestrator_task = asyncio.create_task(
                _feed_orch.run(),
                name="feed_orchestrator",
            )
            _log.info(
                "feed_orchestrator_started",
                mode=getattr(_feed_orch._config, "ingestion_mode", "unknown"),
            )
        else:
            _log.info("feed_orchestrator_skipped — ingestion_mode=rest or no feeds enabled")
    except Exception as _orch_exc:
        # Fail-Soft: el orquestador WS no es crítico para el pipeline REST.
        # Si falla al construir, loguear y continuar. El servicio sigue operativo.
        _log.warning(
            "feed_orchestrator_build_failed — operating in REST-only mode",
            error=str(_orch_exc),
        )

    _log.info(
        "service_started",
        service=_SERVICE_NAME,
        env=ctx.environment,
        run_id=ctx.run_id,
        port=_PORT,
    )

    try:
        yield  # FastAPI sirve peticiones aquí

    finally:
        # ── Shutdown ──────────────────────────────────────────────────────────
        _log.info("service_stopping", service=_SERVICE_NAME)
        _state.healthy = False

        guard.trigger("service_shutdown")
        guard_context.set_guard(None)

        for task in (ingestion_task, bronze_writer_task, feed_orchestrator_task):
            if task is not None and not task.done():
                task.cancel()
                try:
                    # FIX C-05: shield+wait_for creaba tasks zombie.
                    # shield() previene la cancelación pero wait_for igual
                    # lanza TimeoutError y la task shielded sigue corriendo.
                    # Correcto: cancel() + wait_for sin shield — la task recibe
                    # CancelledError y puede hacer cleanup en su try/finally.
                    await asyncio.wait_for(task, timeout=10.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass

        _log.info(
            "service_stopped",
            service=_SERVICE_NAME,
            uptime_s=round(time.monotonic() - _state.started, 1),
        )


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title=_SERVICE_NAME,
    version=_VERSION,
    description="OrangeCashMachine — Market Data Ingestion Service",
    lifespan=_lifespan,
)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get("/health", tags=["ops"])
async def health() -> JSONResponse:
    """
    Liveness probe.

    Retorna 200 si el proceso está vivo y el guard no ha sido activado.
    Kubernetes usa este endpoint para reiniciar el pod si falla.
    """
    guard = _state.guard
    if not _state.healthy or (guard is not None and guard.should_stop()):
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "reason": guard.stop_reason if guard else "not_started",
                "service": _SERVICE_NAME,
            },
        )
    return JSONResponse(
        status_code=200,
        content={
            "status": "healthy",
            "service": _SERVICE_NAME,
            "version": _VERSION,
            "uptime_s": round(time.monotonic() - _state.started, 1),
            "last_result": _state.last_result,
        },
    )


@app.get("/ready", tags=["ops"])
async def ready() -> JSONResponse:
    """
    Readiness probe.

    Retorna 200 si el servicio puede aceptar tráfico:
    - Config cargada
    - Primera ejecución iniciada (last_run no None) O primera aún en curso

    Kubernetes usa este endpoint para controlar el routing de tráfico.
    """
    if _state.ctx is None:
        return JSONResponse(
            status_code=503,
            content={"status": "not_ready", "reason": "context_not_initialized"},
        )
    return JSONResponse(
        status_code=200,
        content={
            "status": "ready",
            "service": _SERVICE_NAME,
            "env": _state.ctx.environment,
            "last_run_s": (round(time.monotonic() - _state.last_run, 1) if _state.last_run else None),
        },
    )


@app.get("/ohlcv/{exchange}/{symbol}/{timeframe}", tags=["data"])
async def get_ohlcv(
    exchange: str,
    symbol: str,
    timeframe: str,
    start: Optional[str] = Query(default=None, description="ISO 8601 UTC start"),
    end: Optional[str] = Query(default=None, description="ISO 8601 UTC end"),
    limit: int = Query(default=500, ge=1, le=10_000),
) -> JSONResponse:
    """
    Lee OHLCV desde Silver (Iceberg).

    La red es la frontera — trading-service y portfolio-service
    llaman este endpoint en lugar de importar IcebergStorage directamente.

    Parameters
    ----------
    exchange  : bybit | kucoin | kucoinfutures
    symbol    : BTC%2FUSDT  (URL-encode la barra)
    timeframe : 1m | 5m | 15m | 1h | 4h | 1d
    start     : ISO 8601 UTC, ej: 2024-01-01T00:00:00Z
    end       : ISO 8601 UTC
    limit     : máximo de velas a retornar (default 500)

    Returns
    -------
    JSON con lista de candles: [{timestamp, open, high, low, close, volume}]
    """
    import pandas as pd

    # Normalizar symbol (el router puede URL-decodear %2F → /)
    symbol_norm = symbol.replace("%2F", "/").replace("%2f", "/")

    try:
        # StorageFactoryPort inyectado en startup — DIP.
        # get_storage(exchange) retorna instancia cacheada o crea una nueva.
        if _state.storage_factory is None:
            raise RuntimeError("storage_factory_not_initialized")
        storage = _state.storage_factory.get_storage(
            exchange=exchange,
            market_type="spot",  # TODO: exponer como query param si se necesita futures
        )

        ts_start = pd.Timestamp(start, tz="UTC") if start else None
        ts_end = pd.Timestamp(end, tz="UTC") if end else None

        df = await asyncio.to_thread(
            storage.load_ohlcv,
            symbol=symbol_norm,
            timeframe=timeframe,
            start=ts_start,
            end=ts_end,
        )

        if df is None or df.empty:
            return JSONResponse(
                status_code=404,
                content={
                    "detail": "no_data",
                    "exchange": exchange,
                    "symbol": symbol_norm,
                    "timeframe": timeframe,
                },
            )

        # Aplicar limit después de filtros temporales
        df = df.tail(limit)

        records = df.assign(timestamp=df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")).to_dict(orient="records")

        return JSONResponse(
            status_code=200,
            content={
                "exchange": exchange,
                "symbol": symbol_norm,
                "timeframe": timeframe,
                "count": len(records),
                "data": records,
            },
        )

    except Exception as exc:
        _log.opt(exception=True).error(
            "ohlcv_endpoint_error",
            exchange=exchange,
            symbol=symbol_norm,
            timeframe=timeframe,
            error=str(exc),
        )
        raise HTTPException(status_code=500, detail=f"storage_error:{type(exc).__name__}")


# _get_storage() eliminado — IcebergStorage se instancia en lifespan (composition root).
# El handler get_ohlcv consume OHLCVStorage via _state.storage (DIP · SRP · Hexagonal).


# ---------------------------------------------------------------------------
# __main__ — ejecución directa
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    uvicorn.run(
        "market_data.main:app",
        host=_HOST,
        port=_PORT,
        log_level="info",
        # reload=False en producción — reload no es compatible con background tasks
        reload=False,
        # workers=1: el ingestion loop usa asyncio — no es fork-safe con >1 workers
        workers=1,
    )
