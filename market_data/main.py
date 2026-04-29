# -*- coding: utf-8 -*-
"""
market_data/main.py
====================

Entrypoint del market-data-service como proceso independiente.

Responsabilidad
---------------
Exponer market_data como microservicio autónomo:
  - Pipeline de ingesta (Prefect flow) corriendo en background loop
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
import sys
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from ocm_platform.observability import bind_pipeline, bootstrap_logging, configure_logging
from ocm_platform.runtime.guard import ExecutionGuard, ExecutionStoppedError
from ocm_platform.runtime.run_config import RunConfig
from ocm_platform.runtime.context import RuntimeContext
from ocm_platform.config.hydra_loader import load_appconfig_standalone

from market_data.safety import guard_context
from market_data.orchestration.entrypoint import build_context

_log = bind_pipeline("market_data.main")

# ---------------------------------------------------------------------------
# Configuración del servicio — leída de env vars con defaults sensatos
# ---------------------------------------------------------------------------

_HOST:             str   = os.getenv("MARKET_DATA_HOST",          "0.0.0.0")
_PORT:             int   = int(os.getenv("MARKET_DATA_PORT",      "8001"))
_INGESTION_INTERVAL_S: float = float(os.getenv("INGESTION_INTERVAL_S", "300"))  # 5 min entre runs
_SERVICE_NAME:     str   = "market-data-service"
_VERSION:          str   = "1.0.0"


# ---------------------------------------------------------------------------
# Estado global del servicio — mutable solo desde el lifespan
# ---------------------------------------------------------------------------

class _ServiceState:
    """Estado mutable del proceso. Un único instancia por proceso (singleton)."""
    guard:    Optional[ExecutionGuard] = None
    ctx:      Optional[RuntimeContext] = None
    started:  float                    = 0.0
    healthy:  bool                     = False
    last_run: Optional[float]          = None
    last_result: str                   = "pending"

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
    from market_data.orchestration.flows.batch_flow import market_data_flow

    log = _log.bind(component="ingestion_loop")
    log.info("ingestion_loop_started", interval_s=_INGESTION_INTERVAL_S)

    while not guard.should_stop():
        run_start = time.monotonic()
        log.info("ingestion_run_starting")

        try:
            guard.check()
            await market_data_flow(runtime_context=ctx)
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
    global _state

    # ── Startup ──────────────────────────────────────────────────────────────
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

    _state.guard   = guard
    _state.ctx     = ctx
    _state.started = time.monotonic()
    _state.healthy = True

    ingestion_task = asyncio.create_task(
        _ingestion_loop(ctx, guard),
        name="market_data_ingestion",
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

        if not ingestion_task.done():
            ingestion_task.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(ingestion_task), timeout=10.0)
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
                "status":      "unhealthy",
                "reason":      guard.stop_reason if guard else "not_started",
                "service":     _SERVICE_NAME,
            },
        )
    return JSONResponse(
        status_code=200,
        content={
            "status":      "healthy",
            "service":     _SERVICE_NAME,
            "version":     _VERSION,
            "uptime_s":    round(time.monotonic() - _state.started, 1),
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
            "status":      "ready",
            "service":     _SERVICE_NAME,
            "env":         _state.ctx.environment,
            "last_run_s":  (
                round(time.monotonic() - _state.last_run, 1)
                if _state.last_run else None
            ),
        },
    )


@app.get("/ohlcv/{exchange}/{symbol}/{timeframe}", tags=["data"])
async def get_ohlcv(
    exchange:  str,
    symbol:    str,
    timeframe: str,
    start:     Optional[str] = Query(default=None, description="ISO 8601 UTC start"),
    end:       Optional[str] = Query(default=None, description="ISO 8601 UTC end"),
    limit:     int           = Query(default=500, ge=1, le=10_000),
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
    from market_data.storage.iceberg.iceberg_storage import IcebergStorage

    # Normalizar symbol (el router puede URL-decodear %2F → /)
    symbol_norm = symbol.replace("%2F", "/").replace("%2f", "/")

    try:
        # IcebergStorage satisface OHLCVStorage Protocol — DIP via port
        storage = await asyncio.to_thread(
            _get_storage, exchange, symbol_norm, timeframe
        )

        ts_start = pd.Timestamp(start, tz="UTC") if start else None
        ts_end   = pd.Timestamp(end,   tz="UTC") if end   else None

        df = await asyncio.to_thread(
            storage.load_ohlcv,
            symbol    = symbol_norm,
            timeframe = timeframe,
            start     = ts_start,
            end       = ts_end,
        )

        if df is None or df.empty:
            return JSONResponse(
                status_code=404,
                content={
                    "detail":    "no_data",
                    "exchange":  exchange,
                    "symbol":    symbol_norm,
                    "timeframe": timeframe,
                },
            )

        # Aplicar limit después de filtros temporales
        df = df.tail(limit)

        records = df.assign(
            timestamp=df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        ).to_dict(orient="records")

        return JSONResponse(
            status_code=200,
            content={
                "exchange":  exchange,
                "symbol":    symbol_norm,
                "timeframe": timeframe,
                "count":     len(records),
                "data":      records,
            },
        )

    except Exception as exc:
        _log.opt(exception=True).error(
            "ohlcv_endpoint_error",
            exchange=exchange, symbol=symbol_norm, timeframe=timeframe,
            error=str(exc),
        )
        raise HTTPException(status_code=500, detail=f"storage_error:{type(exc).__name__}")


def _get_storage(exchange: str, symbol: str, timeframe: str):
    """
    Instancia IcebergStorage para el par dado.

    Separado de la coroutine para ejecutarse en thread (I/O bloqueante).
    SafeOps: si no existe la tabla, load_ohlcv retorna None — no lanza.
    """
    from market_data.storage.iceberg.iceberg_storage import IcebergStorage
    return IcebergStorage(exchange=exchange, market_type="spot")


# ---------------------------------------------------------------------------
# __main__ — ejecución directa
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    uvicorn.run(
        "market_data.main:app",
        host       = _HOST,
        port       = _PORT,
        log_level  = "info",
        # reload=False en producción — reload no es compatible con background tasks
        reload     = False,
        # workers=1: el ingestion loop usa asyncio — no es fork-safe con >1 workers
        workers    = 1,
    )
