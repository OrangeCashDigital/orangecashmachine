# ==============================================================================
# api/main.py — FastAPI application factory + lifespan
# ==============================================================================
#
# Responsabilidades (SRP — una por función):
#   create_app()  — factory: monta middleware, routers, configura app
#   lifespan()    — gestiona recursos: startup + shutdown ordenados
#   serve()       — entrypoint CLI: uvicorn programático
#
# Orden de middleware (LIFO en ejecución — primero registrado = más externo):
#   1. RequestLoggingMiddleware   (más externo — loguea todo)
#   2. RateLimitMiddleware        (segundo — rechaza antes de procesar)
#
# Sin lógica de negocio aquí. Los routers delegan a app/use_cases/.
#
# Principios: SRP · SSOT · Fail-Fast · SafeOps · KISS
# ==============================================================================
from __future__ import annotations

import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import redis.asyncio as aioredis
import uvicorn
from fastapi import FastAPI
from loguru import logger

from api.deps import _redis_pool, get_settings
from api.middleware.logging import RequestLoggingMiddleware
from api.middleware.rate_limit import RateLimitMiddleware
from api.routers import health
from api.routers.health import set_startup_time


# ---------------------------------------------------------------------------
# Lifespan — startup / shutdown ordenados
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Ciclo de vida del proceso API.

    Startup  (antes de servir peticiones):
      1. Cargar settings — Fail-Fast si faltan vars requeridas
      2. Verificar conexión Redis — Fail-Fast si no responde
      3. Registrar startup time para /health

    Shutdown (después de SIGTERM):
      1. Cerrar pool Redis limpiamente
    """
    settings = get_settings()

    # ── Startup ───────────────────────────────────────────────────────
    logger.info(
        "api_startup | env={} host={}:{} rate_limit_rpm={}",
        settings.env, settings.host, settings.port, settings.rate_limit_rpm,
    )

    # Fail-Fast: verificar Redis antes de aceptar tráfico
    redis_client: aioredis.Redis = _redis_pool(settings.redis_url)
    try:
        await redis_client.ping()
        logger.info("api_redis_ok | url={}", settings.redis_url_sanitized)
    except Exception as exc:
        logger.critical("api_redis_unreachable | url={} error={}", settings.redis_url, exc)
        # Fail-Fast: sin Redis no hay rate limiting ni estado — no arrancar
        sys.exit(1)

    set_startup_time(datetime.now(timezone.utc))
    logger.info("api_ready")

    yield  # ── Sirve peticiones ──────────────────────────────────────

    # ── Shutdown ──────────────────────────────────────────────────────
    logger.info("api_shutdown")
    await redis_client.aclose()
    logger.info("api_redis_closed")


# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------

def create_app() -> FastAPI:
    """
    Factory de la aplicación FastAPI.

    Separar factory de entrypoint permite testear la app sin levantar
    un servidor real (httpx.AsyncClient(app=create_app())).
    """
    settings = get_settings()

    app = FastAPI(
        title="OrangeCashMachine API",
        description="Gateway de la plataforma de trading algorítmico OCM",
        version="0.1.0",
        docs_url="/docs"     if not settings.is_production else None,
        redoc_url="/redoc"   if not settings.is_production else None,
        openapi_url="/openapi.json" if not settings.is_production else None,
        lifespan=lifespan,
    )

    # ── Middleware (orden = LIFO en ejecución) ─────────────────────────
    redis_client = _redis_pool(settings.redis_url)

    app.add_middleware(
        RateLimitMiddleware,
        redis_client=redis_client,
        rpm=settings.rate_limit_rpm,
    )
    app.add_middleware(RequestLoggingMiddleware)

    # ── Routers ───────────────────────────────────────────────────────
    app.include_router(health.router)

    return app


# ---------------------------------------------------------------------------
# Entrypoint programático (ocm-api script en pyproject.toml)
# ---------------------------------------------------------------------------

def serve() -> None:
    """
    Entrypoint CLI — registrado como 'ocm-api' en pyproject.toml.
    Uvicorn programático: settings como SSOT, no flags de CLI.
    """
    settings = get_settings()
    uvicorn.run(
        "api.main:create_app",
        factory=True,
        host=settings.host,
        port=settings.port,
        reload=not settings.is_production,
        log_config=None,    # loguru es el logging SSOT — deshabilitar el de uvicorn
    )


# ---------------------------------------------------------------------------
# Desarrollo directo: python api/main.py
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    serve()
