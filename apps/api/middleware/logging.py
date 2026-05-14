# ==============================================================================
# api/middleware/logging.py — Request/Response logging → loguru
# ==============================================================================
#
# Loguea cada request con: método, path, status, duración, ip.
# Integrado con el pipeline loguru existente en ocm_platform/observability/.
#
# No loguea bodies — SafeOps: evitar filtrar datos sensibles en logs.
# Authorization header nunca se loguea — sanitizado explícitamente.
#
# Principios: SRP · SafeOps · KISS
# ==============================================================================
from __future__ import annotations

import time

from fastapi import Request, Response
from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware

# Paths excluidos del log — ruido sin valor operacional
_SILENT_PATHS: frozenset[str] = frozenset({"/health", "/ready", "/metrics"})


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Loguea request + response con duración. Silent para health checks."""

    async def dispatch(self, request: Request, call_next) -> Response:
        if request.url.path in _SILENT_PATHS:
            return await call_next(request)

        start   = time.perf_counter()
        ip      = request.client.host if request.client else "unknown"
        method  = request.method
        path    = request.url.path

        response = await call_next(request)

        elapsed_ms = (time.perf_counter() - start) * 1000
        status     = response.status_code

        level = "WARNING" if status >= 400 else "INFO"
        logger.log(
            level,
            "http_request | method={} path={} status={} duration_ms={:.1f} ip={}",
            method, path, status, elapsed_ms, ip,
        )
        return response
