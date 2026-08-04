# ==============================================================================
# api/middleware/rate_limit.py — Sliding window rate limiter (Redis)
# ==============================================================================
#
# Algoritmo: sliding window con sorted set de Redis.
# Clave: "rl:{ip}" — TTL automático = ventana_segundos.
# Decisión de diseño: IP-based (no user-based) en este nivel.
#   Razón: protege contra DoS antes de que llegue a auth.
#   Para rate limiting por usuario autenticado → implementar en routers.
#
# Fail-Soft: si Redis no responde, la request PASA (degraded mode).
#   Razón: mejor disponibilidad que un 503 por fallo de infra secundaria.
#   SafeOps: loguea el fallo — nunca silencioso.
#
# H6: /health, /ready y /metrics están excluidos (SILENT_PATHS) — las probes
# de infraestructura no representan carga de cliente y no deben poder degradar
# la rotación del nodo.
#
# H5 (limitación conocida — documentada, no corregida): la clave es
# request.client.host. Detrás de un reverse proxy / load balancer todo el
# tráfico comparte la IP del proxy → el límite se vuelve global. No hay proxy
# en el despliegue actual; si aparece, configurar ProxyHeadersMiddleware
# (confianza del header a decidir en infraestructura) o clavar por
# X-Forwarded-For de primer hop / identidad autenticada.
#
# Principios: SRP · Fail-Soft · SafeOps · KISS
# ==============================================================================
from __future__ import annotations

import time

import redis.asyncio as aioredis
from fastapi import Request, Response, status
from fastapi.responses import JSONResponse
from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware

from api.middleware import SILENT_PATHS


class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Sliding window rate limiter basado en Redis sorted sets.

    Ventana: 60 segundos.
    Clave por IP — degraded mode si Redis no responde.
    Excluye SILENT_PATHS (probes de observabilidad).
    """

    _WINDOW_S: int = 60  # ventana deslizante en segundos

    def __init__(self, app, redis_client: aioredis.Redis, rpm: int) -> None:
        super().__init__(app)
        self._redis = redis_client
        self._rpm = rpm

    async def dispatch(self, request: Request, call_next) -> Response:
        if request.url.path in SILENT_PATHS:
            return await call_next(request)

        ip = request.client.host if request.client else "unknown"
        key = f"rl:{ip}"
        now = time.time()

        try:
            pipe = self._redis.pipeline()
            # Eliminar entradas fuera de la ventana
            pipe.zremrangebyscore(key, 0, now - self._WINDOW_S)
            # Contar requests en ventana actual
            pipe.zcard(key)
            # Registrar esta request
            pipe.zadd(key, {str(now): now})
            # TTL = ventana (auto-limpieza)
            pipe.expire(key, self._WINDOW_S)
            results = await pipe.execute()

            count: int = results[1]  # zcard antes del zadd actual

            if count >= self._rpm:
                logger.warning(
                    "rate_limit_exceeded | ip={} count={} limit={}",
                    ip,
                    count,
                    self._rpm,
                )
                return JSONResponse(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    content={
                        "detail": "Rate limit excedido",
                        "retry_after_seconds": self._WINDOW_S,
                    },
                    headers={"Retry-After": str(self._WINDOW_S)},
                )

        except Exception as exc:
            # Fail-Soft: Redis down no debe bloquear el servicio
            logger.warning("rate_limit_redis_error (degraded) | ip={} error={}", ip, exc)

        return await call_next(request)
