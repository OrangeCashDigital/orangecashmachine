# ==============================================================================
# api/routers/health.py — Health & Readiness endpoints
# ==============================================================================
#
# GET /health  → liveness:  ¿está el proceso vivo?     (siempre 200)
# GET /ready   → readiness: ¿puede servir peticiones?  (200 | 503)
#
# Diseño deliberado:
#   /health   — NO toca dependencias externas. Solo responde si el proceso vive.
#               Usado por systemd / Docker HEALTHCHECK.
#   /ready    — Verifica Redis. Si Redis no responde → 503.
#               Usado por load balancer para sacar el nodo de rotación.
#
# Sin auth — estos endpoints deben ser accesibles por infra sin token.
# Sin rate limit — el middleware los excluye (_SILENT_PATHS).
#
# Principios: SRP · Fail-Fast · KISS · SafeOps
# ==============================================================================
from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, status
from fastapi.responses import JSONResponse
from loguru import logger

from api.deps import RedisDep

router = APIRouter(tags=["observability"])

# Registrado en startup por main.py
_startup_time: datetime | None = None


def set_startup_time(t: datetime) -> None:
    """Llamado desde lifespan para registrar el momento de arranque."""
    global _startup_time
    _startup_time = t


@router.get(
    "/health",
    summary="Liveness probe",
    response_description="El proceso está vivo",
)
async def health() -> JSONResponse:
    """
    Liveness probe — nunca falla mientras el proceso esté activo.
    No verifica dependencias externas.
    """
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "status":   "ok",
            "uptime_s": _uptime_seconds(),
        },
    )


@router.get(
    "/ready",
    summary="Readiness probe",
    response_description="El servicio puede atender peticiones",
)
async def ready(redis: RedisDep) -> JSONResponse:
    """
    Readiness probe — verifica Redis.
    503 si Redis no responde: el load balancer debe sacar este nodo.
    """
    try:
        await redis.ping()
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"status": "ready", "redis": "ok"},
        )
    except Exception as exc:
        logger.warning("readiness_check_failed | error={}", exc)
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "not_ready", "redis": "unreachable"},
        )


# ---------------------------------------------------------------------------
# Helpers privados
# ---------------------------------------------------------------------------

def _uptime_seconds() -> float:
    if _startup_time is None:
        return 0.0
    return (datetime.now(timezone.utc) - _startup_time).total_seconds()
