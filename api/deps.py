# ==============================================================================
# api/deps.py — Dependency injection (FastAPI Depends)
# ==============================================================================
#
# SSOT de todas las dependencias compartidas entre routers.
# Ningún router instancia recursos directamente — todo pasa por aquí.
#
# Principios: DIP · SRP · SSOT · Fail-Fast
# ==============================================================================
from __future__ import annotations

from functools import lru_cache
from typing import Annotated

import redis.asyncio as aioredis
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from api.auth.jwt import verify_token
from api.settings import ApiSettings, get_settings

_bearer = HTTPBearer(auto_error=True)


# ---------------------------------------------------------------------------
# Settings — singleton cacheado
# ---------------------------------------------------------------------------



SettingsDep = Annotated[ApiSettings, Depends(get_settings)]


# ---------------------------------------------------------------------------
# Redis — pool async compartido
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _redis_pool(redis_url: str) -> aioredis.Redis:
    """
    Crea el pool Redis una sola vez (singleton por URL).
    decode_responses=True: strings nativos, sin bytes en los handlers.
    """
    return aioredis.from_url(
        redis_url,
        decode_responses=True,
        socket_connect_timeout=2,
        socket_timeout=2,
        retry_on_timeout=False,   # Fail-Fast: no reintentar silenciosamente
    )


async def get_redis(settings: SettingsDep) -> aioredis.Redis:
    """
    Dependencia de Redis.
    Fail-Fast: si Redis no responde en startup, el proceso debe fallar,
    no servir peticiones con estado degradado sin aviso.
    """
    return _redis_pool(settings.redis_url)


RedisDep = Annotated[aioredis.Redis, Depends(get_redis)]


# ---------------------------------------------------------------------------
# Auth — verificación JWT
# ---------------------------------------------------------------------------

async def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(_bearer)],
    settings: SettingsDep,
) -> dict:
    """
    Verifica el JWT Bearer token.
    Fail-Fast: 401 inmediato si el token es inválido o expirado.
    No lanza 500 — los errores de auth son siempre 401/403.
    """
    payload = verify_token(credentials.credentials, settings.jwt_secret)
    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido o expirado",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return payload


CurrentUserDep = Annotated[dict, Depends(get_current_user)]
