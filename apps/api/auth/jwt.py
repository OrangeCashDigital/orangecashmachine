# ==============================================================================
# api/auth/jwt.py — JWT sign / verify
# ==============================================================================
#
# SRP: este módulo solo firma y verifica JWTs.
# No gestiona usuarios, sesiones ni permisos — eso va en routers/auth.py
# cuando se implemente.
#
# Algoritmo: HS256 (simétrico) — apropiado para sistema single-operator.
# Si OCM escala a multi-tenant, migrar a RS256 (asimétrico).
#
# Principios: SRP · Fail-Fast · KISS
# ==============================================================================
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from jose import JWTError, jwt
from loguru import logger


def create_token(
    payload: dict[str, Any],
    secret: str,
    algorithm: str = "HS256",
    expire_minutes: int = 60,
) -> str:
    """
    Crea un JWT firmado con expiración.

    Args:
        payload:        Claims del token (sub, roles, etc.)
        secret:         Secreto de firma (SSOT: ApiSettings.jwt_secret)
        algorithm:      Algoritmo de firma (default HS256)
        expire_minutes: TTL del token en minutos

    Returns:
        Token JWT como string.
    """
    now = datetime.now(timezone.utc)
    data = {
        **payload,
        "iat": now,
        "exp": now + timedelta(minutes=expire_minutes),
    }
    return jwt.encode(data, secret, algorithm=algorithm)


def verify_token(
    token: str,
    secret: str,
    algorithm: str = "HS256",
) -> dict[str, Any] | None:
    """
    Verifica y decodifica un JWT.

    Fail-Soft en verificación: retorna None en lugar de lanzar,
    para que deps.py pueda construir un 401 limpio sin traceback.
    El error se loguea en DEBUG — no es un error de sistema.

    Args:
        token:     JWT a verificar
        secret:    Secreto de verificación
        algorithm: Algoritmo esperado

    Returns:
        Payload decodificado si el token es válido, None si no.
    """
    try:
        return jwt.decode(token, secret, algorithms=[algorithm])
    except JWTError as exc:
        logger.debug("jwt_verify_failed | reason={}", exc)
        return None
