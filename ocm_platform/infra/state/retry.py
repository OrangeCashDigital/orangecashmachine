# -*- coding: utf-8 -*-
"""
ocm_platform/infra/state/retry.py
===================================

Resiliencia síncrona ante fallos transitorios de Redis.

Responsabilidad
---------------
Proveer redis_retry: un helper de reintentos con backoff exponencial
para operaciones Redis síncronas que pueden fallar de forma transitoria
(ConnectionError, TimeoutError).

Por qué existe este módulo
--------------------------
La lógica de retry es transversal al subsistema infra/state: la necesitan
cursor_store, gap_registry y lateness_calibration, pero no pertenece
semánticamente a ninguno de ellos.

Vivir en retry.py garantiza:
  - Una sola implementación (SSOT / DRY)
  - Sin acoplamiento accidental entre bounded contexts
  - Testeable de forma aislada

Consumidores
------------
  cursor_store.py         — _retry delega aquí (SSOT)
  gap_registry.py         — todas las ops store
  lateness_calibration.py — todas las ops store

Política de reintentos
----------------------
  - Solo ConnectionError y TimeoutError son recuperables (fallos transitorios).
  - AuthenticationError, ResponseError → no reintenta (Fail-Fast).
  - Backoff exponencial: base_ms × 2^attempt (50ms, 100ms, 200ms por defecto).
  - Agotados los intentos: relanza la última excepción (Fail-Fast explícito).

Principios
----------
SRP      — única responsabilidad: resiliencia síncrona Redis
SSOT     — una implementación, cero duplicaciones
Fail-Fast — lanza inmediatamente ante errores no recuperables
SafeOps  — los callers envuelven en try/except y degradan si necesario
KISS     — sin clases, sin decoradores, sin estado
"""
from __future__ import annotations

import time
from typing import Callable, TypeVar

import redis
from loguru import logger

# ---------------------------------------------------------------------------
# Constantes de resiliencia
# ---------------------------------------------------------------------------
_RETRY_ATTEMPTS: int   = 3
_RETRY_BASE_MS:  float = 50.0  # ms — backoff: 50 → 100 → 200 ms

_T = TypeVar("_T")


def redis_retry(
    fn:       Callable[[], _T],
    attempts: int   = _RETRY_ATTEMPTS,
    base_ms:  float = _RETRY_BASE_MS,
) -> _T:
    """
    Ejecuta ``fn`` con reintentos exponenciales ante fallos transitorios Redis.

    Solo para contextos síncronos (healthcheck, workers, tests).
    En contextos async usar la variante async en cursor_store (_retry_async).

    Política
    --------
    - Reintenta únicamente ConnectionError y TimeoutError (fallos transitorios).
    - Backoff exponencial: base_ms × 2^attempt milisegundos.
    - Lanza la última excepción si se agotan los intentos (Fail-Fast).

    Parameters
    ----------
    fn       : callable sin argumentos que ejecuta la operación Redis.
    attempts : número máximo de intentos (default 3).
    base_ms  : tiempo base de espera en ms (default 50 ms).

    Raises
    ------
    redis.exceptions.ConnectionError | redis.exceptions.TimeoutError
        Si se agotan los reintentos.

    Examples
    --------
    >>> value = redis_retry(lambda: client.get("my:key"))
    """
    last_exc: Exception = RuntimeError("redis_retry: no attempts made")
    for attempt in range(attempts):
        try:
            return fn()
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
            last_exc = exc
            if attempt < attempts - 1:
                wait_s = (base_ms * (2 ** attempt)) / 1000.0
                logger.warning(
                    "Redis retry {}/{} in {:.0f}ms | error={}",
                    attempt + 1, attempts, base_ms * (2 ** attempt), exc,
                )
                time.sleep(wait_s)
    raise last_exc
