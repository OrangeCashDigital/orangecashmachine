# -*- coding: utf-8 -*-
"""
ocm_platform/infra/state/utils.py
===================================

Utilidades transversales del subsistema infra/state.

Responsabilidad
---------------
Proveer helpers de uso general que varios módulos de infra/state necesitan
sin pertenecer semánticamente a ninguno de ellos:

  encode_redis_key  — codifica un segmento de clave Redis en base64 urlsafe.
  redis_retry       — reintentos síncronos ante fallos transitorios de Redis.

Por qué existe este módulo
--------------------------
Antes, encode_cursor_key y _retry vivían en cursor_store.py y eran
importados por gap_registry.py y lateness_calibration.py creando un
acoplamiento accidental entre bounded contexts:

  gap_registry      → cursor_store  (solo por _encode y _retry)
  lateness_calib.   → cursor_store  (solo por _encode, _retry y RedisCursorStore)

cursor_store gestiona cursores (exchange, symbol, timeframe) → timestamp.
Ese dominio no tiene relación semántica con los gaps ni con la calibración.
El acoplamiento existía por conveniencia, no por diseño.

Con este módulo:
  gap_registry      → utils          (utilidades) + gap_store (su propio puerto)
  lateness_calib.   → utils          (utilidades) + cursor_store (RedisCursorStore)
  cursor_store      → utils          (consume sus propios helpers vía alias)
  backfill          → utils          (encode_redis_key, nombre público)

Principios
----------
SRP  — cada módulo tiene una sola razón para cambiar
DRY  — una implementación, múltiples consumidores
DIP  — los consumidores dependen de la abstracción (utils), no de cursor_store
SSOT — la implementación vive aquí; aliases en cursor_store para retrocompat.
KISS — dos funciones puras, sin estado, sin dependencias de dominio
"""
from __future__ import annotations

import base64
import time
from typing import Callable, TypeVar

import redis
from loguru import logger

# ---------------------------------------------------------------------------
# Constantes de resiliencia
# ---------------------------------------------------------------------------
_RETRY_ATTEMPTS: int   = 3
_RETRY_BASE_MS:  float = 50.0   # ms — backoff exponencial: 50, 100, 200 ms

_T = TypeVar("_T")

# ---------------------------------------------------------------------------
# Codificación de claves Redis
# ---------------------------------------------------------------------------

def encode_redis_key(value: str) -> str:
    """
    Codifica un segmento de clave Redis en base64 urlsafe sin padding.

    Garantiza que caracteres especiales (/, :, espacios) no corrompan
    la estructura jerárquica de las claves Redis (separador ':').

    Parameters
    ----------
    value : str
        Segmento a codificar (exchange, symbol, timeframe, env…).

    Returns
    -------
    str
        Cadena base64 urlsafe sin '=' de relleno.

    Examples
    --------
    >>> encode_redis_key("BTC/USDT")
    'QlRDL1VTRFQ'
    >>> encode_redis_key("bybit")
    'Ynliafl'
    """
    return base64.urlsafe_b64encode(value.encode()).decode().rstrip("=")


# ---------------------------------------------------------------------------
# Resiliencia síncrona Redis
# ---------------------------------------------------------------------------

def redis_retry(
    fn:        Callable[[], _T],
    attempts:  int   = _RETRY_ATTEMPTS,
    base_ms:   float = _RETRY_BASE_MS,
) -> _T:
    """
    Ejecuta ``fn`` con reintentos exponenciales ante fallos transitorios Redis.

    Solo para contextos síncronos (healthcheck, workers, tests).
    En contextos async usar ``redis_retry_async`` (aún en cursor_store).

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
