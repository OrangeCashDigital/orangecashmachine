# -*- coding: utf-8 -*-
"""
market_data/adapters/inbound/rest/_retry.py
============================================

Helper compartido (DRY): retry asíncrono con backoff exponencial.

Centraliza la lógica de reintento para que FundingRateFetcher,
OpenInterestFetcher, TradesFetcher y OHLCVFetcher no la dupliquen.

Principios: DRY · SafeOps · SRP
"""
from __future__ import annotations

import asyncio
from typing import Any, Callable, Coroutine, Optional, Type

from loguru import logger

_log = logger.bind(module="adapters.inbound.rest._retry")


async def retry_async(
    coro_fn: Callable[[], Coroutine[Any, Any, Any]],
    *,
    attempts:                  int                        = 3,
    backoff_base:              float                      = 1.5,
    backoff_cap:               float                      = 20.0,
    context:                   str                        = "",
    circuit_open_exc:          Optional[Type[BaseException]] = None,
    not_supported_passthrough: bool                       = False,
) -> Any:
    """
    Ejecuta ``coro_fn()`` con retry + backoff exponencial.

    Parámetros
    ----------
    coro_fn : callable sin argumentos que retorna una corrutina.
    attempts : número máximo de intentos (default 3).
    backoff_base : base del backoff exponencial (default 1.5).
    backoff_cap : techo de backoff en segundos (default 20.0).
    context : string de trazabilidad para los logs.
    circuit_open_exc : excepción de circuit-breaker abierto — se
        relanza inmediatamente sin reintentar (Fail-Fast).
    not_supported_passthrough : si True, retorna None cuando el
        endpoint no está soportado o los intentos se agotan.

    Retorna
    -------
    Resultado de ``coro_fn()`` si tiene éxito.
    None si ``not_supported_passthrough=True`` y el endpoint no
    está soportado o los intentos se agotan.

    Lanza
    -----
    La última excepción si los intentos se agotan y
    ``not_supported_passthrough=False``.
    """
    last_exc: Optional[BaseException] = None

    for attempt in range(1, attempts + 1):
        try:
            return await coro_fn()

        except Exception as exc:

            # Fail-Fast: circuit breaker abierto — no reintentar.
            if circuit_open_exc is not None and isinstance(exc, circuit_open_exc):
                raise

            # Fail-Soft: endpoint no soportado — no es retriable.
            if not_supported_passthrough:
                err_lower = str(exc).lower()
                if "notsupported" in err_lower or "not supported" in err_lower:
                    _log.debug(
                        "Endpoint not supported — skipping | context={}", context
                    )
                    return None

            last_exc = exc

            if attempt < attempts:
                backoff = min(backoff_base ** attempt, backoff_cap)
                _log.warning(
                    "Attempt {}/{} failed — retrying in {:.1f}s | "
                    "context={} error={}",
                    attempt, attempts, backoff, context, exc,
                )
                await asyncio.sleep(backoff)
            else:
                _log.warning(
                    "All {} attempts exhausted | context={} last_error={}",
                    attempts, context, exc,
                )

    # Intentos agotados.
    if not_supported_passthrough:
        return None

    assert last_exc is not None  # invariante: attempts >= 1 y falló → hay exc
    raise last_exc


__all__ = ["retry_async"]
