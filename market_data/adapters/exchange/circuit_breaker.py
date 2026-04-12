"""
services/exchange/circuit_breaker.py
=====================================
Circuit breaker compartido por exchange_id.

Un breaker global por exchange (no por instancia) evita que spot
y futures tengan breakers independientes que escalen en cascada
cuando el exchange está bajo presión.
"""
from __future__ import annotations

import time
from typing import Dict

import pybreaker

from market_data.adapters.exchange.errors import ExchangeCircuitOpenError  # noqa: F401 re-export

__all__ = [
    "ExchangeCircuitOpenError",
    "_get_breaker",
    "get_breaker_state",
    "breaker_call_async",
]

_CB_FAIL_MAX:      int = 10
_CB_RESET_TIMEOUT: int = 120

_BREAKERS: Dict[str, pybreaker.CircuitBreaker] = {}


def _get_breaker(exchange_id: str) -> pybreaker.CircuitBreaker:
    """Retorna (o crea) el circuit breaker para un exchange. Thread-safe en CPython."""
    return _BREAKERS.setdefault(
        exchange_id,
        pybreaker.CircuitBreaker(
            fail_max      = _CB_FAIL_MAX,
            reset_timeout = _CB_RESET_TIMEOUT,
            name          = exchange_id,
        ),
    )


def get_breaker_state(exchange_id: str) -> dict:
    """
    Estado observable del circuit breaker.

    Returns
    -------
    dict: exchange, state, fail_counter, cooldown_remaining_ms.
    SafeOps: nunca lanza excepción al caller.
    """
    try:
        breaker = _BREAKERS.get(exchange_id)
        if breaker is None:
            return {
                "exchange":              exchange_id,
                "state":                 "closed",
                "fail_counter":          0,
                "cooldown_remaining_ms": 0,
            }
        state_name  = breaker.current_state
        cooldown_ms = 0
        if state_name == "open":
            opened_at = getattr(breaker._state, "opened_at", None)
            if opened_at is not None:
                elapsed_s   = time.time() - opened_at
                remaining_s = max(0.0, _CB_RESET_TIMEOUT - elapsed_s)
                cooldown_ms = int(remaining_s * 1000)
        return {
            "exchange":              exchange_id,
            "state":                 state_name,
            "fail_counter":          breaker.fail_counter,
            "cooldown_remaining_ms": cooldown_ms,
        }
    except Exception:
        return {
            "exchange":              exchange_id,
            "state":                 "unknown",
            "fail_counter":          0,
            "cooldown_remaining_ms": 0,
        }


async def breaker_call_async(breaker: pybreaker.CircuitBreaker, coro_fn):
    """
    Reemplaza breaker.call_async() — roto en pybreaker 1.x con Python 3.11+.

    Replica la lógica del breaker manualmente:
    - Si el circuito está abierto → lanza CircuitBreakerError inmediatamente.
    - Si la llamada falla → incrementa el contador de fallos.
    - Si la llamada tiene éxito → resetea el contador.

    Parameters
    ----------
    breaker : pybreaker.CircuitBreaker
    coro_fn : callable que retorna una coroutine (async def _call(): ...)
    """
    if breaker.current_state == "open":
        raise pybreaker.CircuitBreakerError(breaker)

    try:
        result = await coro_fn()
        # En half-open: un éxito cierra el breaker via close()
        if breaker.current_state == "half-open":
            breaker.close()
        return result
    except pybreaker.CircuitBreakerError:
        raise
    except Exception:
        breaker._inc_counter()
        raise
