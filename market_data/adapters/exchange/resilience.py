"""
market_data/adapters/exchange/resilience.py
===========================================
Resilience layer — retry policies + circuit breaker via aioresilience.

Responsabilidad
---------------
• Traducir excepciones CCXT → taxonomía canónica (rate_limit, timeout, network)
• Aplicar retry policies por tipo de error
• Gestionar circuit breaker

Arquitectura
------------
CCXT exception → classify_error() → error_type → aioresilience policy

El resilience layer NO conoce CCXT — solo ve:
• RetryableError
• NonRetryableError
"""

from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, Optional, TypeVar

import aioresilience
from aioresilience import CircuitBreakerOpenError as AioResilienceCBError
from aioresilience import RetryPolicy, RetryConfig
from aioresilience.config import CircuitConfig

from core.config.schema import ResilienceConfig, ResilienceRetryPolicy

__all__ = [
    "ErrorType",
    "classify_error",
    "ResilienceLayer",
    "RetryExhaustedError",
    "CircuitBreakerOpenError",
]

T = TypeVar("T")

ErrorType = str

_RETRYABLE_ERROR_TYPES: frozenset[str] = frozenset({"rate_limit", "timeout", "network"})


class RetryExhaustedError(Exception):
    """Todas las tentativas de retry se agotaron."""

    def __init__(self, error_type: ErrorType, original_exc: Exception):
        self.error_type = error_type
        self.original_exc = original_exc
        super().__init__(f"Retry exhausted for {error_type}: {original_exc}")


class CircuitBreakerOpenError(Exception):
    """Circuit breaker está abierto — requests bloqueados."""

    def __init__(self, exchange_id: str):
        self.exchange_id = exchange_id
        super().__init__(f"Circuit breaker open for {exchange_id}")


def classify_error(exc: Exception) -> ErrorType:
    """
    Traduce excepciones CCXT/Red a taxonomía canónica.

    Esta es la única capa que conoce tanto aioresilience como CCXT.

    Parameters
    ----------
    exc : excepción capturada

    Returns
    -------
    error_type : "rate_limit" | "timeout" | "network" | "unknown"
    """
    exc_str = str(exc).lower()

    if "429" in exc_str or "rate limit" in exc_str or "ratelimit" in exc_str:
        return "rate_limit"

    if isinstance(exc, asyncio.TimeoutError):
        return "timeout"

    if "timeout" in exc_str or "timed out" in exc_str:
        return "timeout"

    if "connection" in exc_str or "network" in exc_str or "resolve" in exc_str:
        return "network"

    if "500" in exc_str or "502" in exc_str or "503" in exc_str or "504" in exc_str:
        return "network"

    if "circuit" in exc_str or isinstance(exc, AioResilienceCBError):
        return "network"

    if "forbidden" in exc_str or "unauthorized" in exc_str:
        return "unknown"

    if "not found" in exc_str or "404" in exc_str:
        return "unknown"

    return "network"


def _build_retry_config(
    config: ResilienceRetryPolicy,
) -> RetryConfig:
    """Construye RetryConfig de aioresilience desde ResilienceRetryPolicy."""
    from aioresilience.retry import RetryStrategy

    return RetryConfig(
        max_attempts=config.max_attempts,
        initial_delay=1.0,
        max_delay=config.cap_seconds,
        backoff_multiplier=config.backoff_factor,
        strategy=RetryStrategy.EXPONENTIAL,
        jitter=config.jitter,
    )


def _is_retryable(exc: Exception) -> bool:
    """Determina si una excepción es retryable."""
    return classify_error(exc) in _RETRYABLE_ERROR_TYPES


class ResilienceLayer:
    """
    Capa de resiliencia con retry + circuit breaker.

    Uso
    ---
    layer = ResilienceLayer(exchange_id="bybit", config=resilience_config)

    async def fetch():
        return await layer.retry_call(lambda: ccxt.fetch_ohlcv(...))
    """

    def __init__(
        self,
        exchange_id: str,
        config: Optional[ResilienceConfig] = None,
    ) -> None:
        self._exchange_id = exchange_id
        self._config = config or ResilienceConfig()

        circuit_config = CircuitConfig(
            failure_threshold=self._config.limits.max_concurrency,
            recovery_timeout=120.0,
        )
        self._breaker = aioresilience.CircuitBreaker(
            name=exchange_id,
            config=circuit_config,
        )

        self._policies: dict[ErrorType, RetryPolicy] = {
            "rate_limit": RetryPolicy(config=_build_retry_config(self._config.rate_limit)),
            "timeout": RetryPolicy(config=_build_retry_config(self._config.timeout)),
            "network": RetryPolicy(config=_build_retry_config(self._config.network)),
        }

    async def retry_call(
        self,
        coro_fn: Callable[[], Awaitable[T]],
    ) -> T:
        """
        Ejecuta una coroutine con retry y circuit breaker.

        Parameters
        ----------
        coro_fn : callable que retorna una coroutine

        Returns
        -------
        resultado de la coroutine

        Raises
        ------
        RetryExhaustedError : cuando todos los retries fallan
        CircuitBreakerOpenError : cuando el breaker está abierto
        """
        try:
            can_exec = await self._breaker.can_execute()
            if not can_exec:
                raise CircuitBreakerOpenError(self._exchange_id)
            return await self._retry_with_policy(coro_fn, self._policies["network"])
        except AioResilienceCBError:
            raise CircuitBreakerOpenError(self._exchange_id)

    async def _retry_with_policy(
        self,
        coro_fn: Callable[[], Awaitable[T]],
        policy: RetryPolicy,
    ) -> T:
        """Wrapper que aplica retry policy."""
        try:
            return await policy.execute(coro_fn)
        except Exception as exc:
            if classify_error(exc) == "unknown":
                raise
            raise RetryExhaustedError(classify_error(exc), exc) from exc

    async def call_with_policy(
        self,
        coro_fn: Callable[[], Awaitable[T]],
        error_type: ErrorType,
    ) -> T:
        """
        Ejecuta con política de retry específica por tipo de error.

        Parameters
        ----------
        coro_fn   : callable que retorna una coroutine
        error_type : tipo de error para seleccionar la policy

        Returns
        -------
        resultado de la coroutine
        """
        policy = self._policies.get(error_type, self._policies["network"])
        try:
            return await policy.execute(coro_fn)
        except Exception as exc:
            if classify_error(exc) == "unknown":
                raise
            raise RetryExhaustedError(error_type, exc) from exc

    def breaker_state(self) -> dict:
        """Estado del circuit breaker."""
        try:
            state = self._breaker.get_state()
            return {
                "exchange_id": self._exchange_id,
                "state": str(state),
                "failure_count": 0,
            }
        except Exception:
            return {
                "exchange_id": self._exchange_id,
                "state": "unknown",
                "failure_count": 0,
            }

    def is_breaker_open(self) -> bool:
        """True si el breaker está abierto."""
        try:
            state = self._breaker.get_state()
            return str(state) == "CircuitState.OPEN"
        except Exception:
            return False
