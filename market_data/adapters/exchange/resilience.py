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
    "register_resilience_layer",
    "get_breaker_state",
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

        # failure_threshold: número de fallos consecutivos para abrir el breaker.
        # Invariante de dominio: independiente de max_concurrency.
        # 5 fallos consecutivos → breaker abierto (conservador para exchanges cripto).
        _CB_FAILURE_THRESHOLD = getattr(
            self._config, "cb_failure_threshold", 5
        )
        circuit_config = CircuitConfig(
            failure_threshold=_CB_FAILURE_THRESHOLD,
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

        Selecciona automáticamente la retry policy según el tipo de error
        clasificado en el primer intento fallido. Orden de decisión:

          1. Circuit breaker abierto → CircuitBreakerOpenError inmediato (Fail-Fast)
          2. Error clasificado       → policy específica (rate_limit/timeout/network)
          3. Error desconocido       → re-raise sin retry (NonRetryable)

        Parameters
        ----------
        coro_fn : callable que retorna una coroutine (sin argumentos)

        Raises
        ------
        RetryExhaustedError     : todos los retries de la policy agotados
        CircuitBreakerOpenError : breaker abierto — no se intenta la llamada
        """
        try:
            can_exec = await self._breaker.can_execute()
            if not can_exec:
                raise CircuitBreakerOpenError(self._exchange_id)
        except AioResilienceCBError:
            raise CircuitBreakerOpenError(self._exchange_id)

        # Primer intento para clasificar el error y seleccionar policy.
        # Si tiene éxito, retorna directamente (fast path — 0 overhead de retry).
        try:
            return await coro_fn()
        except AioResilienceCBError:
            raise CircuitBreakerOpenError(self._exchange_id)
        except Exception as exc:
            error_type = classify_error(exc)
            if error_type == "unknown":
                raise  # NonRetryable — re-raise sin envolver
            # Seleccionar policy específica según tipo de error clasificado.
            # rate_limit → backoff agresivo; timeout → backoff moderado; network → rápido
            policy = self._policies.get(error_type, self._policies["network"])
            return await self._retry_with_policy(coro_fn, policy, error_type)

    async def _retry_with_policy(
        self,
        coro_fn: Callable[[], Awaitable[T]],
        policy: RetryPolicy,
        error_type: ErrorType,
    ) -> T:
        """
        Aplica retry policy al callable dado.

        El error_type ya fue clasificado por retry_call en el primer intento —
        no se reclasifica aquí para evitar divergencia entre el tipo registrado
        en el throttle y el tipo usado para seleccionar la policy.

        Parameters
        ----------
        coro_fn    : callable sin argumentos que retorna coroutine
        policy     : RetryPolicy seleccionada por retry_call
        error_type : tipo ya clasificado (evita doble classify_error)
        """
        try:
            return await policy.execute(coro_fn)
        except AioResilienceCBError:
            raise CircuitBreakerOpenError(self._exchange_id)
        except Exception as exc:
            if classify_error(exc) == "unknown":
                raise
            raise RetryExhaustedError(error_type, exc) from exc

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


# ===========================================================================
# Module-level registry — acceso a ResilienceLayer por exchange_id
# ===========================================================================
# SSOT: una sola instancia por exchange_id por proceso.
# CCXTAdapter registra su capa en __init__ via register_resilience_layer().
# Consumidores llaman get_breaker_state() sin acoplamiento directo al adapter.

_layer_registry: dict[str, "ResilienceLayer"] = {}


def register_resilience_layer(exchange_id: str, layer: "ResilienceLayer") -> None:
    """
    Registra una ResilienceLayer activa en el registry del proceso.

    Llamado por CCXTAdapter.__init__ inmediatamente tras construir
    su ResilienceLayer. Idempotente: sobreescribe si ya existe.

    Parameters
    ----------
    exchange_id : identificador del exchange (ej. "bybit", "binance")
    layer       : instancia de ResilienceLayer a registrar
    """
    _layer_registry[exchange_id] = layer


def get_breaker_state(exchange_id: str) -> dict:
    """
    Retorna el estado del circuit breaker para un exchange dado.

    Fail-Soft: si el exchange no esta registrado o el breaker no
    responde, retorna estado "unknown" sin lanzar excepcion.

    Parameters
    ----------
    exchange_id : identificador del exchange

    Returns
    -------
    dict con keys: exchange_id, state, failure_count
    """
    layer = _layer_registry.get(exchange_id)
    if layer is None:
        return {
            "exchange_id": exchange_id,
            "state": "unknown",
            "failure_count": 0,
        }
    return layer.breaker_state()
