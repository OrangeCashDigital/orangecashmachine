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

import ccxt.async_support as _ccxt_async

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

    SSOT de clasificación de errores para toda la capa de resiliencia.

    Estrategia: tipos primero, strings como fallback.
    ─────────────────────────────────────────────────
    isinstance() es robusto ante cambios de mensaje upstream (CCXT,
    aiohttp, exchanges individuales). Los strings de error varían entre
    versiones, idiomas de respuesta HTTP, y wrappers de exchange.

    Orden de decisión
    -----------------
    1. Tipos CCXT conocidos        → clasificación exacta (inmutable)
    2. asyncio.TimeoutError        → timeout
    3. AioResilienceCBError        → circuit open (tratado como network)
    4. Fallback por mensaje        → para excepciones sin tipo conocido
       (aiohttp, OSError, wrappers genéricos de exchange)
    5. _NETWORK_SIGNALS            → ssl/eof/pipe como red legítima
    6. Default "unknown"           → Fail-Fast (NonRetryable)

    Returns
    -------
    "rate_limit" | "timeout" | "network" | "unknown"
    """
    # ── 1. Tipos CCXT — robusto, no depende de mensajes ──────────────────────
    # RateLimitExceeded hereda de NetworkError en ccxt — debe ir primero.
    if isinstance(exc, _ccxt_async.RateLimitExceeded):
        return "rate_limit"

    if isinstance(exc, (_ccxt_async.RequestTimeout, asyncio.TimeoutError)):
        return "timeout"

    # NetworkError es la clase base de todos los errores de red de ccxt:
    # cubre ConnectionError, ExchangeNotAvailable, DDoSProtection, etc.
    if isinstance(exc, _ccxt_async.NetworkError):
        return "network"

    # AuthenticationError, BadSymbol, NotSupported, etc. — permanentes.
    if isinstance(exc, _ccxt_async.ExchangeError):
        return "unknown"

    # ── 2. Tipos stdlib y aioresilience ──────────────────────────────────────
    if isinstance(exc, AioResilienceCBError):
        # Circuit breaker de aioresilience — no es un error de red real,
        # pero tratarlo como "network" permite que el caller lo maneje
        # consistentemente. CircuitBreakerOpenError se lanza antes de
        # llegar aquí en retry_call, pero si escapa, clasificar como network.
        return "network"

    # ── 3. Fallback por mensaje — para excepciones sin tipo ccxt ─────────────
    # Cubre: aiohttp.ClientError, OSError, wrappers de exchange no tipados,
    # y mensajes de error que no tienen clase específica en ccxt.
    exc_str = str(exc).lower()

    if "429" in exc_str or "rate limit" in exc_str or "ratelimit" in exc_str:
        return "rate_limit"

    if "timeout" in exc_str or "timed out" in exc_str:
        return "timeout"

    if "connection" in exc_str or "network" in exc_str or "resolve" in exc_str:
        return "network"

    if "500" in exc_str or "502" in exc_str or "503" in exc_str or "504" in exc_str:
        return "network"

    if "forbidden" in exc_str or "unauthorized" in exc_str:
        return "unknown"

    if "not found" in exc_str or "404" in exc_str:
        return "unknown"

    # ── 4. Señales de red de bajo nivel ──────────────────────────────────────
    # ssl handshake, eof, broken pipe, reset by peer, DNS nodename.
    # No tienen clase propia — solo detectables por mensaje.
    _NETWORK_SIGNALS = ("ssl", "eof", "broken pipe", "reset by peer", "nodename")
    if any(sig in exc_str for sig in _NETWORK_SIGNALS):
        return "network"

    # ── 5. Default Fail-Fast ──────────────────────────────────────────────────
    # Errores no reconocidos explícitamente son NonRetryable.
    # Esto previene que bugs de código (AttributeError, ImportError)
    # se silencien en retries infinitos.
    return "unknown"


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
        Aplica retry policy al callable dado, con breaker check por intento.

        El error_type ya fue clasificado por retry_call en el primer intento —
        no se reclasifica aquí para evitar divergencia entre el tipo registrado
        en el throttle y el tipo usado para seleccionar la policy.

        Breaker-aware
        -------------
        aioresilience.RetryPolicy.execute() no consulta el circuit breaker
        entre reintentos — solo ejecuta el callable N veces con backoff.
        Si el breaker se abre durante el loop (por fallos de otro worker
        concurrente), los intentos restantes deben abortarse de inmediato.

        Para implementar esto, envolvemos coro_fn con _breaker_guard que
        verifica can_execute() antes de cada invocación. Si el breaker está
        abierto, _breaker_guard lanza AioResilienceCBError, que aioresilience
        trata como un fallo y eventualmente agota los retries, propagando
        CircuitBreakerOpenError al caller.

        Parameters
        ----------
        coro_fn    : callable sin argumentos que retorna coroutine
        policy     : RetryPolicy seleccionada por retry_call
        error_type : tipo ya clasificado (evita doble classify_error)
        """
        async def _breaker_guard() -> T:
            """Verifica el breaker antes de cada intento. Fail-Fast si abierto."""
            try:
                can_exec = await self._breaker.can_execute()
                if not can_exec:
                    raise AioResilienceCBError(
                        f"Circuit breaker open for {self._exchange_id} "
                        f"(checked inside retry loop)"
                    )
            except AioResilienceCBError:
                raise  # re-raise para que policy lo vea como fallo
            return await coro_fn()

        try:
            return await policy.execute(_breaker_guard)
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

    async def notify_rate_limit(self) -> None:
        """
        Notifica un evento de rate-limit (429) al circuit breaker.

        API pública para consumidores externos que detectan un 429
        fuera del flujo normal de retry_call (p.ej. ohlcv_fetcher).
        Elimina el acceso directo a breaker._inc_counter().

        Principios
        ----------
        - SRP: ResilienceLayer es el único propietario del breaker.
        - Encapsulamiento: API pública en lugar de hack privado.
        - Fail-Soft: si on_failure() lanza, se absorbe — el breaker
          no puede interrumpir el retry loop del caller.
        """
        try:
            await self._breaker.on_failure()
        except Exception:
            pass  # Fail-Soft — nunca interrumpir el flujo del caller

    async def notify_rate_limit(self) -> None:
        """
        Notifica un evento de rate-limit (429) al circuit breaker.

        API pública para que consumidores externos (p.ej. ohlcv_fetcher)
        puedan registrar fallos sin acceder a _breaker directamente.

        Diseño
        ------
        - SRP: ResilienceLayer es el único propietario del breaker.
        - Encapsulamiento: elimina el hack breaker._inc_counter() en fetcher.
        - Idempotente: on_failure() es seguro de llamar múltiples veces.
        - Fail-Soft: si el breaker no responde, se absorbe silenciosamente
          para no interferir con el flujo de retry del caller.
        """
        try:
            await self._breaker.on_failure()
        except Exception:
            pass  # Fail-Soft — el breaker no puede interrumpir el retry loop


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
