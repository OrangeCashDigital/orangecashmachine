"""
services/exchange/ccxt_adapter.py
==================================

CCXT Exchange Adapter — única fuente de verdad del cliente ccxt.

Arquitectura de resiliencia (3 capas)
-------------------------------------
1. AdaptiveThrottle (cerebro) — decide concurrencia
2. AdaptiveLimiter (músculo)  — aplica hard limit (aiometer)
3. ResilienceLayer (protección) — retries + circuit breaker (aioresilience)

Flujo: Throttle → Limiter → Resilience → ccxt.fetch_*()

Módulos relacionados
--------------------
errors.py     — clases de excepción
limiter.py    — wrapper aiometer
resilience.py — retry + circuit breaker
throttle.py   — concurrencia adaptiva por pipeline
"""

# =============================================================================
# INTERNAL MODULE — NO importar directamente desde consumidores externos.
# Usar: from market_data.adapters.exchange import CCXTAdapter (API pública)
# =============================================================================


from __future__ import annotations

import asyncio
import random
import copy
import time
from typing import Any, Dict, List, Optional, TYPE_CHECKING

import ccxt.async_support as ccxt
from loguru import logger

from market_data.adapters.exchange.base import ExchangeAdapter
from market_data.adapters.exchange.exchange_quirks import get_quirks
from market_data.adapters.exchange.errors import (
    ExchangeAdapterError,
    UnsupportedExchangeError,
    ExchangeConnectionError,
    ExchangeCircuitOpenError,
)
from market_data.adapters.exchange.limiter import (
    AdaptiveLimiter,
    get_or_create_limiter,
    get_limiter_state,
)
from market_data.adapters.exchange.resilience import (
    CircuitBreakerOpenError,
    RetryExhaustedError,
    ResilienceLayer,
)
from market_data.adapters.exchange.throttle import (
    AdaptiveThrottle,
    get_or_create_throttle,
    get_throttle_state,
)
from market_data.observability.metrics import EXCHANGE_CIRCUIT_OPEN

if TYPE_CHECKING:
    from ocm_platform.config.schema import ExchangeConfig, ResilienceConfig

# Símbolos definidos en este módulo.
# Las clases de resiliencia, throttle y limiter se re-exportan desde
# market_data.adapters.exchange.__init__ — no desde aquí.
__all__ = [
    "CCXTAdapter",
]


# ==========================================================
# Constants
# ==========================================================

_INIT_RETRIES = 3
_BACKOFF_BASE = 2.0
_LOAD_MARKETS_TIMEOUT = 30.0
_DEFAULT_EXCHANGE = "binance"
_MARKETS_CACHE_TTL = 60.0  # segundos — reutilizar markets en reconnect

# Cache global de markets compartida entre instancias del mismo exchange.
# Clave: "{exchange_id}:{default_type}" — cada combinación tiene su propio snapshot.
# Esto evita llamadas redundantes a load_markets() cuando múltiples tasks
# crean adapters independientes para el mismo exchange en el mismo proceso.
# Thread-safety: asyncio es single-threaded — no se necesita Lock.
_GLOBAL_MARKETS_CACHE: Dict[str, Any] = {}
_GLOBAL_MARKETS_CACHED_AT: Dict[str, float] = {}

# Timeouts por operación (segundos)
_FETCH_TICKER_TIMEOUT = 10.0
_FETCH_OHLCV_TIMEOUT = 30.0
_FETCH_TRADES_TIMEOUT = 15.0


class CCXTAdapter(ExchangeAdapter):
    """
    Adapter asíncrono para exchanges vía ccxt.

    Acepta credenciales de dos formas (prioridad: explícitas > ExchangeConfig):
      1. Parámetros explícitos: exchange_id, api_key, api_secret
      2. ExchangeConfig:        config=exc_cfg

    Uso
    ---
    # Desde ExchangeConfig (recomendado en pipelines)
    adapter = CCXTAdapter(config=exc_cfg)

    # Explícito (testing, scripts)
    adapter = CCXTAdapter(exchange_id="binance", api_key="...", api_secret="...")

    async with adapter:
        df = await adapter.fetch_ohlcv("BTC/USDT", "1h", since=..., limit=500)
    """

    def __init__(
        self,
        exchange_id: Optional[str] = None,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        config: Optional["ExchangeConfig"] = None,
        default_type: Optional[str] = None,
    ) -> None:

        self._exchange_id = self._resolve_exchange_id(exchange_id, config)
        self._default_type = default_type or self._resolve_default_type(config)

        if config is not None:
            _creds = config.ccxt_credentials()
            self._api_key = _creds.get("apiKey") or None
            self._api_secret = _creds.get("secret") or None
            self._api_password = _creds.get("password") or None
            self._resilience_config: Optional["ResilienceConfig"] = config.resilience
        else:
            self._api_key = api_key or None
            self._api_secret = api_secret or None
            self._api_password = None
            self._resilience_config = None

        self._client: Optional[ccxt.Exchange] = None
        # asyncio.Lock lazy — se crea en el loop activo para evitar
        # RuntimeError: Event loop is closed cuando el adapter se
        # instancia fuera de un loop (ej. Prefect entre flow runs).
        # Ref: docs.python.org/3/library/asyncio-sync.html
        self._init_lock: Optional[asyncio.Lock] = None
        self._markets_cache: Optional[Dict[str, Any]] = None
        self._markets_cached_at: float = 0.0

        self._limiter = get_or_create_limiter(
            exchange_id=self._exchange_id,
            max_concurrency=(self._resilience_config.limits.max_concurrency if self._resilience_config else 5),
            max_rate=(self._resilience_config.limits.max_rate if self._resilience_config else 10.0),
        )

        self._throttle = get_or_create_throttle(
            exchange_id=self._exchange_id,
            market_type=self._default_type or "spot",
            dataset="ohlcv",
            initial=self._limiter.max_concurrency,
            maximum=self._limiter.max_concurrency * 2,
            latency_target_ms=500,
            limiter=self._limiter,
        )

        self._resilience = ResilienceLayer(
            exchange_id=self._exchange_id,
            config=self._resilience_config,
        )
        # Registrar en registry de proceso para que get_breaker_state() funcione.
        # Idempotente: sobreescribe si ya existe (misma instancia en reconnect).
        from market_data.adapters.exchange.resilience import register_resilience_layer
        register_resilience_layer(self._exchange_id, self._resilience)

        self._closed: bool = False

    # ----------------------------------------------------------
    # Sync primitives — lazy init (loop-safe)
    # ----------------------------------------------------------

    @property
    def _lock(self) -> asyncio.Lock:
        """asyncio.Lock lazy — creado en el loop activo corriente."""
        if self._init_lock is None:
            self._init_lock = asyncio.Lock()
        return self._init_lock

    # ----------------------------------------------------------
    # Lifecycle
    # ----------------------------------------------------------

    async def connect(self) -> None:
        """Inicializa cliente y carga mercados (idempotente)."""
        if self._client is not None:
            return
        async with self._lock:
            if self._client is None:
                await self._initialize()

    async def reconnect(self) -> None:
        """Fuerza reconexión cerrando el cliente actual (thread-safe)."""
        async with self._lock:
            old_client = self._client
            self._client = None
            if old_client is not None:
                try:
                    await old_client.close()
                except Exception:
                    pass
            await self._initialize()

    async def is_healthy(self) -> bool:
        """
        Healthcheck ligero: verifica que el cliente existe y la sesión HTTP está abierta.
        No hace ninguna llamada de red — solo inspecciona estado interno.
        """
        if self._client is None:
            return False
        try:
            session = getattr(self._client, "session", None)
            if session is None:
                return True
            # session.closed es suficiente — session._loop fue eliminado en
            # aiohttp >= 3.9 y getattr retornaba None silenciosamente,
            # desactivando el check de loop sin ningún aviso.
            if hasattr(session, "closed") and session.closed:
                return False
            return True
        except Exception:
            return False

    async def close(self) -> None:
        """Cierre seguro e idempotente — nunca lanza excepción."""
        if self._closed:
            return
        self._closed = True
        async with self._lock:
            if self._client is None:
                return
            try:
                await self._client.close()
                logger.debug("Exchange closed | {}", self._exchange_id)
            except Exception as exc:
                logger.warning("Error closing exchange | {} | {}", self._exchange_id, exc)
            finally:
                self._client = None

    # ----------------------------------------------------------
    # Public API — todos los métodos de I/O tienen timeout explícito
    # ----------------------------------------------------------

    async def fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        client = await self._get_client()
        async with self._limiter.slot():
            async def _call():
                return await asyncio.wait_for(
                    client.fetch_ticker(symbol),
                    timeout=_FETCH_TICKER_TIMEOUT,
                )

            try:
                _t0 = time.perf_counter()
                result = await self._resilience.retry_call(_call)
                self._throttle.record_success(latency_ms=(time.perf_counter() - _t0) * 1000)
                return result
            except RetryExhaustedError as exc:
                self._throttle.record_error(error_type=exc.error_type)
                if exc.error_type == "rate_limit":
                    self._throttle.record_rate_limit_hit()
                raise
            except ExchangeCircuitOpenError:
                self._handle_circuit_open("fetch_ticker")
                raise

    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: Optional[int] = None,
        limit: int = 100,
        market_type: Optional[str] = None,
        end_ms: Optional[int] = None,
    ) -> List[List[Any]]:
        client = await self._get_client()
        params: Dict[str, Any] = {}
        effective_type = market_type or self._default_type
        if effective_type:
            params["defaultType"] = effective_type
        _quirks = get_quirks(self._exchange_id)
        if _quirks.reject_zero_since and since == 0:
            since = None
        if _quirks.requires_end_at:
            now_ts = int(time.time() * 1000)
            _end_at_ms = end_ms if end_ms is not None else now_ts
            params["endAt"] = min(_end_at_ms, now_ts) // 1000

        async with self._limiter.slot():
            async def _call():
                return await asyncio.wait_for(
                    client.fetch_ohlcv(
                        symbol,
                        timeframe,
                        since=since,
                        limit=limit,
                        params=params,
                    ),
                    timeout=_FETCH_OHLCV_TIMEOUT,
                )

            try:
                _t0 = time.perf_counter()
                result = await self._resilience.retry_call(_call)
                self._throttle.record_success(latency_ms=(time.perf_counter() - _t0) * 1000)
                return result
            except RetryExhaustedError as exc:
                self._throttle.record_error(error_type=exc.error_type)
                if exc.error_type == "rate_limit":
                    self._throttle.record_rate_limit_hit()
                raise
            except ExchangeCircuitOpenError:
                self._handle_circuit_open("fetch_ohlcv")
                raise
            except asyncio.TimeoutError:
                self._throttle.record_error(error_type="timeout")
                raise
            except ccxt.AuthenticationError as exc:
                # Fatal permanente — credenciales inválidas o IP ban.
                # No existe retry útil: el problema no es transitorio.
                # Re-raise como ExchangeAdapterError para que el caller
                # (ohlcv_fetcher) reciba un error tipado sin necesidad
                # de importar ccxt directamente.
                raise ExchangeAdapterError(
                    f"Authentication failed for {self._exchange_id}: {exc}"
                ) from exc

    async def fetch_trades(
        self,
        symbol: str,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        client = await self._get_client()
        async with self._limiter.slot():
            async def _call():
                return await asyncio.wait_for(
                    client.fetch_trades(symbol, limit=limit),
                    timeout=_FETCH_TRADES_TIMEOUT,
                )

            try:
                _t0 = time.perf_counter()
                result = await self._resilience.retry_call(_call)
                self._throttle.record_success(latency_ms=(time.perf_counter() - _t0) * 1000)
                return result
            except RetryExhaustedError as exc:
                self._throttle.record_error(error_type=exc.error_type)
                if exc.error_type == "rate_limit":
                    self._throttle.record_rate_limit_hit()
                raise
            except ExchangeCircuitOpenError:
                self._handle_circuit_open("fetch_trades")
                raise

    async def load_markets(self) -> Dict[str, Any]:
        client = await self._get_client()
        return await asyncio.wait_for(
            client.load_markets(),
            timeout=_LOAD_MARKETS_TIMEOUT,
        )

    def get_market(self, symbol: str) -> Optional[Dict[str, Any]]:
        return (self._markets_cache or {}).get(symbol)

    def parse8601(self, date_str: str) -> int:
        import ccxt as ccxt_sync

        return ccxt_sync.Exchange.parse8601(date_str)

    def iso8601(self, timestamp_ms: int) -> str:
        import ccxt as ccxt_sync

        return ccxt_sync.Exchange.iso8601(timestamp_ms)

    async def inspect_required_credentials(self) -> Dict[str, Any]:
        import ccxt as ccxt_sync

        if not hasattr(ccxt_sync, self._exchange_id):
            raise UnsupportedExchangeError(f"Exchange '{self._exchange_id}' not supported")
        exchange_class = getattr(ccxt_sync, self._exchange_id)
        instance = exchange_class()
        required = instance.requiredCredentials
        logger.info(
            "Required credentials | {} fields={}",
            self._exchange_id,
            [k for k, v in required.items() if v],
        )
        return {k: v for k, v in required.items() if v}

    # ----------------------------------------------------------
    # Internal helpers
    # ----------------------------------------------------------

    def _handle_circuit_open(self, operation: str) -> None:
        """
        Registra métricas y ajusta throttle cuando el circuit breaker está abierto.

        Extraído para eliminar triplicación en fetch_ticker / fetch_ohlcv /
        fetch_trades. SafeOps: nunca lanza excepción (las métricas son
        best-effort; el raise lo hace el caller).
        """
        try:
            EXCHANGE_CIRCUIT_OPEN.labels(
                exchange=self._exchange_id, operation=operation
            ).inc()
            self._throttle.record_error(error_type="rate_limit")
            self._throttle.record_rate_limit_hit()
        except Exception:
            pass  # métricas son best-effort — nunca bloquear el raise

    # ----------------------------------------------------------
    # Credential resolvers (privados — SRP)
    # ----------------------------------------------------------

    @staticmethod
    def _resolve_exchange_id(
        explicit: Optional[str],
        config: Optional["ExchangeConfig"],
    ) -> str:
        if explicit:
            return explicit.lower()
        if config is not None:
            return config.name.value
        return _DEFAULT_EXCHANGE

    @staticmethod
    def _resolve_default_type(
        config: Optional["ExchangeConfig"],
    ) -> Optional[str]:
        if config is None:
            return None
        return config.markets.futures_default_type

    # ----------------------------------------------------------
    # Internal
    # ----------------------------------------------------------

    async def _get_client(self) -> ccxt.Exchange:
        """
        Retorna el cliente ccxt activo, inicializando o reconectando si necesario.

        Política de sesión (SRP — CCXTAdapter es self-contained)
        ---------------------------------------------------------
        1. Cliente inexistente → inicializar (cold start)
        2. Cliente existe, sesión sana → retornar directamente (fast path)
        3. Cliente existe, sesión muerta → reconectar antes de retornar

        La reconexión proactiva aquí elimina la necesidad de que
        consumidores externos (ohlcv_fetcher, etc.) detecten y manejen
        sesiones muertas — esa responsabilidad pertenece al adapter.

        is_healthy() es O(1): no hace llamadas de red, solo inspecciona
        session.closed. El overhead por llamada es negligible.
        """
        # Cold start: cliente nunca inicializado
        if self._client is None:
            async with self._lock:
                if self._client is None:
                    await self._initialize()
            return self._client

        # Fast path: cliente existe y sesión sana
        if await self.is_healthy():
            return self._client

        # Sesión muerta: reconectar proactivamente antes de la llamada.
        # Esto evita que el error "Session is closed" salga del adapter
        # y tenga que ser manejado por el caller.
        logger.debug(
            "Session dead before request — reconnecting proactively | {}",
            self._exchange_id,
        )
        await self.reconnect()
        return self._client

    async def _initialize(self) -> None:
        if not hasattr(ccxt, self._exchange_id):
            raise UnsupportedExchangeError(f"Exchange '{self._exchange_id}' not supported by ccxt")

        exchange_class = getattr(ccxt, self._exchange_id)
        last_exc: Optional[Exception] = None

        for attempt in range(1, _INIT_RETRIES + 1):
            try:
                params: Dict[str, Any] = {
                    "enableRateLimit": True,
                    "options": {
                        "adjustForTimeDifference": True,
                        "recvWindow": 10_000,
                    },
                }
                if self._api_key:
                    params["apiKey"] = self._api_key
                if self._api_secret:
                    params["secret"] = self._api_secret
                if self._api_password:
                    params["password"] = self._api_password
                if self._default_type:
                    params["options"]["defaultType"] = self._default_type

                client = exchange_class(params)

                now = time.perf_counter()
                global_key = f"{self._exchange_id}:{self._default_type or 'spot'}"
                # Prioridad de cache: instancia > global > load_markets()
                instance_valid = (
                    self._markets_cache is not None and (now - self._markets_cached_at) < _MARKETS_CACHE_TTL
                )
                global_valid = (
                    global_key in _GLOBAL_MARKETS_CACHE
                    and (now - _GLOBAL_MARKETS_CACHED_AT.get(global_key, 0.0)) < _MARKETS_CACHE_TTL
                )

                if instance_valid:
                    client.markets = self._markets_cache
                    latency = 0.0
                    logger.debug(
                        "Exchange connected (markets from instance cache) | {} cache_age={:.1f}s",
                        self._exchange_id,
                        now - self._markets_cached_at,
                    )
                elif global_valid:
                    client.markets = copy.deepcopy(_GLOBAL_MARKETS_CACHE[global_key])
                    self._markets_cache = client.markets
                    self._markets_cached_at = now
                    latency = 0.0
                    logger.debug(
                        "Exchange connected (markets from global cache) | {} cache_age={:.1f}s",
                        self._exchange_id,
                        now - _GLOBAL_MARKETS_CACHED_AT[global_key],
                    )
                else:
                    start = time.perf_counter()
                    await asyncio.wait_for(
                        client.load_markets(),
                        timeout=_LOAD_MARKETS_TIMEOUT,
                    )
                    latency = (time.perf_counter() - start) * 1000
                    self._markets_cache = client.markets
                    self._markets_cached_at = time.perf_counter()
                    _GLOBAL_MARKETS_CACHE[global_key] = copy.deepcopy(client.markets)
                    _GLOBAL_MARKETS_CACHED_AT[global_key] = self._markets_cached_at

                self._client = client
                logger.info(
                    "Exchange connected | {} latency={:.1f}ms",
                    self._exchange_id,
                    latency,
                )
                return

            except asyncio.TimeoutError as exc:
                last_exc = exc
                logger.warning(
                    "load_markets timeout | {} attempt={}",
                    self._exchange_id,
                    attempt,
                )
                # Destruir cliente parcialmente inicializado — puede tener
                # sockets abiertos o estado inconsistente tras cancel de wait_for.
                try:
                    await client.close()
                except Exception:
                    pass
                client = None

            except Exception as exc:
                last_exc = exc
                delay = (_BACKOFF_BASE**attempt) + random.random()
                logger.warning(
                    "Connection failed | {} attempt={} retry_in={:.1f}s error={}",
                    self._exchange_id,
                    attempt,
                    delay,
                    exc,
                )
                # Destruir cliente antes de reintentar — evita leaks de sesión.
                try:
                    await client.close()
                except Exception:
                    pass
                client = None
                await asyncio.sleep(delay)

        raise ExchangeConnectionError(
            f"Failed to connect to '{self._exchange_id}' after {_INIT_RETRIES} attempts"
        ) from last_exc
