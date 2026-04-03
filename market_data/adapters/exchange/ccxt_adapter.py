"""
services/exchange/ccxt_adapter.py
==================================

CCXT Exchange Adapter — única fuente de verdad del cliente ccxt.

Responsabilidad
---------------
• Construir y gestionar lifecycle del cliente ccxt async
• Resolver credenciales desde ExchangeConfig o parámetros explícitos
• Exponer interfaz limpia para fetchers y pipelines
• NO contiene lógica de negocio  •  NO depende de AppConfig global

Módulos relacionados
--------------------
errors.py          — clases de excepción
circuit_breaker.py — circuit breaker compartido por exchange
throttle.py        — concurrencia adaptiva por pipeline
"""
from __future__ import annotations

import asyncio
import random
import time
from typing import Any, Dict, List, Optional, TYPE_CHECKING

import ccxt.async_support as ccxt
import pybreaker
from loguru import logger

from market_data.adapters.exchange.base import ExchangeAdapter
from market_data.adapters.exchange.errors import (
    ExchangeAdapterError,
    UnsupportedExchangeError,
    ExchangeConnectionError,
    ExchangeCircuitOpenError,
)
from market_data.adapters.exchange.circuit_breaker import _get_breaker, get_breaker_state, breaker_call_async
from market_data.adapters.exchange.throttle import (
    AdaptiveThrottle,
    _THROTTLES,
    get_or_create_throttle,
    get_throttle_state,
)

if TYPE_CHECKING:
    from core.config.schema import ExchangeConfig

# Backward-compat re-exports — los importadores existentes no necesitan cambios
__all__ = [
    "CCXTAdapter",
    "ExchangeAdapterError",
    "UnsupportedExchangeError",
    "ExchangeConnectionError",
    "ExchangeCircuitOpenError",
    "get_breaker_state",
    "AdaptiveThrottle",
    "get_or_create_throttle",
    "get_throttle_state",
]


# ==========================================================
# Constants
# ==========================================================

_INIT_RETRIES          = 3
_BACKOFF_BASE          = 2.0
_LOAD_MARKETS_TIMEOUT  = 30.0
_DEFAULT_EXCHANGE      = "binance"
_MARKETS_CACHE_TTL     = 60.0   # segundos — reutilizar markets en reconnect

# Timeouts por operación (segundos)
_FETCH_TICKER_TIMEOUT  = 10.0
_FETCH_OHLCV_TIMEOUT   = 30.0
_FETCH_TRADES_TIMEOUT  = 15.0


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
        exchange_id:  Optional[str] = None,
        api_key:      Optional[str] = None,
        api_secret:   Optional[str] = None,
        config:       Optional["ExchangeConfig"] = None,
        default_type: Optional[str] = None,
    ) -> None:

        self._exchange_id  = self._resolve_exchange_id(exchange_id, config)
        self._api_key      = self._resolve_api_key(api_key, config)
        self._api_secret   = self._resolve_api_secret(api_secret, config)
        self._api_password = self._resolve_api_password(config)
        self._default_type = default_type or self._resolve_default_type(config)

        self._client:            Optional[ccxt.Exchange] = None
        self._init_lock:         asyncio.Lock            = asyncio.Lock()
        self._markets_cache:     Optional[Dict[str, Any]] = None
        self._markets_cached_at: float = 0.0
        # Circuit breaker compartido por exchange_id (singleton global).
        # Spot y futures del mismo exchange comparten el mismo breaker —
        # si bybit está degradado, ambos pipelines lo detectan juntos.
        self._breaker = _get_breaker(self._exchange_id)

    # ----------------------------------------------------------
    # Lifecycle
    # ----------------------------------------------------------

    async def connect(self) -> None:
        """Inicializa cliente y carga mercados (idempotente)."""
        if self._client is not None:
            return
        async with self._init_lock:
            if self._client is None:
                await self._initialize()

    async def reconnect(self) -> None:
        """Fuerza reconexión cerrando el cliente actual (thread-safe)."""
        async with self._init_lock:
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
            if hasattr(session, "closed") and session.closed:
                return False
            session_loop = getattr(session, "_loop", None) or getattr(session, "connector", None)
            if session_loop is not None:
                try:
                    current_loop = asyncio.get_running_loop()
                    loop = getattr(session, "_loop", None)
                    if loop is not None and loop is not current_loop:
                        return False
                except RuntimeError:
                    pass
            return True
        except Exception:
            return False

    async def close(self) -> None:
        """Cierre seguro e idempotente — nunca lanza excepción."""
        async with self._init_lock:
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
        try:
            async def _call():
                return await asyncio.wait_for(
                    client.fetch_ticker(symbol),
                    timeout=_FETCH_TICKER_TIMEOUT,
                )
            return await breaker_call_async(self._breaker, _call)
        except pybreaker.CircuitBreakerError as exc:
            from market_data.observability.metrics import EXCHANGE_CIRCUIT_OPEN
            EXCHANGE_CIRCUIT_OPEN.labels(
                exchange=self._exchange_id, operation="fetch_ticker"
            ).inc()
            raise ExchangeCircuitOpenError(
                f"Circuit open for '{self._exchange_id}' — ticker unavailable"
            ) from exc

    async def fetch_ohlcv(
        self,
        symbol:      str,
        timeframe:   str,
        since:       Optional[int] = None,
        limit:       int = 100,
        market_type: Optional[str] = None,
    ) -> List[List[Any]]:
        client = await self._get_client()
        params: Dict[str, Any] = {}
        effective_type = market_type or self._default_type
        if effective_type:
            params["defaultType"] = effective_type
        if self._exchange_id in ("kucoin", "kucoinfutures") and since is not None:
            # since=0 no es válido — estos exchanges lo rechazan
            if since == 0:
                since = None
            elif self._exchange_id == "kucoin" and not effective_type:
                now_ts = int(time.time())
                if since > now_ts:
                    params["endAt"] = now_ts
        try:
            async def _call():
                return await asyncio.wait_for(
                    client.fetch_ohlcv(
                        symbol, timeframe,
                        since=since, limit=limit, params=params,
                    ),
                    timeout=_FETCH_OHLCV_TIMEOUT,
                )
            try:
                return await breaker_call_async(self._breaker, _call)
            except asyncio.TimeoutError:
                # Timeout de red — NO cuenta como fallo de breaker.
                # Re-raise para que el fetcher lo trate como error transitorio.
                raise
        except pybreaker.CircuitBreakerError as exc:
            from market_data.observability.metrics import EXCHANGE_CIRCUIT_OPEN
            EXCHANGE_CIRCUIT_OPEN.labels(
                exchange=self._exchange_id, operation="fetch_ohlcv"
            ).inc()
            raise ExchangeCircuitOpenError(
                f"Circuit open for '{self._exchange_id}' — ohlcv unavailable"
            ) from exc

    async def fetch_trades(
        self,
        symbol: str,
        limit:  int = 100,
    ) -> List[Dict[str, Any]]:
        client = await self._get_client()
        try:
            async def _call():
                return await asyncio.wait_for(
                    client.fetch_trades(symbol, limit=limit),
                    timeout=_FETCH_TRADES_TIMEOUT,
                )
            return await breaker_call_async(self._breaker, _call)
        except pybreaker.CircuitBreakerError as exc:
            from market_data.observability.metrics import EXCHANGE_CIRCUIT_OPEN
            EXCHANGE_CIRCUIT_OPEN.labels(
                exchange=self._exchange_id, operation="fetch_trades"
            ).inc()
            raise ExchangeCircuitOpenError(
                f"Circuit open for '{self._exchange_id}' — trades unavailable"
            ) from exc

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
    # Credential resolvers (privados — SRP)
    # ----------------------------------------------------------

    @staticmethod
    def _resolve_exchange_id(
        explicit: Optional[str],
        config:   Optional["ExchangeConfig"],
    ) -> str:
        if explicit:
            return explicit.lower()
        if config is not None:
            return config.name.value
        return _DEFAULT_EXCHANGE

    @staticmethod
    def _resolve_api_key(
        explicit: Optional[str],
        config:   Optional["ExchangeConfig"],
    ) -> Optional[str]:
        if explicit:
            return explicit
        if config is not None and config.has_credentials:
            return config.api_key.get_secret_value() or None
        return None

    @staticmethod
    def _resolve_api_secret(
        explicit: Optional[str],
        config:   Optional["ExchangeConfig"],
    ) -> Optional[str]:
        if explicit:
            return explicit
        if config is not None and config.has_credentials:
            return config.api_secret.get_secret_value() or None
        return None

    @staticmethod
    def _resolve_api_password(
        config: Optional["ExchangeConfig"],
    ) -> Optional[str]:
        if config is not None and config.has_passphrase:
            return config.api_password.get_secret_value() or None
        return None

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
        if self._client is not None:
            return self._client
        async with self._init_lock:
            if self._client is None:
                await self._initialize()
        return self._client

    async def _initialize(self) -> None:
        if not hasattr(ccxt, self._exchange_id):
            raise UnsupportedExchangeError(
                f"Exchange '{self._exchange_id}' not supported by ccxt"
            )

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
                cache_valid = (
                    self._markets_cache is not None
                    and (now - self._markets_cached_at) < _MARKETS_CACHE_TTL
                )

                if cache_valid:
                    client.markets = self._markets_cache
                    latency = 0.0
                    logger.debug(
                        "Exchange connected (markets from cache) | {} cache_age={:.1f}s",
                        self._exchange_id, now - self._markets_cached_at,
                    )
                else:
                    start = time.perf_counter()
                    await asyncio.wait_for(
                        client.load_markets(),
                        timeout=_LOAD_MARKETS_TIMEOUT,
                    )
                    latency = (time.perf_counter() - start) * 1000
                    self._markets_cache     = client.markets
                    self._markets_cached_at = time.perf_counter()

                self._client = client
                logger.info(
                    "Exchange connected | {} latency={:.1f}ms",
                    self._exchange_id, latency,
                )
                return

            except asyncio.TimeoutError as exc:
                last_exc = exc
                logger.warning(
                    "load_markets timeout | {} attempt={}",
                    self._exchange_id, attempt,
                )

            except Exception as exc:
                last_exc = exc
                delay = (_BACKOFF_BASE ** attempt) + random.random()
                logger.warning(
                    "Connection failed | {} attempt={} retry_in={:.1f}s error={}",
                    self._exchange_id, attempt, delay, exc,
                )
                await asyncio.sleep(delay)

        raise ExchangeConnectionError(
            f"Failed to connect to '{self._exchange_id}' after {_INIT_RETRIES} attempts"
        ) from last_exc
