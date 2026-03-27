"""
services/exchange/ccxt_adapter.py
==================================

CCXT Exchange Adapter — única fuente de verdad del cliente ccxt.

Responsabilidad
---------------
• Construir y gestionar lifecycle del cliente ccxt async
• Resolver credenciales desde ExchangeConfig o parámetros explícitos
• Exponer interfaz limpia para consumo por fetchers y pipelines
• NO contiene lógica de negocio
• NO depende de AppConfig global

Principios
----------
SOLID   – SRP + DIP (adapter desacoplado, credenciales resueltas aquí)
DRY     – init centralizado
KISS    – API mínima y clara
SafeOps – retries, timeouts, cierre seguro

Timeouts
--------
Cada método de I/O tiene su propio asyncio.wait_for con timeout explícito.
El timeout del task de Prefect es el techo máximo — estos son los límites
operativos por llamada individual.
"""

from __future__ import annotations

import asyncio
import random
import time
from typing import Any, Dict, List, Optional, TYPE_CHECKING

import ccxt.async_support as ccxt
import pybreaker
from loguru import logger

from services.exchange.base import ExchangeAdapter

if TYPE_CHECKING:
    from core.config.schema import ExchangeConfig


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


# Circuit breaker: abre tras 5 fallos consecutivos, resetea a los 60s.
# Compartido por exchange_id — un breaker global por exchange, no por instancia.
# Evita que spot y futures del mismo exchange tengan breakers independientes
# que se disparan en cascada cuando el exchange está bajo presión.
_CB_FAIL_MAX:       int   = 10
_CB_RESET_TIMEOUT:  int   = 120

_BREAKERS: dict[str, "pybreaker.CircuitBreaker"] = {}


def _get_breaker(exchange_id: str) -> "pybreaker.CircuitBreaker":
    # setdefault es atómico en CPython — elimina race condition del if/else.
    # En worst case se construye un CircuitBreaker extra que se descarta (GC).
    return _BREAKERS.setdefault(
        exchange_id,
        pybreaker.CircuitBreaker(
            fail_max      = _CB_FAIL_MAX,
            reset_timeout = _CB_RESET_TIMEOUT,
            name          = exchange_id,
        ),
    )


# ==========================================================
# Exceptions
# ==========================================================

class ExchangeAdapterError(Exception):
    """Base adapter error."""

class UnsupportedExchangeError(ExchangeAdapterError):
    """Exchange no soportado por ccxt."""

class ExchangeConnectionError(ExchangeAdapterError):
    """Fallo de conexión tras retries."""

class ExchangeCircuitOpenError(ExchangeAdapterError):
    """Circuit breaker abierto — exchange en cooldown."""


# ==========================================================
# Breaker introspection
# ==========================================================

def get_breaker_state(exchange_id: str) -> dict:
    """
    Estado observable del circuit breaker para un exchange.

    Returns
    -------
    {
        "exchange":              str,
        "state":                 "closed" | "open" | "half-open",
        "fail_counter":          int,
        "cooldown_remaining_ms": int,  # 0 si estado != open
    }

    SafeOps: nunca lanza excepcion al caller.
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
            # pybreaker guarda el timestamp de apertura en el state object.
            # _state.opened_at es el atributo correcto en pybreaker >= 0.6.
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



# ==========================================================
# Adaptive Throttle — concurrencia dinámica por exchange
# ==========================================================

from collections import deque as _deque

class AdaptiveThrottle:
    """
    Controla la concurrencia activa por exchange y la ajusta en tiempo real.

    Algoritmo
    ---------
    Mantiene una ventana deslizante de los últimos N resultados (ok/error).
    Si la tasa de error supera el umbral alto  → reduce slots (-1, mínimo 1).
    Si la tasa de error cae bajo el umbral bajo → aumenta slots (+1, hasta max).
    Los ajustes solo ocurren cada MIN_CALLS llamadas para evitar oscilación.

    Thread-safety
    -------------
    Asyncio single-thread: no necesita Lock. Solo se llama desde coroutines
    dentro del mismo event loop.
    """

    _WINDOW        = 20    # tamaño de la ventana de observación
    _HIGH_ERR      = 0.30  # >30% errores → bajar concurrencia
    _LOW_ERR       = 0.05  # <5%  errores → subir concurrencia
    _MIN_CALLS     = 5     # mínimo de llamadas antes de ajustar

    def __init__(self, exchange_id: str, initial: int, maximum: int) -> None:
        self.exchange_id = exchange_id
        self._current    = max(1, min(initial, maximum))
        self._maximum    = maximum
        self._window: "_deque[bool]" = _deque(maxlen=self._WINDOW)
        self._calls_since_adjust = 0

    @property
    def current(self) -> int:
        return self._current

    def record_success(self) -> None:
        self._window.append(True)
        self._calls_since_adjust += 1
        self._maybe_scale_up()

    def record_error(self) -> None:
        self._window.append(False)
        self._calls_since_adjust += 1
        self._maybe_scale_down()

    def _error_rate(self) -> float:
        if not self._window:
            return 0.0
        return sum(1 for ok in self._window if not ok) / len(self._window)

    def _maybe_scale_down(self) -> None:
        if self._calls_since_adjust < self._MIN_CALLS:
            return
        if self._error_rate() > self._HIGH_ERR and self._current > 1:
            self._current = max(1, int(self._current * 0.75))
            self._calls_since_adjust = 0
            from loguru import logger as _log
            _log.warning(
                "AdaptiveThrottle scale DOWN | exchange={} concurrent={} error_rate={:.0%}",
                self.exchange_id, self._current, self._error_rate(),
            )

    def _maybe_scale_up(self) -> None:
        if self._calls_since_adjust < self._MIN_CALLS:
            return
        if self._error_rate() < self._LOW_ERR and self._current < self._maximum:
            self._current = min(self._maximum, self._current + 1)
            self._calls_since_adjust = 0
            from loguru import logger as _log
            _log.debug(
                "AdaptiveThrottle scale UP | exchange={} concurrent={}",
                self.exchange_id, self._current,
            )


_THROTTLES: dict[str, "AdaptiveThrottle"] = {}


def get_or_create_throttle(
    exchange_id: str,
    initial:     int,
    maximum:     int,
) -> "AdaptiveThrottle":
    """
    Singleton de AdaptiveThrottle por exchange_id.

    Si ya existe un throttle para el exchange, lo devuelve sin modificar
    sus contadores internos — permite que el estado persista entre tasks
    del mismo run. Si no existe, lo crea con initial/maximum del probe.
    """
    if exchange_id not in _THROTTLES:
        _THROTTLES[exchange_id] = AdaptiveThrottle(
            exchange_id=exchange_id,
            initial=initial,
            maximum=maximum,
        )
    return _THROTTLES[exchange_id]


def get_throttle_state(exchange_id: str) -> dict:
    """Estado observable del throttle. SafeOps: nunca lanza excepción."""
    try:
        t = _THROTTLES.get(exchange_id)
        if t is None:
            return {"exchange": exchange_id, "concurrent": 0, "error_rate": 0.0}
        return {
            "exchange":    exchange_id,
            "concurrent":  t.current,
            "error_rate":  t._error_rate(),
            "maximum":     t._maximum,
        }
    except Exception:
        return {"exchange": exchange_id, "concurrent": 0, "error_rate": 0.0}


# ==========================================================
# Adapter
# ==========================================================

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

    async def __aenter__(self) -> "CCXTAdapter":
        await self.connect()
        return self

    async def __aexit__(self, *_) -> None:
        await self.close()

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
            return await self._breaker.call_async(_call)
        except pybreaker.CircuitBreakerError as exc:
            from services.observability.metrics import EXCHANGE_CIRCUIT_OPEN
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
        if self._exchange_id == "kucoin" and since is not None and not effective_type:
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
                return await self._breaker.call_async(_call)
            except asyncio.TimeoutError:
                # Timeout de red — NO cuenta como fallo de breaker.
                # Re-raise para que el fetcher lo trate como error transitorio.
                raise
        except pybreaker.CircuitBreakerError as exc:
            from services.observability.metrics import EXCHANGE_CIRCUIT_OPEN
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
            return await self._breaker.call_async(_call)
        except pybreaker.CircuitBreakerError as exc:
            from services.observability.metrics import EXCHANGE_CIRCUIT_OPEN
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

    async def test_connection(self) -> bool:
        """Test de conectividad. SafeOps: nunca lanza excepción."""
        try:
            start   = time.perf_counter()
            markets = await self.load_markets()
            latency = (time.perf_counter() - start) * 1000
            logger.info(
                "Exchange OK | {} markets={} latency={:.1f}ms",
                self._exchange_id, len(markets), latency,
            )
            return True
        except Exception as exc:
            logger.error(
                "Exchange connection failed | {} error={}",
                self._exchange_id, exc,
            )
            return False

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
