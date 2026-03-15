"""
connectors/exchange_client_async.py
===================================

Cliente asíncrono centralizado para exchanges usando ccxt.

Responsabilidad
---------------
Gestionar el ciclo de vida del cliente ccxt:
• inicialización
• validación de conexión
• acceso al cliente
• cierre seguro

NO descarga datos.
NO gestiona retries de fetch.
NO implementa circuit breakers.

Principios
----------
SOLID   – SRP: solo lifecycle del cliente
DRY     – construcción del cliente en un único lugar
KISS    – wrapper mínimo sobre ccxt
SafeOps – cierre seguro, credenciales protegidas
"""

from __future__ import annotations

import asyncio
import random
import time
from typing import Optional

import ccxt.async_support as ccxt
from loguru import logger

from core.config.schema import AppConfig


# ==========================================================
# Constants
# ==========================================================

_INIT_RETRIES = 3
_BACKOFF_BASE = 2.0
_LOAD_MARKETS_TIMEOUT = 30.0

_DEFAULT_EXCHANGE = "binance"

_CCXT_OPTIONS = {
    "enableRateLimit": True,
    "options": {
        "adjustForTimeDifference": True,
        "recvWindow": 10_000,
    },
}


# ==========================================================
# Exceptions
# ==========================================================

class ExchangeClientError(Exception):
    """Base error."""


class UnsupportedExchangeError(ExchangeClientError):
    """Exchange no soportado por ccxt."""


class ExchangeConnectionError(ExchangeClientError):
    """No se pudo establecer conexión."""


# ==========================================================
# ExchangeClientAsync
# ==========================================================

class ExchangeClientAsync:
    """
    Cliente ccxt asíncrono.

    Inicialización lazy y thread-safe.

    Uso
    ---
    async with ExchangeClientAsync(app_config=config) as client:
        exchange = await client.get_client()
    """

    def __init__(
        self,
        app_config: Optional[AppConfig] = None,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        exchange_id: Optional[str] = None,
    ) -> None:

        self._exchange_id = _resolve_exchange_id(app_config, exchange_id)

        self._api_key = _resolve_api_key(app_config, api_key)
        self._api_secret = _resolve_api_secret(app_config, api_secret)

        self._client: Optional[ccxt.Exchange] = None

        # lock evita doble inicialización concurrente
        self._init_lock = asyncio.Lock()

    # ------------------------------------------------------
    # Public API
    # ------------------------------------------------------

    async def get_client(self) -> ccxt.Exchange:
        """Devuelve cliente ccxt inicializado."""

        if self._client is not None:
            return self._client

        async with self._init_lock:
            if self._client is None:
                await self._initialize()

        return self._client

    async def test_connection(self) -> bool:
        """
        Test de conexión.

        SafeOps: nunca lanza excepción.
        """

        try:
            client = await self.get_client()

            start = time.perf_counter()

            markets = await asyncio.wait_for(
                client.load_markets(),
                timeout=_LOAD_MARKETS_TIMEOUT,
            )

            latency = (time.perf_counter() - start) * 1000

            logger.info(
                "Exchange OK | exchange={} markets={} latency={:.1f}ms",
                self._exchange_id,
                len(markets),
                latency,
            )

            return True

        except Exception as exc:
            logger.error(
                "Exchange connection failed | exchange={} error={}",
                self._exchange_id,
                exc,
            )
            return False

    async def close(self) -> None:
        """Cierre seguro."""

        if self._client is None:
            return

        try:
            await self._client.close()
            logger.debug("Exchange connection closed | {}", self._exchange_id)

        except Exception as exc:
            logger.warning(
                "Error closing exchange | {} | {}",
                self._exchange_id,
                exc,
            )

        finally:
            self._client = None

    # ------------------------------------------------------
    # Context manager
    # ------------------------------------------------------

    async def __aenter__(self) -> "ExchangeClientAsync":
        return self

    async def __aexit__(self, *_):
        await self.close()

    # ------------------------------------------------------
    # Initialization
    # ------------------------------------------------------

    async def _initialize(self) -> None:

        if not hasattr(ccxt, self._exchange_id):
            raise UnsupportedExchangeError(
                f"Exchange '{self._exchange_id}' not supported"
            )

        exchange_class = getattr(ccxt, self._exchange_id)

        last_exc: Optional[Exception] = None

        for attempt in range(1, _INIT_RETRIES + 1):

            try:

                client = exchange_class({
                    **_CCXT_OPTIONS,
                    "apiKey": self._api_key,
                    "secret": self._api_secret,
                })

                start = time.perf_counter()

                await asyncio.wait_for(
                    client.load_markets(),
                    timeout=_LOAD_MARKETS_TIMEOUT,
                )

                latency = (time.perf_counter() - start) * 1000

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
                    "load_markets timeout | exchange={} attempt={}",
                    self._exchange_id,
                    attempt,
                )

            except Exception as exc:
                last_exc = exc

                delay = (_BACKOFF_BASE ** attempt) + random.random()

                logger.warning(
                    "Connection attempt failed | exchange={} attempt={} wait={:.1f}s error={}",
                    self._exchange_id,
                    attempt,
                    delay,
                    exc,
                )

                await asyncio.sleep(delay)

        raise ExchangeConnectionError(
            f"Failed to connect to '{self._exchange_id}'"
        ) from last_exc


# ==========================================================
# Credential resolvers
# ==========================================================

def _resolve_exchange_id(
    config: Optional[AppConfig],
    explicit: Optional[str],
) -> str:

    if explicit:
        return explicit.lower()

    if config and config.exchanges:
        return config.exchanges[0].name.value

    return _DEFAULT_EXCHANGE


def _resolve_api_key(
    config: Optional[AppConfig],
    explicit: Optional[str],
) -> str:

    if explicit:
        return explicit

    if config and config.exchanges:
        return config.exchanges[0].api_key.get_secret_value()

    raise ExchangeClientError("No api_key available")


def _resolve_api_secret(
    config: Optional[AppConfig],
    explicit: Optional[str],
) -> str:

    if explicit:
        return explicit

    if config and config.exchanges:
        return config.exchanges[0].api_secret.get_secret_value()

    raise ExchangeClientError("No api_secret available")