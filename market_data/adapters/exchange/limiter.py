"""
market_data/adapters/exchange/limiter.py
=========================================
AdaptiveLimiter — rate limiter que sincroniza con AdaptiveThrottle.

Responsabilidad
---------------
• Aplicar HARD LIMIT de requests (concurrencia + rate limit)
• Recibir actualizaciones de concurrencia desde AdaptiveThrottle
• Ser el "músculo" que ejecuta los límites decididos por el throttle

Arquitectura
------------
El limiter es estático por diseño — NO toma decisiones.
Solo aplica los límites que el AdaptiveThrottle decide.

Flujo: AdaptiveThrottle → update_concurrency() → limiter enforce
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Optional

from loguru import logger

if TYPE_CHECKING:
    from market_data.adapters.exchange.throttle import AdaptiveThrottle

__all__ = [
    "AdaptiveLimiter",
    "_LIMITERS",
    "get_or_create_limiter",
    "get_limiter_state",
]


_LIMITERS: dict[str, "AdaptiveLimiter"] = {}


class AdaptiveLimiter:
    """
    Rate limiter que sincroniza con AdaptiveThrottle.

    El throttle es el cerebro que decide.
    El limiter es el músculo que ejecuta.

    Uso
    ---
    limiter = AdaptiveLimiter(
        exchange_id="bybit",
        max_concurrency=5,
        max_rate=10.0,
    )
    throttle = AdaptiveThrottle(
        exchange_id="bybit",
        limiter=limiter,
    )

    # El throttle actualiza el limiter cuando escala
    async with limiter:
        await limiter.acquire()
        result = await ccxt.fetch_ohlcv(...)
    """

    def __init__(
        self,
        exchange_id: str,
        max_concurrency: int = 5,
        max_rate: float = 10.0,
        throttle: Optional["AdaptiveThrottle"] = None,
    ) -> None:
        """
        Parameters
        ----------
        exchange_id     : identificador único del exchange
        max_concurrency: concurrencia máxima inicial
        max_rate       : requests por segundo máximo
        throttle       : referencia al throttle (para actualizaciones de concurrencia)
        """
        self._exchange_id = exchange_id
        self._max_concurrency = max_concurrency
        self._max_rate = max_rate
        self._throttle = throttle

        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._min_interval = 1.0 / max_rate if max_rate > 0 else 0.0
        self._last_acquire = 0.0

        self._closed = False

    @property
    def max_concurrency(self) -> int:
        """Concurrencia máxima actual."""
        return self._max_concurrency

    @property
    def max_rate(self) -> float:
        """Rate limit actual (req/s)."""
        return self._max_rate

    async def acquire(self) -> None:
        """Adquiere un slot del limiter (hard limit)."""
        await self._semaphore.acquire()
        try:
            if self._min_interval > 0:
                now = time.monotonic()
                elapsed = now - self._last_acquire
                if elapsed < self._min_interval:
                    await asyncio.sleep(self._min_interval - elapsed)
                self._last_acquire = time.monotonic()
        except Exception:
            self._semaphore.release()
            raise

    def release(self) -> None:
        """Libera un slot del semaphore."""
        self._semaphore.release()

    def update_concurrency(self, n: int) -> None:
        """
        Actualiza la concurrencia máxima del limiter.

        Llamado por AdaptiveThrottle cuando detecta presión
        y decide reducir/aumentar la concurrencia.

        Parameters
        ----------
        n : nueva concurrencia máxima
        """
        n = max(1, n)
        if n == self._max_concurrency:
            return

        self._max_concurrency = n
        self._semaphore = asyncio.Semaphore(n)
        logger.debug(
            "AdaptiveLimiter updated | exchange={} max_concurrency={}",
            self._exchange_id,
            n,
        )

    def update_rate(self, rate: float) -> None:
        """
        Actualiza el rate limit del limiter.

        Parameters
        ----------
        rate : nuevo rate limit en requests/segundo
        """
        rate = max(0.1, rate)
        if rate == self._max_rate:
            return

        self._max_rate = rate
        self._min_interval = 1.0 / rate if rate > 0 else 0.0
        logger.debug(
            "AdaptiveLimiter updated | exchange={} max_rate={:.1f}",
            self._exchange_id,
            rate,
        )

    async def __aenter__(self) -> "AdaptiveLimiter":
        return self

    async def __aexit__(self, *args) -> None:
        pass

    def state(self) -> dict:
        """Estado observable del limiter para métricas."""
        return {
            "exchange_id": self._exchange_id,
            "max_concurrency": self._max_concurrency,
            "max_rate": self._max_rate,
        }


def get_or_create_limiter(
    exchange_id: str,
    max_concurrency: int = 5,
    max_rate: float = 10.0,
    throttle: Optional["AdaptiveThrottle"] = None,
) -> AdaptiveLimiter:
    """
    Retorna o crea un limiter para un exchange.

    Singleton por exchange — mismo exchange comparte limiter.
    """
    if exchange_id not in _LIMITERS:
        _LIMITERS[exchange_id] = AdaptiveLimiter(
            exchange_id=exchange_id,
            max_concurrency=max_concurrency,
            max_rate=max_rate,
            throttle=throttle,
        )
    return _LIMITERS[exchange_id]


def get_limiter_state(exchange_id: str) -> dict:
    """Estado observable del limiter."""
    limiter = _LIMITERS.get(exchange_id)
    if limiter is None:
        return {
            "exchange_id": exchange_id,
            "max_concurrency": 0,
            "max_rate": 0.0,
        }
    return limiter.state()
