"""
HistoricalFetcherAsync – OrangeCashMachine
==========================================

Fetcher profesional de datos históricos OHLCV.

Responsabilidades
-----------------

• Descargar datos OHLCV desde exchange
• Manejar retries y rate limits
• Aplicar circuit breaker
• Transformar datos antes de almacenarlos
• Soportar descargas incrementales

Principios aplicados
--------------------

• SOLID
• DRY
• KISS
• SafeOps
"""

from __future__ import annotations

import asyncio
from typing import Optional, List

import pandas as pd
import pybreaker
from loguru import logger

from market_data.batch.storage.historical_storage import HistoricalStorage
from market_data.batch.transformers.transformer import OHLCVTransformer
from market_data.connectors.exchange_client_async import ExchangeClientAsync


# ==========================================================
# Exceptions
# ==========================================================

class HistoricalFetcherAsyncError(Exception):
    """Errores del fetcher."""


# ==========================================================
# Adaptive Throttler
# ==========================================================

class AdaptiveThrottlerAsync:
    """
    Control dinámico de concurrencia para APIs externas.
    """

    def __init__(
        self,
        initial_limit: int = 6,
        min_limit: int = 1,
        max_limit: int = 12,
    ):

        self.limit = initial_limit
        self.min_limit = min_limit
        self.max_limit = max_limit

        self._semaphore = asyncio.Semaphore(initial_limit)
        self._lock = asyncio.Lock()

    async def acquire(self):
        await self._semaphore.acquire()

    def release(self):
        self._semaphore.release()

    async def decrease(self):

        async with self._lock:

            if self.limit > self.min_limit:
                self.limit -= 1
                logger.warning(f"Concurrency reduced → {self.limit}")

    async def increase(self):

        async with self._lock:

            if self.limit < self.max_limit:
                self.limit += 1
                logger.info(f"Concurrency increased → {self.limit}")


# ==========================================================
# Fetcher
# ==========================================================

class HistoricalFetcherAsync:
    """
    Fetcher profesional para datasets OHLCV históricos.
    """

    DEFAULT_LIMIT = 500
    MAX_RETRIES = 5
    BACKOFF_BASE = 1.6

    OHLCV_COLUMNS = [
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]

    VALID_UNITS = {
        "m": 60_000,
        "h": 3_600_000,
        "d": 86_400_000,
    }

    # ------------------------------------------------------

    def __init__(
        self,
        exchange_client: Optional[ExchangeClientAsync] = None,
        storage: Optional[HistoricalStorage] = None,
        transformer: Optional[OHLCVTransformer] = None,
        max_concurrent: int = 6,
    ):

        self.exchange_client = exchange_client or ExchangeClientAsync()
        self.storage = storage or HistoricalStorage()
        self.transformer = transformer or OHLCVTransformer

        self.throttler = AdaptiveThrottlerAsync(initial_limit=max_concurrent)

        self.circuit_breaker = pybreaker.CircuitBreaker(
            fail_max=5,
            reset_timeout=30,
        )

    # ------------------------------------------------------
    # Utilities
    # ------------------------------------------------------

    @classmethod
    def _timeframe_to_ms(cls, timeframe: str) -> int:

        try:

            unit = timeframe[-1]
            value = int(timeframe[:-1])

        except Exception:
            raise HistoricalFetcherAsyncError(
                f"Invalid timeframe format: {timeframe}"
            )

        if unit not in cls.VALID_UNITS:
            raise HistoricalFetcherAsyncError(
                f"Invalid timeframe unit: {unit}"
            )

        return value * cls.VALID_UNITS[unit]

    # ------------------------------------------------------

    async def _determine_start_ts(
        self,
        symbol: str,
        timeframe: str,
        start_date: Optional[str],
    ) -> int:

        last_ts = self.storage.get_last_timestamp(symbol, timeframe)

        if last_ts is not None:

            logger.info(
                f"Incremental download → {symbol} {timeframe} from {last_ts}"
            )

            return int(last_ts.timestamp() * 1000)

        if start_date is None:
            raise HistoricalFetcherAsyncError(
                "start_date required when no historical data exists"
            )

        client = await self.exchange_client.get_client()

        ts = client.parse8601(start_date)

        logger.info(
            f"Initial download → {symbol} {timeframe} from {start_date}"
        )

        return ts

    # ------------------------------------------------------

    async def _fetch_chunk(
        self,
        symbol: str,
        timeframe: str,
        since: int,
        limit: int,
    ) -> List[list]:

        client = await self.exchange_client.get_client()

        for attempt in range(1, self.MAX_RETRIES + 1):

            try:

                async def call():
                    return await client.fetch_ohlcv(
                        symbol,
                        timeframe,
                        since=since,
                        limit=limit,
                    )

                data = await self.circuit_breaker.call_async(call)

                await self.throttler.increase()

                return data

            except Exception as e:

                await self.throttler.decrease()

                wait = self.BACKOFF_BASE ** attempt

                logger.warning(
                    f"Fetch retry {attempt}/{self.MAX_RETRIES} "
                    f"{symbol} {timeframe} error={e} wait={wait:.2f}s"
                )

                await asyncio.sleep(wait)

        logger.error(f"Fetch failed → {symbol} {timeframe}")

        return []

    # ------------------------------------------------------
    # Chunk Downloader
    # ------------------------------------------------------

    async def download_data_chunked(
        self,
        symbol: str,
        timeframe: str,
        start_date: Optional[str] = None,
        limit: int = DEFAULT_LIMIT,
    ) -> List[pd.DataFrame]:

        since_ts = await self._determine_start_ts(
            symbol,
            timeframe,
            start_date,
        )

        delta_ms = self._timeframe_to_ms(timeframe)

        chunks: List[pd.DataFrame] = []
        total_rows = 0

        while True:

            await self.throttler.acquire()

            try:

                raw = await self._fetch_chunk(
                    symbol,
                    timeframe,
                    since_ts,
                    limit,
                )

            finally:

                self.throttler.release()

            if not raw:
                break

            df = pd.DataFrame(raw, columns=self.OHLCV_COLUMNS)

            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")

            df = self.transformer.transform(df)

            if df.empty:
                break

            total_rows += len(df)

            chunks.append(df)

            last_ts = df["timestamp"].max()

            since_ts = int(last_ts.timestamp() * 1000) + delta_ms

            if len(raw) < limit:
                break

        logger.success(
            f"Downloaded {total_rows} rows → {symbol} {timeframe}"
        )

        return chunks

    # ------------------------------------------------------
    # Full Download
    # ------------------------------------------------------

    async def download_data(
        self,
        symbol: str,
        timeframe: str,
        start_date: Optional[str] = None,
        limit: int = DEFAULT_LIMIT,
    ) -> pd.DataFrame:

        chunks = await self.download_data_chunked(
            symbol,
            timeframe,
            start_date,
            limit,
        )

        if not chunks:
            return pd.DataFrame(columns=self.OHLCV_COLUMNS)

        return (
            pd.concat(chunks)
            .sort_values("timestamp")
            .reset_index(drop=True)
        )

    # ------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------

    async def close(self):
        """
        Cierra conexiones async con el exchange.
        """

        await self.exchange_client.close()