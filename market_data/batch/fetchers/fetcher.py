"""
fetcher.py
==========

Fetcher profesional de datos históricos OHLCV.

Responsabilidades
-----------------
• Descargar OHLCV histórico desde exchange via ccxt.async_support
• Soportar descargas incrementales
• Manejar paginación robusta
• Implementar retries con backoff exponencial
• Proteger con circuit breaker
• Transformar y validar cada chunk

Principios
----------
SOLID
    SRP  – Solo descarga datos
    DIP  – client, storage y transformer inyectables

DRY
    Retry centralizado

KISS
    Pipeline controla throttling

SafeOps
    Circuit breaker
    logs estructurados
    protecciones anti-loop
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import List, Optional

import pandas as pd
import pybreaker
from loguru import logger

from market_data.batch.storage.storage import HistoricalStorage
from market_data.batch.transformers.transformer import OHLCVTransformer
from services.exchange.ccxt_adapter import CCXTAdapter
from core.config.schema import AppConfig


# ==========================================================
# Constants
# ==========================================================

DEFAULT_CHUNK_LIMIT: int = 500
MAX_RETRIES: int = 5
BACKOFF_BASE: float = 1.6
MAX_CHUNKS_PER_RUN: int = 100_000  # SafeOps guard

OHLCV_COLUMNS: tuple[str, ...] = (
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
)

_TIMEFRAME_UNIT_MS: dict[str, int] = {
    "m": 60_000,
    "h": 3_600_000,
    "d": 86_400_000,
    "w": 604_800_000,
}

_CIRCUIT_BREAKER_FAIL_MAX: int = 5
_CIRCUIT_BREAKER_RESET_TIMEOUT: int = 60


# ==========================================================
# Exceptions
# ==========================================================

class FetcherError(Exception):
    """Base error for fetcher."""


class InvalidTimeframeError(FetcherError):
    """Invalid timeframe format."""


class MissingStartDateError(FetcherError):
    """Start date required for first download."""


class ChunkFetchError(FetcherError):
    """Chunk download failed after retries."""


# ==========================================================
# Download Result
# ==========================================================

@dataclass(slots=True)
class DownloadResult:
    """Typed result of a full download."""

    symbol: str
    timeframe: str
    df: pd.DataFrame
    chunks: int = 0
    total_rows: int = 0

    @property
    def has_data(self) -> bool:
        return not self.df.empty


# ==========================================================
# HistoricalFetcherAsync
# ==========================================================

class HistoricalFetcherAsync:
    """
    Async OHLCV historical downloader.

    Robust downloader designed for large scale pipelines.
    """

    def __init__(
        self,
        config: Optional[AppConfig] = None,
        exchange_client: Optional[CCXTAdapter] = None,
        storage: Optional[HistoricalStorage] = None,
        transformer: Optional[OHLCVTransformer] = None,
    ) -> None:

        self._config = config

        if exchange_client is None:
            raise ValueError(
                "exchange_client is required. "
                "HistoricalFetcherAsync never decides which exchange to use — "
                "that responsibility belongs to the pipeline/orchestrator (DIP)."
            )
        self._exchange_client = exchange_client

        self._storage = storage or HistoricalStorage()

        self._transformer = transformer or OHLCVTransformer()

        self._circuit_breaker = pybreaker.CircuitBreaker(
            fail_max=_CIRCUIT_BREAKER_FAIL_MAX,
            reset_timeout=_CIRCUIT_BREAKER_RESET_TIMEOUT,
        )

    # ----------------------------------------------------------
    # Public API
    # ----------------------------------------------------------

    async def download_data(
        self,
        symbol: str,
        timeframe: str,
        start_date: Optional[str] = None,
        limit: int = DEFAULT_CHUNK_LIMIT,
    ) -> pd.DataFrame:
        """
        Download OHLCV historical data.

        Returns
        -------
        pd.DataFrame
        """

        self._validate_inputs(symbol, timeframe, limit)

        result = await self._download_chunked(
            symbol=symbol,
            timeframe=timeframe,
            start_date=start_date,
            limit=limit,
        )

        if not result.has_data:

            logger.info(
                "No new data | symbol={} timeframe={}",
                symbol,
                timeframe,
            )

            return pd.DataFrame(columns=list(OHLCV_COLUMNS))

        logger.info(
            "Download complete | symbol={} timeframe={} chunks={} rows={}",
            symbol,
            timeframe,
            result.chunks,
            result.total_rows,
        )

        return result.df

    async def close(self) -> None:
        """Close exchange connection safely."""
        await self._exchange_client.close()

    # ----------------------------------------------------------
    # Core Download Logic
    # ----------------------------------------------------------

    async def _download_chunked(
        self,
        symbol: str,
        timeframe: str,
        start_date: Optional[str],
        limit: int,
    ) -> DownloadResult:

        since_ts = await self._resolve_start_timestamp(
            symbol,
            timeframe,
            start_date,
        )

        delta_ms = _timeframe_to_ms(timeframe)

        collected: List[pd.DataFrame] = []

        total_rows = 0
        last_seen_ts: Optional[int] = None

        chunk_counter = 0

        while True:

            if chunk_counter > MAX_CHUNKS_PER_RUN:

                raise FetcherError(
                    "Maximum chunk limit reached (possible infinite loop)"
                )

            raw = await self._fetch_chunk_with_retry(
                symbol,
                timeframe,
                since_ts,
                limit,
            )

            if not raw:
                break

            chunk_df = _raw_to_dataframe(raw)

            chunk_df = self._transformer.transform(chunk_df)

            if chunk_df.empty:
                break

            collected.append(chunk_df)

            total_rows += len(chunk_df)
            chunk_counter += 1

            last_ts = int(chunk_df["timestamp"].max().timestamp() * 1000)

            if last_seen_ts == last_ts:

                logger.warning(
                    "Duplicate timestamp detected | symbol={} timeframe={} since_ts={} last_ts={}",
                    symbol,
                    timeframe,
                    since_ts,
                    last_ts,
                )

                break

            last_seen_ts = last_ts
            since_ts = last_ts + delta_ms

            if len(raw) < limit:
                break

        if not collected:
            return DownloadResult(symbol, timeframe, pd.DataFrame())

        combined = (
            pd.concat(collected, ignore_index=True)
            .sort_values("timestamp")
            .reset_index(drop=True)
        )

        return DownloadResult(
            symbol=symbol,
            timeframe=timeframe,
            df=combined,
            chunks=len(collected),
            total_rows=total_rows,
        )

    # ----------------------------------------------------------
    # Timestamp Resolution
    # ----------------------------------------------------------

    async def _resolve_start_timestamp(
        self,
        symbol: str,
        timeframe: str,
        start_date: Optional[str],
    ) -> int:

        last_ts = self._storage.get_last_timestamp(symbol, timeframe)

        if last_ts is not None:

            logger.debug(
                "Incremental download | symbol={} timeframe={} since={}",
                symbol,
                timeframe,
                last_ts,
            )

            return int(last_ts.timestamp() * 1000)

        if start_date is None:

            raise MissingStartDateError(
                f"First download requires start_date for {symbol}/{timeframe}"
            )

        return self._exchange_client.parse8601(start_date)

    # ----------------------------------------------------------
    # Chunk Fetch
    # ----------------------------------------------------------

    async def _fetch_chunk_with_retry(
        self,
        symbol: str,
        timeframe: str,
        since: int,
        limit: int,
    ) -> List[list]:

        last_exc: Optional[Exception] = None

        for attempt in range(1, MAX_RETRIES + 1):

            try:

                if self._circuit_breaker.current_state == "open":
                    raise pybreaker.CircuitBreakerError("Circuit breaker is OPEN")
                data = await self._exchange_client.fetch_ohlcv(
                    symbol, timeframe, since=since, limit=limit,
                )

                return data

            except pybreaker.CircuitBreakerError as exc:

                logger.error(
                    "Circuit breaker OPEN | symbol={} timeframe={}",
                    symbol,
                    timeframe,
                )

                raise ChunkFetchError(
                    f"Circuit breaker open for {symbol}/{timeframe}"
                ) from exc

            except Exception as exc:

                last_exc = exc

                wait = BACKOFF_BASE ** attempt

                logger.warning(
                    "Chunk fetch failed | symbol={} timeframe={} attempt={}/{} wait={:.1f}s error={}",
                    symbol,
                    timeframe,
                    attempt,
                    MAX_RETRIES,
                    wait,
                    exc,
                )

                await asyncio.sleep(wait)

        raise ChunkFetchError(
            f"Failed to fetch chunk for {symbol}/{timeframe}"
        ) from last_exc

    # ----------------------------------------------------------
    # Input Validation
    # ----------------------------------------------------------

    @staticmethod
    def _validate_inputs(
        symbol: str,
        timeframe: str,
        limit: int,
    ) -> None:

        if not symbol:
            raise ValueError("symbol cannot be empty")

        if limit <= 0:
            raise ValueError("limit must be > 0")

        _timeframe_to_ms(timeframe)


# ==========================================================
# Helpers
# ==========================================================

def _timeframe_to_ms(timeframe: str) -> int:

    try:
        unit = timeframe[-1].lower()
        value = int(timeframe[:-1])
    except Exception as exc:
        raise InvalidTimeframeError(
            f"Invalid timeframe '{timeframe}'"
        ) from exc

    if unit not in _TIMEFRAME_UNIT_MS:
        raise InvalidTimeframeError(
            f"Invalid timeframe unit '{unit}'"
        )

    return value * _TIMEFRAME_UNIT_MS[unit]


def _raw_to_dataframe(raw: List[list]) -> pd.DataFrame:

    df = pd.DataFrame(raw, columns=list(OHLCV_COLUMNS))

    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        unit="ms",
        utc=True,
    )

    return df