"""
fetcher.py
==========

Fetcher profesional OHLCV con:

• Descarga incremental con overlap configurable
• Idempotencia (compatible con storage)
• Retry + backoff exponencial
• Circuit breaker correcto
• Protección contra loops y gaps
• Validación robusta de datos

SafeOps Ready 🚀
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


# ==========================================================
# Constants
# ==========================================================

DEFAULT_CHUNK_LIMIT = 500
MAX_RETRIES = 5
BACKOFF_BASE = 1.6
MAX_CHUNKS_PER_RUN = 100_000

# 🔥 NUEVO
DEFAULT_OVERLAP_BARS = 2  # CRÍTICO para consistencia

OHLCV_COLUMNS = ("timestamp", "open", "high", "low", "close", "volume")

_TIMEFRAME_UNIT_MS = {
    "m": 60_000,
    "h": 3_600_000,
    "d": 86_400_000,
    "w": 604_800_000,
}


# ==========================================================
# Exceptions
# ==========================================================

class FetcherError(Exception): ...
class InvalidTimeframeError(FetcherError): ...
class MissingStartDateError(FetcherError): ...
class ChunkFetchError(FetcherError): ...


# ==========================================================
# Result
# ==========================================================

@dataclass(slots=True)
class DownloadResult:
    symbol: str
    timeframe: str
    df: pd.DataFrame
    chunks: int = 0
    total_rows: int = 0

    @property
    def has_data(self) -> bool:
        return not self.df.empty


# ==========================================================
# Fetcher
# ==========================================================

class HistoricalFetcherAsync:

    def __init__(
        self,
        exchange_client: CCXTAdapter,
        storage: Optional[HistoricalStorage] = None,
        transformer: Optional[OHLCVTransformer] = None,
        overlap_bars: int = DEFAULT_OVERLAP_BARS,
    ) -> None:

        self._exchange = exchange_client
        self._storage = storage or HistoricalStorage()
        self._transformer = transformer or OHLCVTransformer()
        self._overlap = overlap_bars

        self._breaker = pybreaker.CircuitBreaker(
            fail_max=5,
            reset_timeout=60,
        )

    # ======================================================
    # Public API
    # ======================================================

    async def download_data(
        self,
        symbol: str,
        timeframe: str,
        start_date: Optional[str] = None,
        limit: int = DEFAULT_CHUNK_LIMIT,
    ) -> pd.DataFrame:

        self._validate_inputs(symbol, timeframe, limit)

        result = await self._download_chunked(
            symbol, timeframe, start_date, limit
        )

        if not result.has_data:
            logger.info("No new data | {} {}", symbol, timeframe)
            return pd.DataFrame(columns=list(OHLCV_COLUMNS))

        logger.info(
            "Download complete | {} {} chunks={} rows={}",
            symbol, timeframe, result.chunks, result.total_rows
        )

        return result.df

    async def close(self) -> None:
        await self._exchange.close()

    # ======================================================
    # Core
    # ======================================================

    async def _download_chunked(
        self,
        symbol: str,
        timeframe: str,
        start_date: Optional[str],
        limit: int,
    ) -> DownloadResult:

        since_ts = await self._resolve_start_timestamp(
            symbol, timeframe, start_date
        )

        tf_ms = _timeframe_to_ms(timeframe)

        collected: List[pd.DataFrame] = []
        last_seen_ts: Optional[int] = None

        for chunk_idx in range(MAX_CHUNKS_PER_RUN):

            raw = await self._fetch_chunk_with_retry(
                symbol, timeframe, since_ts, limit
            )

            if not raw:
                break

            df = _raw_to_dataframe(raw)
            df = self._transformer.transform(df)
            df = _sanitize_dataframe(df)

            if df.empty:
                break

            collected.append(df)

            last_ts = int(df["timestamp"].max().timestamp() * 1000)

            # 🔥 Protección anti-loop
            if last_seen_ts == last_ts:
                logger.warning("Loop detected | {} {}", symbol, timeframe)
                break

            last_seen_ts = last_ts

            # 🔥 AVANCE CON OVERLAP
            since_ts = last_ts - (self._overlap * tf_ms)

            if len(raw) < limit:
                break

        if not collected:
            return DownloadResult(symbol, timeframe, pd.DataFrame())

        combined = (
            pd.concat(collected, ignore_index=True)
            .sort_values("timestamp")
            .drop_duplicates(subset="timestamp", keep="last")
            .reset_index(drop=True)
        )

        return DownloadResult(
            symbol=symbol,
            timeframe=timeframe,
            df=combined,
            chunks=len(collected),
            total_rows=len(combined),
        )

    # ======================================================
    # Timestamp logic
    # ======================================================

    async def _resolve_start_timestamp(
        self,
        symbol: str,
        timeframe: str,
        start_date: Optional[str],
    ) -> int:

        last_ts = self._storage.get_last_timestamp(symbol, timeframe)

        tf_ms = _timeframe_to_ms(timeframe)

        if last_ts is not None:
            return int(last_ts.timestamp() * 1000) - (self._overlap * tf_ms)

        if not start_date:
            raise MissingStartDateError(f"{symbol}/{timeframe} needs start_date")

        return self._exchange.parse8601(start_date)

    # ======================================================
    # Fetch with retry + breaker
    # ======================================================

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
                return await self._breaker.call_async(
                    self._exchange.fetch_ohlcv,
                    symbol,
                    timeframe,
                    since,
                    limit,
                )

            except Exception as exc:
                last_exc = exc

                wait = BACKOFF_BASE ** attempt

                logger.warning(
                    "Fetch failed | {} {} attempt={} wait={:.2f}s err={}",
                    symbol, timeframe, attempt, wait, exc
                )

                await asyncio.sleep(wait)

        raise ChunkFetchError(f"{symbol}/{timeframe} failed") from last_exc

    # ======================================================
    # Validation
    # ======================================================

    @staticmethod
    def _validate_inputs(symbol: str, timeframe: str, limit: int) -> None:
        if not symbol:
            raise ValueError("symbol required")
        if limit <= 0:
            raise ValueError("limit > 0 required")
        _timeframe_to_ms(timeframe)


# ==========================================================
# Helpers
# ==========================================================

def _timeframe_to_ms(timeframe: str) -> int:
    try:
        return int(timeframe[:-1]) * _TIMEFRAME_UNIT_MS[timeframe[-1]]
    except Exception as exc:
        raise InvalidTimeframeError(timeframe) from exc


def _raw_to_dataframe(raw: List[list]) -> pd.DataFrame:
    df = pd.DataFrame(raw, columns=list(OHLCV_COLUMNS))
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    return df


def _sanitize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpieza mínima crítica (SafeOps)
    """
    df = df.dropna(subset=["timestamp"])
    return df.sort_values("timestamp")