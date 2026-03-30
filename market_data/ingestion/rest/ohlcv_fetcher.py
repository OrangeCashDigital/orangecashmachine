"""
market_data/ingestion/rest/ohlcv_fetcher.py
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
import random
from dataclasses import dataclass
from typing import List, Optional

import pandas as pd
import pybreaker
import ccxt.async_support as _ccxt_async
from loguru import logger

from core.logging.setup import bind_pipeline

from market_data.storage.silver.silver_storage import SilverStorage
from market_data.core.transformers.transformer import OHLCVTransformer
from market_data.adapters.exchange.ccxt_adapter import CCXTAdapter, ExchangeCircuitOpenError
from market_data.observability.metrics import (
    FETCH_CHUNK_DURATION,
    FETCH_CHUNKS_TOTAL,
    FETCH_CHUNK_ERRORS_TOTAL,
)
import time


# ==========================================================
# Constants
# ==========================================================

DEFAULT_CHUNK_LIMIT   = 500
MAX_RETRIES           = 5
BACKOFF_BASE          = 1.6
MAX_BACKOFF_SECONDS   = 30.0
MAX_CHUNKS_PER_RUN    = 100_000
DEFAULT_OVERLAP_BARS  = 3

OHLCV_COLUMNS = ("timestamp", "open", "high", "low", "close", "volume")

# ==========================================================
# Exceptions
from market_data.processing.exceptions import (  # noqa: E402
    FetcherError, MissingStartDateError, ChunkFetchError,
    SymbolNotFoundError, InvalidMarketTypeError,
)
# ==========================================================


from market_data.processing.utils.timeframe import InvalidTimeframeError  # noqa: E402


# ==========================================================
# Result
# ==========================================================

@dataclass(slots=True)
class DownloadResult:
    symbol:     str
    timeframe:  str
    df:         pd.DataFrame
    chunks:     int = 0
    total_rows: int = 0

    @property
    def has_data(self) -> bool:
        return not self.df.empty


# ==========================================================
# CursorStore import (aqui para evitar circular import)
# ==========================================================

from infra.state.cursor_store import CursorStore, InMemoryCursorStore


# ==========================================================
# Fetcher
# ==========================================================

class HistoricalFetcherAsync:

    async def ensure_exchange(self) -> None:
        if not await self._exchange.is_healthy():
            await self._exchange.reconnect()

    def __init__(
        self,
        exchange_client:    CCXTAdapter,
        storage:            Optional[SilverStorage]     = None,
        transformer:        Optional[OHLCVTransformer]  = None,
        overlap_bars:       int                         = DEFAULT_OVERLAP_BARS,
        cursor_store:       Optional[CursorStore]       = None,
        backfill_mode:      bool                        = False,
        market_type:        Optional[str]               = None,
        config_start_date:  Optional[str]               = None,
    ) -> None:
        self._exchange          = exchange_client
        self._storage           = storage or SilverStorage(
            redis_client=getattr(cursor_store, '_client', None),
        )
        self._transformer       = transformer or OHLCVTransformer()
        self._overlap           = overlap_bars
        self._cursor: CursorStore = cursor_store or InMemoryCursorStore()
        self._backfill_mode     = backfill_mode
        self._market_type       = market_type
        self._config_start_date = config_start_date
        self._log = bind_pipeline(
            "fetcher",
            exchange=getattr(exchange_client, "_exchange_id", "unknown"),
        )

        # Circuit breaker eliminado — vive en CCXTAdapter._get_breaker(),
        # compartido por exchange_id. No se duplica aquí.

    # ======================================================
    # Public API
    # ======================================================

    async def download_data(
        self,
        symbol:     str,
        timeframe:  str,
        start_date: Optional[str] = None,
        limit:      int           = DEFAULT_CHUNK_LIMIT,
    ) -> pd.DataFrame:

        self._validate_inputs(symbol, timeframe, limit)
        self._validate_market(symbol, self._market_type)

        result = await self._download_chunked(symbol, timeframe, start_date, limit)

        if not result.has_data:
            self._log.bind(symbol=symbol, timeframe=timeframe).info("No new data")
            return pd.DataFrame(columns=list(OHLCV_COLUMNS))

        self._log.bind(symbol=symbol, timeframe=timeframe, chunks=result.chunks, rows=result.total_rows).info("Download complete")
        return result.df

    async def fetch_chunk(
        self,
        symbol:    str,
        timeframe: str,
        since:     int,
        limit:     int = DEFAULT_CHUNK_LIMIT,
    ) -> List[list]:
        """API pública de fetch de un chunk. Usada por BackfillStrategy."""
        return await self._fetch_chunk_with_retry(symbol, timeframe, since, limit)

    async def close(self) -> None:
        await self._exchange.close()

    # ======================================================
    # Core
    # ======================================================

    async def _download_chunked(
        self,
        symbol:     str,
        timeframe:  str,
        start_date: Optional[str],
        limit:      int,
    ) -> DownloadResult:

        since_ts = await self._resolve_start_timestamp(symbol, timeframe, start_date)
        tf_ms    = timeframe_to_ms(timeframe)

        collected:    List[pd.DataFrame] = []
        last_seen_ts: Optional[int]      = None

        for chunk_idx in range(MAX_CHUNKS_PER_RUN):

            exchange_name = getattr(self._exchange, "_exchange_id", "unknown")
            _t0 = time.perf_counter()
            _status = "success"
            try:
                raw = await self._fetch_chunk_with_retry(symbol, timeframe, since_ts, limit)
            except ExchangeCircuitOpenError:
                _status = "circuit_open"
                self._log.bind(symbol=symbol, timeframe=timeframe, chunk=chunk_idx).warning("Circuit open — aborting chunked download")
                FETCH_CHUNKS_TOTAL.labels(
                    exchange=exchange_name, symbol=symbol, timeframe=timeframe,
                    status=_status,
                ).inc()
                if not collected:
                    # Sin datos — propagar para que el pipeline aborte el exchange.
                    raise
                # Datos parciales — devolver lo que hay.
                break
            except Exception as _chunk_exc:
                _status = "error"
                FETCH_CHUNK_ERRORS_TOTAL.labels(
                    exchange=exchange_name,
                    symbol=symbol,
                    timeframe=timeframe,
                    error_type=type(_chunk_exc).__name__,
                ).inc()
                raise
            finally:
                FETCH_CHUNK_DURATION.labels(
                    exchange=exchange_name, symbol=symbol, timeframe=timeframe,
                ).observe(time.perf_counter() - _t0)
                if _status not in ("circuit_open", "error"):
                    FETCH_CHUNKS_TOTAL.labels(
                        exchange=exchange_name, symbol=symbol, timeframe=timeframe,
                        status=_status,
                    ).inc()

            if not raw:
                FETCH_CHUNKS_TOTAL.labels(
                    exchange=exchange_name, symbol=symbol, timeframe=timeframe,
                    status="empty",
                ).inc()
                break

            df = _raw_to_dataframe(raw)
            df = self._transformer.transform(
                df,
                symbol    = symbol,
                timeframe = timeframe,
                exchange  = getattr(self._exchange, "_exchange_id", "unknown"),
            )
            df = _sanitize_dataframe(df)

            if df.empty:
                break

            collected.append(df)

            last_ts = int(df["timestamp"].max().timestamp() * 1000)

            if last_ts <= since_ts:
                _stale_severity = "regression" if last_ts < since_ts else "stale"
                self._log.bind(symbol=symbol, timeframe=timeframe, severity=_stale_severity, last_ts=last_ts, since_ts=since_ts).warning("Stale window — aborting")
                FETCH_CHUNKS_TOTAL.labels(
                    exchange=exchange_name, symbol=symbol, timeframe=timeframe,
                    status=_stale_severity,
                ).inc()
                break

            last_seen_ts = last_ts
            since_ts     = last_ts - (self._overlap * tf_ms)

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
            symbol     = symbol,
            timeframe  = timeframe,
            df         = combined,
            chunks     = len(collected),
            total_rows = len(combined),
        )

    # ======================================================
    # Timestamp logic
    # ======================================================

    async def _resolve_start_timestamp(
        self,
        symbol:     str,
        timeframe:  str,
        start_date: Optional[str],
    ) -> int:
        """
        Jerarquía de resolución de timestamp de inicio.

        backfill_mode=True:
            1. config_start_date como floor (sin discovery dinámico)

        backfill_mode=False (incremental):
            1. Cursor Redis/async  → reanuda desde último punto guardado
            2. Parquet last_ts     → fallback si cursor vacío
            3. start_date del arg  → primer inicio manual
            4. _config_start_date  → primer inicio desde config YAML
            5. MissingStartDateError → falla explícita
        """
        exchange_name = getattr(self._exchange, "_exchange_id", "unknown")
        tf_ms = timeframe_to_ms(timeframe)

        if self._backfill_mode:
            candidate = start_date or self._config_start_date
            if not candidate:
                raise MissingStartDateError(
                    f"backfill_mode=True requiere config_start_date | {symbol}/{timeframe}"
                )
            self._log.bind(symbol=symbol, timeframe=timeframe, desde=candidate).info("Backfill mode")
            return self._exchange.parse8601(candidate)

        # A. Cursor async — await obligatorio
        cursor_ts = await self._cursor.get(exchange_name, symbol, timeframe)
        if cursor_ts is not None:
            self._log.bind(symbol=symbol, timeframe=timeframe, ts_ms=cursor_ts).debug("Cursor hit")
            return cursor_ts - (self._overlap * tf_ms)

        # B. Fallback parquet
        last_ts = self._storage.get_last_timestamp(symbol, timeframe)
        if last_ts is not None:
            self._log.bind(symbol=symbol, timeframe=timeframe).debug("Cursor miss — fallback parquet")
            return int(last_ts.timestamp() * 1000) - (self._overlap * tf_ms)

        # C. Primer inicio desde arg o config
        for candidate in [start_date, self._config_start_date]:
            if candidate:
                self._log.bind(symbol=symbol, timeframe=timeframe, desde=candidate).info("Primer inicio")
                return self._exchange.parse8601(candidate)

        raise MissingStartDateError(
            f"{symbol}/{timeframe} en modo incremental sin cursor, "
            f"sin parquet y sin start_date configurado."
        )

    # ======================================================
    # Fetch with retry + breaker
    # ======================================================

    # _SESSION_ERRORS y _TRANSIENT_ERRORS eliminados — reemplazados por
    # clasificación tipada via jerarquía ccxt en _fetch_chunk_with_retry.

    async def _fetch_chunk_with_retry(
        self,
        symbol:    str,
        timeframe: str,
        since:     int,
        limit:     int,
    ) -> List[list]:
        """
        Retry con clasificación tipada de errores ccxt.

        Jerarquía ccxt 4.x relevante:
          BaseError
          ├── NetworkError          — transitorios: timeout, conn reset, etc.
          │   ├── RequestTimeout    — timeout de red
          │   └── RateLimitExceeded — 429 (pesa doble en AdaptiveThrottle)
          └── ExchangeError
              └── AuthenticationError — fatal, no reintentar jamás
        """
        last_exc:           Optional[Exception] = None
        session_reconnected: bool               = False

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                return await self._exchange.fetch_ohlcv(
                    symbol, timeframe, since, limit,
                )
            except ExchangeCircuitOpenError:
                # Circuit abierto — reintentar es inútil. Re-raise inmediato.
                raise
            except _ccxt_async.AuthenticationError as exc:
                # Fatal — credenciales inválidas o IP ban. No reintentar jamás.
                self._log.bind(symbol=symbol, timeframe=timeframe, attempt=attempt, err=str(exc)).error("Auth error — aborting")
                raise ChunkFetchError(f"{symbol}/{timeframe} auth failed") from exc
            except _ccxt_async.RateLimitExceeded as exc:
                last_exc = exc
                wait     = min(BACKOFF_BASE ** attempt, MAX_BACKOFF_SECONDS)
                wait    *= random.uniform(0.5, 1.5)
                try:
                    breaker = getattr(self._exchange, "_breaker", None)
                    if breaker is not None:
                        breaker.call(lambda: (_ for _ in ()).throw(exc))
                except Exception:
                    pass
                self._log.bind(symbol=symbol, timeframe=timeframe, attempt=attempt, wait_s=round(wait,2)).warning("Rate limit (429) — notified breaker")
                await asyncio.sleep(wait)
            except (_ccxt_async.RequestTimeout, asyncio.TimeoutError) as exc:
                # Timeout de red — NO cuenta como fallo de breaker.
                last_exc = exc
                wait     = min(BACKOFF_BASE ** attempt, MAX_BACKOFF_SECONDS)
                wait    *= random.uniform(0.5, 1.5)
                self._log.bind(symbol=symbol, timeframe=timeframe, attempt=attempt, wait_s=round(wait,2), err=str(exc)).warning("Timeout — retrying")
                await asyncio.sleep(wait)
            except _ccxt_async.NetworkError as exc:
                # Errores de red transitorios — conn reset, DNS, session muerta.
                last_exc = exc
                wait     = min(BACKOFF_BASE ** attempt, MAX_BACKOFF_SECONDS)
                wait    *= random.uniform(0.5, 1.5)
                err_str  = str(exc)
                is_session_dead = any(
                    m in err_str for m in ("Session is closed", "Connection closed")
                )
                if is_session_dead and not session_reconnected:
                    self._log.bind(symbol=symbol, timeframe=timeframe, attempt=attempt, err=str(exc)).warning("Session dead — reconnecting")
                    await self._exchange.reconnect()
                    session_reconnected = True
                    continue
                self._log.bind(symbol=symbol, timeframe=timeframe, attempt=attempt, wait_s=round(wait,2), err=str(exc)).warning("Network error — retrying")
                await asyncio.sleep(wait)
            except Exception as exc:
                # Fallback: error desconocido — loguear tipo completo.
                last_exc = exc
                wait     = min(BACKOFF_BASE ** attempt, MAX_BACKOFF_SECONDS)
                wait    *= random.uniform(0.5, 1.5)
                self._log.bind(symbol=symbol, timeframe=timeframe, attempt=attempt, wait_s=round(wait,2), error_type=type(exc).__name__, err=str(exc)).warning("Fetch failed (unknown)")
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
        timeframe_to_ms(timeframe)

    def _validate_market(self, symbol: str, market_type: Optional[str] = None) -> None:
        market = self._exchange.get_market(symbol)
        if not market:
            return
        exchange_id = getattr(self._exchange, "_exchange_id", "unknown")
        if market_type == "swap" and not market.get("swap", False):
            raise InvalidMarketTypeError(
                f"Symbol '{symbol}' is not swap/futures in {exchange_id} "
                f"(type={market.get('type', '?')}) — fix config"
            )
        if market_type == "spot" and not market.get("spot", False):
            raise InvalidMarketTypeError(
                f"Symbol '{symbol}' is not spot in {exchange_id} "
                f"(type={market.get('type', '?')}) — fix config"
            )


# ==========================================================
# Helpers
# ==========================================================

from market_data.processing.utils.timeframe import timeframe_to_ms  # noqa: E402


def _raw_to_dataframe(raw: List[list]) -> pd.DataFrame:
    df = pd.DataFrame(raw, columns=list(OHLCV_COLUMNS))
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    return df


def _sanitize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["timestamp"])
    return df.sort_values("timestamp")


from market_data.processing.utils.timeframe import timeframe_to_ms  # noqa: E402,F811
