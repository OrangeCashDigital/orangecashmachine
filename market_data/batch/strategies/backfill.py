"""
market_data/batch/strategies/backfill.py
=========================================

Strategy de backfill histórico completo hacia atrás.
"""

from __future__ import annotations

import asyncio
import time
from typing import Optional

import pandas as pd
from loguru import logger

from market_data.batch.fetchers.fetcher import DEFAULT_CHUNK_LIMIT
from market_data.batch.schemas.timeframe import timeframe_to_ms as _timeframe_to_ms
from market_data.batch.strategies.base import (
    PairResult,
    PipelineContext,
    PipelineMode,
    StrategyMixin,
)
from services.observability.metrics import ROWS_INGESTED, PIPELINE_ERRORS

_BACKFILL_TTL_SECONDS: int = 30 * 86_400
_ORIGIN_KEY_PREFIX:    str = "origin"
_BACKFILL_KEY_PREFIX:  str = "backfill"
_MAX_BACKFILL_CHUNKS:  int = 100_000


class BackfillStrategy(StrategyMixin):
    _mode = PipelineMode.BACKFILL

    async def execute_pair(
        self,
        symbol:    str,
        timeframe: str,
        idx:       int,
        total:     int,
        ctx:       PipelineContext,
    ) -> PairResult:

        result     = PairResult(symbol=symbol, timeframe=timeframe, mode=PipelineMode.BACKFILL)
        pair_start = time.monotonic()

        try:
            logger.info(
                "Backfill iniciando [{}/{}] | exchange={} symbol={} timeframe={}",
                idx, total, ctx.exchange_id, symbol, timeframe,
            )

            origin_ms = await self._discover_origin(symbol, timeframe, ctx)
            if origin_ms is None:
                logger.warning(
                    "Backfill skip — no se pudo determinar origen | symbol={} timeframe={}",
                    symbol, timeframe,
                )
                result.skipped     = True
                result.duration_ms = int((time.monotonic() - pair_start) * 1000)
                return result

            logger.info(
                "Backfill origin | exchange={} symbol={} timeframe={} origin={}",
                ctx.exchange_id, symbol, timeframe,
                pd.Timestamp(origin_ms, unit="ms", tz="UTC").isoformat(),
            )

            start_ms = self._resolve_backfill_start(symbol, timeframe, ctx)

            if start_ms is None:
                logger.info(
                    "Backfill skip — sin datos en Silver | symbol={} timeframe={}",
                    symbol, timeframe,
                )
                result.skipped     = True
                result.duration_ms = int((time.monotonic() - pair_start) * 1000)
                return result

            if start_ms <= origin_ms:
                logger.info(
                    "Backfill completo — ya se alcanzó el origen | "
                    "symbol={} timeframe={} oldest={} origin={}",
                    symbol, timeframe,
                    pd.Timestamp(start_ms,  unit="ms", tz="UTC").isoformat(),
                    pd.Timestamp(origin_ms, unit="ms", tz="UTC").isoformat(),
                )
                result.skipped     = True
                result.duration_ms = int((time.monotonic() - pair_start) * 1000)
                return result

            total_rows, chunks = await self._paginate_backward(
                symbol    = symbol,
                timeframe = timeframe,
                since_ms  = start_ms,
                origin_ms = origin_ms,
                ctx       = ctx,
            )

            result.rows        = total_rows
            result.chunks      = chunks
            result.duration_ms = int((time.monotonic() - pair_start) * 1000)

            if total_rows > 0:
                ROWS_INGESTED.labels(
                    exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe,
                ).inc(total_rows)

            logger.success(
                "Backfill completado [{}/{}] | exchange={} symbol={} timeframe={} "
                "chunks={} rows={} duration={}ms",
                idx, total, ctx.exchange_id, symbol, timeframe,
                chunks, total_rows, result.duration_ms,
            )

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            result.error       = str(exc)
            result.duration_ms = int((time.monotonic() - pair_start) * 1000)
            error_type = "transient" if result.is_transient_error else "fatal"
            PIPELINE_ERRORS.labels(exchange=ctx.exchange_id, error_type=error_type).inc()
            logger.error(
                "Backfill fallido [{}/{}] | exchange={} symbol={} timeframe={} "
                "error={} duration={}ms",
                idx, total, ctx.exchange_id, symbol, timeframe,
                exc, result.duration_ms,
            )

        return result

    # ----------------------------------------------------------
    # Origin Discovery
    # ----------------------------------------------------------

    async def _discover_origin(
        self,
        symbol:    str,
        timeframe: str,
        ctx:       PipelineContext,
    ) -> Optional[int]:
        cache_key = self._origin_key(ctx, symbol, timeframe)

        try:
            raw = ctx.cursor.get_raw(cache_key)
            if raw:
                ts_ms = int(raw)
                logger.debug(
                    "Origin cache hit | exchange={} symbol={} timeframe={} origin={}",
                    ctx.exchange_id, symbol, timeframe,
                    pd.Timestamp(ts_ms, unit="ms", tz="UTC").isoformat(),
                )
                return ts_ms
        except Exception:
            pass

        try:
            raw_data = await ctx.fetcher._fetch_chunk_with_retry(
                symbol=symbol, timeframe=timeframe, since=0, limit=1,
            )
            if not raw_data:
                return None

            origin_ms = int(raw_data[0][0])

            try:
                ctx.cursor.set_raw(cache_key, str(origin_ms), _BACKFILL_TTL_SECONDS)
                logger.debug(
                    "Origin cached | exchange={} symbol={} timeframe={} origin={}",
                    ctx.exchange_id, symbol, timeframe,
                    pd.Timestamp(origin_ms, unit="ms", tz="UTC").isoformat(),
                )
            except Exception:
                pass

            return origin_ms

        except Exception as exc:
            logger.warning(
                "Origin discovery failed | exchange={} symbol={} timeframe={} error={}",
                ctx.exchange_id, symbol, timeframe, exc,
            )
            return None

    # ----------------------------------------------------------
    # Backfill Cursor
    # ----------------------------------------------------------

    def _resolve_backfill_start(
        self,
        symbol:    str,
        timeframe: str,
        ctx:       PipelineContext,
    ) -> Optional[int]:
        try:
            bk_key = self._backfill_key(ctx, symbol, timeframe)
            raw = ctx.cursor.get_raw(bk_key)
            if raw:
                ts_ms = int(raw)
                logger.debug(
                    "Backfill cursor hit | exchange={} symbol={} timeframe={} ts={}",
                    ctx.exchange_id, symbol, timeframe,
                    pd.Timestamp(ts_ms, unit="ms", tz="UTC").isoformat(),
                )
                return ts_ms
        except Exception as exc:
            logger.debug(
                "Backfill cursor read failed (non-critical) | exchange={} symbol={} timeframe={} error={}",
                ctx.exchange_id, symbol, timeframe, exc,
            )

        oldest = self._get_oldest_silver_ts(ctx, symbol, timeframe)
        if oldest is not None:
            return int(oldest.timestamp() * 1000)

        return None

    def _update_backfill_cursor(
        self,
        symbol:    str,
        timeframe: str,
        ts_ms:     int,
        ctx:       PipelineContext,
    ) -> None:
        try:
            bk_key = self._backfill_key(ctx, symbol, timeframe)
            raw    = ctx.cursor.get_raw(bk_key)
            if raw is None or ts_ms < int(raw):
                ctx.cursor.set_raw(bk_key, str(ts_ms), _BACKFILL_TTL_SECONDS)
        except Exception as exc:
            logger.debug(
                "Backfill cursor write failed (non-critical) | exchange={} symbol={} timeframe={} error={}",
                ctx.exchange_id, symbol, timeframe, exc,
            )

    def _get_oldest_silver_ts(
        self,
        ctx:       PipelineContext,
        symbol:    str,
        timeframe: str,
    ) -> Optional[pd.Timestamp]:
        try:
            files = ctx.storage.find_partition_files(symbol, timeframe)
            if not files:
                return None
            timestamps = []
            for f in files[:3]:
                try:
                    df = pd.read_parquet(f, columns=["timestamp"])
                    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
                    timestamps.append(df["timestamp"].min())
                except Exception:
                    continue
            return min(timestamps) if timestamps else None
        except Exception as exc:
            logger.warning(
                "Oldest silver ts failed | symbol={} timeframe={} error={}",
                symbol, timeframe, exc,
            )
            return None

    # ----------------------------------------------------------
    # Backward Pagination
    # ----------------------------------------------------------

    async def _paginate_backward(
        self,
        symbol:    str,
        timeframe: str,
        since_ms:  int,
        origin_ms: int,
        ctx:       PipelineContext,
    ) -> tuple[int, int]:

        tf_ms       = _timeframe_to_ms(timeframe)
        chunk_limit = DEFAULT_CHUNK_LIMIT
        current_end = since_ms
        total_rows  = 0
        chunks      = 0
        last_end    = None

        for _ in range(_MAX_BACKFILL_CHUNKS):

            chunk_start = max(current_end - (chunk_limit * tf_ms), origin_ms)

            logger.debug(
                "Backfill chunk {} | symbol={} timeframe={} range=[{} → {}]",
                chunks + 1, symbol, timeframe,
                pd.Timestamp(chunk_start, unit="ms", tz="UTC").strftime("%Y-%m-%d %H:%M"),
                pd.Timestamp(current_end, unit="ms", tz="UTC").strftime("%Y-%m-%d %H:%M"),
            )

            try:
                raw = await ctx.fetcher._fetch_chunk_with_retry(
                    symbol=symbol, timeframe=timeframe,
                    since=chunk_start, limit=chunk_limit,
                )
            except Exception as exc:
                logger.warning(
                    "Backfill chunk fetch failed | symbol={} timeframe={} error={}",
                    symbol, timeframe, exc,
                )
                break

            if not raw:
                break

            df = pd.DataFrame(raw, columns=["timestamp","open","high","low","close","volume"])
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
            ts_ns = df["timestamp"].astype("int64") // 1_000_000
            df = df[ts_ns < current_end].sort_values("timestamp").reset_index(drop=True)

            if df.empty:
                break

            oldest_in_chunk = int(df["timestamp"].min().timestamp() * 1000)

            # Anti-loop robusto: detecta si el exchange devuelve datos
            # que no retroceden (oldest_in_chunk >= last_end) O si el chunk
            # actual es idéntico al anterior (exchange repite misma ventana).
            if last_end is not None and oldest_in_chunk >= last_end:
                logger.warning(
                    "Backfill anti-loop detectado | symbol={} timeframe={} "
                    "oldest_in_chunk={} last_end={} — abortando paginación",
                    symbol, timeframe, oldest_in_chunk, last_end,
                )
                break

            qres = ctx.quality.run(
                df=df, symbol=symbol, timeframe=timeframe, exchange=ctx.exchange_id,
            )

            if qres.accepted:
                ctx.storage.save_ohlcv(df=qres.df, symbol=symbol, timeframe=timeframe)
                total_rows += len(qres.df)
            else:
                logger.warning(
                    "Backfill chunk rechazado por calidad | symbol={} timeframe={} score={:.1f}",
                    symbol, timeframe, qres.score,
                )

            chunks  += 1
            last_end = oldest_in_chunk

            self._update_backfill_cursor(symbol, timeframe, oldest_in_chunk, ctx)

            current_end = oldest_in_chunk

            logger.debug(
                "Backfill progress | symbol={} timeframe={} chunk={} "
                "rows_chunk={} total_rows={} oldest={}",
                symbol, timeframe, chunks, len(df), total_rows,
                pd.Timestamp(current_end, unit="ms", tz="UTC").strftime("%Y-%m-%d"),
            )

            if current_end <= origin_ms:
                logger.info(
                    "Backfill alcanzó el origen | symbol={} timeframe={}",
                    symbol, timeframe,
                )
                break

        return total_rows, chunks

    # ----------------------------------------------------------
    # Key helpers
    # ----------------------------------------------------------

    def _origin_key(self, ctx: PipelineContext, symbol: str, timeframe: str) -> str:
        from services.state.cursor_store import _encode
        env = getattr(ctx.cursor, "_env", "development")
        return f"{env}:{_ORIGIN_KEY_PREFIX}:{_encode(ctx.exchange_id)}:{_encode(symbol)}:{_encode(timeframe)}"

    def _backfill_key(self, ctx: PipelineContext, symbol: str, timeframe: str) -> str:
        from services.state.cursor_store import _encode
        env = getattr(ctx.cursor, "_env", "development")
        return f"{env}:{_BACKFILL_KEY_PREFIX}:{_encode(ctx.exchange_id)}:{_encode(symbol)}:{_encode(timeframe)}"
