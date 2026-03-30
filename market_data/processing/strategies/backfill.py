"""
market_data/processing/strategies/backfill.py
=========================================

Strategy de backfill histórico completo hacia atrás.
"""

from __future__ import annotations

import asyncio
import time
from typing import Optional

import pandas as pd
from core.logging.setup import bind_pipeline
from market_data.ingestion.rest.ohlcv_fetcher import DEFAULT_CHUNK_LIMIT
from market_data.processing.utils.timeframe import timeframe_to_ms
from market_data.processing.strategies.base import (
    PairResult,
    PipelineContext,
    PipelineMode,
    StrategyMixin,
)
from market_data.observability.metrics import ROWS_INGESTED, PIPELINE_ERRORS

_log = bind_pipeline("backfill")

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
        log        = _log.bind(exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe, mode="backfill")

        try:
            log.info("Backfill iniciando", idx=idx, total=total)

            origin_ms = await self._discover_origin(symbol, timeframe, ctx)
            if origin_ms is None:
                log.warning("Backfill skip — no se pudo determinar origen")
                result.skipped     = True
                result.duration_ms = int((time.monotonic() - pair_start) * 1000)
                return result

            log.info("Backfill origin", origin=pd.Timestamp(origin_ms, unit="ms", tz="UTC").isoformat())

            start_ms = await self._resolve_backfill_start(symbol, timeframe, ctx, origin_ms)


            if start_ms <= origin_ms:
                log.info("Backfill completo — ya se alcanzó el origen",
                    oldest=pd.Timestamp(start_ms,  unit="ms", tz="UTC").isoformat(),
                    origin=pd.Timestamp(origin_ms, unit="ms", tz="UTC").isoformat(),
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

            log.success("Backfill completado",
                idx=idx, total=total, chunks=chunks,
                rows=total_rows, duration_ms=result.duration_ms,
            )

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            result.error       = str(exc)
            result.duration_ms = int((time.monotonic() - pair_start) * 1000)
            error_type = "transient" if result.is_transient_error else "fatal"
            PIPELINE_ERRORS.labels(exchange=ctx.exchange_id, error_type=error_type).inc()
            log.error("Backfill fallido",
                idx=idx, total=total,
                error=str(exc), duration_ms=result.duration_ms,
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
                _log.bind(exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe).debug(
                    "Origin cache hit", origin=pd.Timestamp(ts_ms, unit="ms", tz="UTC").isoformat(),
                )
                return ts_ms
        except Exception:
            pass

        try:
            raw_data = await ctx.fetcher.fetch_chunk(
                symbol=symbol, timeframe=timeframe, since=0, limit=1,
            )
            if not raw_data:
                return None

            origin_ms = int(raw_data[0][0])

            try:
                ctx.cursor.set_raw(cache_key, str(origin_ms), _BACKFILL_TTL_SECONDS)
                _log.bind(exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe).debug(
                    "Origin cached", origin=pd.Timestamp(origin_ms, unit="ms", tz="UTC").isoformat(),
                )
            except Exception:
                pass

            return origin_ms

        except Exception as exc:
            _log.bind(exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe).warning(
                "Origin discovery failed", error=str(exc),
            )
            return None

    # ----------------------------------------------------------
    # Backfill Cursor
    # ----------------------------------------------------------

    async def _resolve_backfill_start(
        self,
        symbol:    str,
        timeframe: str,
        ctx:       PipelineContext,
        origin_ms: int,
    ) -> int:
        # A. Cursor Redis
        try:
            bk_key = self._backfill_key(ctx, symbol, timeframe)
            raw = ctx.cursor.get_raw(bk_key)
            if raw:
                ts_ms = int(raw)
                _log.bind(exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe).debug(
                    "Backfill cursor hit", ts=pd.Timestamp(ts_ms, unit="ms", tz="UTC").isoformat(),
                )
                return ts_ms
        except Exception as exc:
            _log.bind(exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe).debug(
                "Backfill cursor read failed (non-critical)", error=str(exc),
            )

        # B. Oldest timestamp en Silver (I/O en threadpool para no bloquear el loop)
        oldest = await asyncio.to_thread(self._get_oldest_silver_ts, ctx, symbol, timeframe)
        if oldest is not None:
            return int(oldest.timestamp() * 1000)

        # C. Cold start — sin cursor ni Silver: usar origin_ms como inicio.
        #    start_ms > origin_ms garantizado por +1, evita el skip de "ya completo".
        _log.bind(exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe).info(
            "Backfill cold start — sin datos previos, paginando desde origin"
        )
        return origin_ms + 1

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
            _log.bind(exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe).debug(
                "Backfill cursor write failed (non-critical)", error=str(exc),
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
            _log.bind(symbol=symbol, timeframe=timeframe).warning(
                "Oldest silver ts failed", error=str(exc),
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

        tf_ms       = timeframe_to_ms(timeframe)
        chunk_limit = DEFAULT_CHUNK_LIMIT
        current_end = since_ms
        total_rows  = 0
        chunks      = 0
        last_end    = None
        log         = _log.bind(exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe, mode="backfill")

        for _ in range(_MAX_BACKFILL_CHUNKS):

            chunk_start = max(current_end - (chunk_limit * tf_ms), origin_ms)

            log.debug("Backfill chunk",
                chunk=chunks + 1,
                range_start=pd.Timestamp(chunk_start, unit="ms", tz="UTC").strftime("%Y-%m-%d %H:%M"),
                range_end=pd.Timestamp(current_end,   unit="ms", tz="UTC").strftime("%Y-%m-%d %H:%M"),
            )

            try:
                raw = await ctx.fetcher.fetch_chunk(
                    symbol=symbol, timeframe=timeframe,
                    since=chunk_start, limit=chunk_limit,
                )
            except Exception as exc:
                log.warning("Backfill chunk fetch failed", error=str(exc))
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
                log.warning("Backfill anti-loop detectado — abortando paginación",
                    oldest_in_chunk=oldest_in_chunk, last_end=last_end,
                )
                break

            qres = ctx.quality.run(
                df=df, symbol=symbol, timeframe=timeframe, exchange=ctx.exchange_id,
            )

            if qres.accepted:
                ctx.storage.save_ohlcv(df=qres.df, symbol=symbol, timeframe=timeframe)
                total_rows += len(qres.df)
            else:
                log.warning("Backfill chunk rechazado por calidad", score=round(qres.score, 1))

            chunks  += 1
            last_end = oldest_in_chunk

            self._update_backfill_cursor(symbol, timeframe, oldest_in_chunk, ctx)

            current_end = oldest_in_chunk

            log.debug("Backfill progress",
                chunk=chunks, rows_chunk=len(df), total_rows=total_rows,
                oldest=pd.Timestamp(current_end, unit="ms", tz="UTC").strftime("%Y-%m-%d"),
            )

            if current_end <= origin_ms:
                log.info("Backfill alcanzó el origen")
                break

        return total_rows, chunks

    # ----------------------------------------------------------
    # Key helpers
    # ----------------------------------------------------------

    def _origin_key(self, ctx: PipelineContext, symbol: str, timeframe: str) -> str:
        from infra.state.cursor_store import _encode
        env = getattr(ctx.cursor, "_env", "development")
        return f"{env}:{_ORIGIN_KEY_PREFIX}:{_encode(ctx.exchange_id)}:{_encode(symbol)}:{_encode(timeframe)}"

    def _backfill_key(self, ctx: PipelineContext, symbol: str, timeframe: str) -> str:
        from infra.state.cursor_store import _encode
        env = getattr(ctx.cursor, "_env", "development")
        return f"{env}:{_BACKFILL_KEY_PREFIX}:{_encode(ctx.exchange_id)}:{_encode(symbol)}:{_encode(timeframe)}"
