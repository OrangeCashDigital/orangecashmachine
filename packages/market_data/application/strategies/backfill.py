# -*- coding: utf-8 -*-
"""
market_data/application/strategies/backfill.py
================================================

Strategy de backfill histórico completo hacia atrás.

Orquesta: fetcher (REST) · cursor (Redis) · storage (Iceberg) ·
          publisher (OHLCVPublisherPort) · quality gate · exchange_quirks.

Capas permitidas para importar
-------------------------------
  domain/          → value objects, tipos, contratos
  ports/outbound/  → puertos abstractos (MetricsPort, OHLCVPublisherPort)
  adapters/        → mappers ACL (pandas_to_domain)
  ocm/             → plataforma OCM (bind_pipeline, encoding)

Lo que NO importa (DIP)
-----------------------
  infrastructure/  → inyectados por composition root

Principios: SRP · DIP · SafeOps · KISS
"""
from __future__ import annotations

import time
from typing import Optional

import pandas as pd
from ocm.observability import bind_pipeline

# ── Domain ───────────────────────────────────────────────────────────────────
from market_data.domain.constants import DEFAULT_CHUNK_LIMIT, MAX_BACKFILL_CHUNKS
from market_data.domain.policies.base import (
    PairResult,
    PipelineContext,
    PipelineMode,
    StrategyMixin,
)
from market_data.domain.value_objects.timeframe import timeframe_to_ms
from market_data.domain.value_objects.exchange_quirks import (
    get_origin_fallback_ms,
    get_quirks,
)

# ── Ports ─────────────────────────────────────────────────────────────────────
from market_data.ports.outbound.publisher import SOURCE_BACKFILL

# ── Ports ─────────────────────────────────────────────────────────────────────
from market_data.ports.outbound.chunk_converter import OHLCVChunkConverterPort
from ocm.runtime.state import encode_redis_key as _encode

_log = bind_pipeline("backfill")

_BACKFILL_TTL_SECONDS: int = 30 * 86_400
_ORIGIN_KEY_PREFIX:    str = "origin"
_BACKFILL_KEY_PREFIX:  str = "backfill"


class BackfillStrategy(StrategyMixin):
    _mode = PipelineMode.BACKFILL

    async def _run(
        self,
        symbol:    str,
        timeframe: str,
        idx:       int,
        total:     int,
        ctx:       PipelineContext,
        result:    PairResult,
    ) -> None:

        log = _log.bind(
            exchange=ctx.exchange_id, symbol=symbol,
            timeframe=timeframe, mode="backfill",
        )
        log.info("Backfill iniciando", idx=idx, total=total)

        origin_ms = await self._discover_origin(symbol, timeframe, ctx)
        if origin_ms is None:
            log.warning("Backfill skip — no se pudo determinar origen")
            result.skipped = True
            return

        log.info(
            "Backfill origin",
            origin=pd.Timestamp(origin_ms, unit="ms", tz="UTC").isoformat(),
        )

        start_ms = await self._resolve_backfill_start(symbol, timeframe, ctx, origin_ms)

        if start_ms < origin_ms:
            log.info(
                "Backfill completo — ya se alcanzó el origen o start_date",
                oldest=pd.Timestamp(start_ms,  unit="ms", tz="UTC").isoformat(),
                origin=pd.Timestamp(origin_ms, unit="ms", tz="UTC").isoformat(),
                start_date=ctx.start_date,
            )
            result.skipped = True
            return

        total_rows, chunks = await self._paginate_backward(
            symbol    = symbol,
            timeframe = timeframe,
            since_ms  = start_ms,
            origin_ms = origin_ms,
            ctx       = ctx,
        )

        result.rows   = total_rows
        result.chunks = chunks

        if total_rows > 0:
            ctx.metrics.rows_ingested_inc(
                exchange=ctx.exchange_id, timeframe=timeframe, delta=total_rows,
            )

        log.success(
            "Backfill completado",
            idx=idx, total=total, chunks=chunks, rows=total_rows,
        )

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
                _log.bind(
                    exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe,
                ).debug(
                    "Origin cache hit",
                    origin=pd.Timestamp(ts_ms, unit="ms", tz="UTC").isoformat(),
                )
                return ts_ms
        except Exception as _cache_exc:
            _log.bind(
                exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe,
            ).debug("Origin cache read failed", error=str(_cache_exc))

        try:
            raw_data = await ctx.fetcher.fetch_chunk(
                symbol=symbol, timeframe=timeframe, since=1, limit=1,
            )
            if not raw_data:
                return None

            origin_ms = int(raw_data[0][0])

            # Sanity: exchanges que ignoran since=1 devuelven near-now. Fallback.
            _now_ms     = int(time.time() * 1000)
            _one_day_ms = 86_400_000
            if origin_ms > _now_ms - _one_day_ms:
                _log.bind(
                    exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe,
                ).info(
                    "Origin discovery: since=1 no soportado — usando fallback",
                    returned_origin=pd.Timestamp(origin_ms, unit="ms", tz="UTC").isoformat(),
                )
                origin_ms = get_origin_fallback_ms(ctx.exchange_id, ctx.market_type)

            try:
                ctx.cursor.set_raw(cache_key, str(origin_ms), _BACKFILL_TTL_SECONDS)
            except Exception as _cache_exc:
                _log.bind(
                    exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe,
                ).debug("Origin cache write failed", error=str(_cache_exc))

            return origin_ms

        except Exception as exc:
            _log.bind(
                exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe,
            ).warning("Origin discovery failed", error=str(exc))
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
        """start_date actúa como floor: el backfill nunca retrocede más allá."""
        start_date_ms = self._parse_start_date_ms(ctx.start_date, origin_ms)

        # A. Cursor Redis — reanuda desde donde se detuvo
        try:
            bk_key = self._backfill_key(ctx, symbol, timeframe)
            raw = ctx.cursor.get_raw(bk_key)
            if raw:
                return max(int(raw), start_date_ms)
        except Exception as exc:
            _log.bind(
                exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe,
            ).debug("Backfill cursor read failed (non-critical)", error=str(exc))

        # B. Cold start: paginar desde now hacia origin
        # Kappa: cursor Redis es SSOT. Sin cursor → cold start desde now.
        # El fallback a Silver (get_oldest_timestamp) fue eliminado —
        # el productor REST no consulta el lago para resolver su posición.
        now_ms           = int(pd.Timestamp.utcnow().timestamp() * 1000)
        effective_origin = max(start_date_ms, origin_ms)
        _log.bind(
            exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe,
        ).info(
            "Backfill cold start — paginando desde now hacia origin",
            effective_origin=pd.Timestamp(effective_origin, unit="ms", tz="UTC").isoformat(),
        )
        return now_ms

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
            _log.bind(
                exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe,
            ).debug("Backfill cursor write failed (non-critical)", error=str(exc))

    @staticmethod
    def _parse_start_date_ms(
        start_date: str,
        origin_ms:  Optional[int] = None,
    ) -> int:
        if start_date == "auto":
            if origin_ms is None:
                raise ValueError(
                    "start_date='auto' requiere origin_ms — "
                    "llamar después de _discover_origin()."
                )
            return origin_ms
        return int(pd.Timestamp(start_date, tz="UTC").value // 1_000_000)

    async def _paginate_backward(
        self,
        symbol:    str,
        timeframe: str,
        since_ms:  int,
        origin_ms: int,
        ctx:       PipelineContext,
    ) -> tuple[int, int]:

        tf_ms        = timeframe_to_ms(timeframe)
        chunk_limit  = DEFAULT_CHUNK_LIMIT
        current_end  = since_ms
        total_rows   = 0
        chunks       = 0
        last_end     = None
        log          = _log.bind(
            exchange=ctx.exchange_id, symbol=symbol,
            timeframe=timeframe, mode="backfill",
        )

        start_date_ms_pg    = self._parse_start_date_ms(ctx.start_date, origin_ms)
        effective_origin_pg = max(start_date_ms_pg, origin_ms)

        for _ in range(MAX_BACKFILL_CHUNKS):

            if current_end <= effective_origin_pg:
                log.info(
                    "Backfill: paginacion completa — origen alcanzado",
                    effective_origin=pd.Timestamp(
                        effective_origin_pg, unit="ms", tz="UTC",
                    ).isoformat(),
                )
                break

            chunk_start    = max(
                current_end - (chunk_limit * tf_ms), effective_origin_pg,
            )
            effective_span = current_end - chunk_start

            if effective_span <= 0:
                log.warning(
                    "Backfill: effective_span degenerado — abortando",
                    effective_span_ms=effective_span, tf_ms=tf_ms,
                )
                break

            log.debug(
                "Backfill chunk",
                chunk=chunks + 1,
                range_start=pd.Timestamp(chunk_start, unit="ms", tz="UTC").strftime("%Y-%m-%d %H:%M"),
                range_end=pd.Timestamp(current_end,  unit="ms", tz="UTC").strftime("%Y-%m-%d %H:%M"),
            )

            try:
                _quirks = get_quirks(ctx.exchange_id)
                if _quirks.backward_pagination:
                    raw = await ctx.fetcher.fetch_chunk(
                        symbol=symbol, timeframe=timeframe,
                        since=None, limit=chunk_limit, end_ms=current_end,
                    )
                else:
                    raw = await ctx.fetcher.fetch_chunk(
                        symbol=symbol, timeframe=timeframe,
                        since=chunk_start, limit=chunk_limit,
                    )
            except Exception as exc:
                log.warning("Backfill chunk fetch failed", error=str(exc))
                break

            if not raw:
                log.warning(
                    "Empty chunk — advancing cursor defensively",
                    chunk_start=pd.Timestamp(chunk_start, unit="ms", tz="UTC").strftime("%Y-%m-%d %H:%M"),
                )
                current_end = chunk_start
                self._update_backfill_cursor(symbol, timeframe, current_end, ctx)
                break

            if len(raw) < chunk_limit * 0.1:
                log.warning(
                    "Sparse chunk — possible data gap or exchange throttling",
                    received=len(raw), expected=chunk_limit,
                )

            df = pd.DataFrame(
                raw, columns=["timestamp", "open", "high", "low", "close", "volume"],
            )
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
            df = df.sort_values("timestamp").reset_index(drop=True)
            df = df[df["timestamp"] < pd.Timestamp(current_end, unit="ms", tz="UTC")]

            if df.empty:
                break

            oldest_in_chunk = int(df["timestamp"].min().timestamp() * 1000)

            if last_end is not None and oldest_in_chunk >= last_end:
                log.warning(
                    "Backfill anti-loop detectado — abortando paginación",
                    oldest_in_chunk=oldest_in_chunk, last_end=last_end,
                )
                break

            qres = ctx.quality.run(
                df=df, symbol=symbol,
                timeframe=timeframe, exchange=ctx.exchange_id,
            )

            if qres.accepted:
                try:
                    if ctx.publisher is not None:
                        converter: OHLCVChunkConverterPort = ctx._chunk_converter  # type: ignore[assignment]
                        chunk = converter.to_chunk(
                            df        = qres.df,
                            exchange  = ctx.exchange_id,
                            symbol    = symbol,
                            timeframe = timeframe,
                            source    = SOURCE_BACKFILL,
                            run_id    = getattr(ctx, "run_id", ""),
                        )
                        ok = await ctx.publisher.publish_chunk(chunk)
                        if not ok:
                            log.warning(
                                "Backfill chunk kafka publish failed — cursor NO avanzado",
                                chunk=chunks + 1,
                            )
                            ctx.metrics.pipeline_errors_inc(
                                exchange=ctx.exchange_id, error_type="transient",
                            )
                            current_end = oldest_in_chunk
                            continue
                    else:
                        # Kappa: publisher es obligatorio. Sin publisher → error fatal.
                        # No hay fallback a Iceberg — el productor no escribe al lago.
                        log.error(
                            "publisher=None — Kappa requiere publisher. "
                            "Verificar KAFKA_ENABLED y broker. Abortando chunk.",
                            chunk=chunks + 1,
                        )
                        ctx.metrics.pipeline_errors_inc(
                            exchange=ctx.exchange_id, error_type="fatal",
                        )
                        raise RuntimeError(
                            "BackfillStrategy: ctx.publisher es None. "
                            "En modo Kappa el publisher es obligatorio."
                        )
                    total_rows += chunk.count
                    self._update_backfill_cursor(
                        symbol, timeframe,
                        chunk.candles[-1].timestamp_ms, ctx,
                    )

                except Exception as exc:
                    ctx.metrics.pipeline_errors_inc(
                        exchange=ctx.exchange_id, error_type="fatal",
                    )
                    log.error(
                        "Backfill chunk save failed — cursor NO avanzado, abortando",
                        error=str(exc), chunk=chunks + 1,
                        oldest=pd.Timestamp(oldest_in_chunk, unit="ms", tz="UTC").isoformat(),
                    )
                    raise
            else:
                log.warning(
                    "Backfill chunk rechazado por calidad — ventana NO avanzada",
                    score=round(qres.score, 1), chunk=chunks + 1,
                )
                current_end = oldest_in_chunk
                continue

            chunks  += 1
            last_end = oldest_in_chunk
            current_end = oldest_in_chunk

            log.debug(
                "Backfill progress",
                chunk=chunks, rows_chunk=len(df), total_rows=total_rows,
                oldest=pd.Timestamp(current_end, unit="ms", tz="UTC").strftime("%Y-%m-%d"),
            )

            if current_end <= effective_origin_pg:
                log.info("Backfill alcanzó el origen")
                break

        # Kappa: commit_version eliminado — Iceberg lo gestiona el consumer,
        # no el productor. El cursor Redis ya fue actualizado chunk a chunk.
        return total_rows, chunks

    # ----------------------------------------------------------
    # Key helpers
    # ----------------------------------------------------------

    def _origin_key(self, ctx: PipelineContext, symbol: str, timeframe: str) -> str:
        env = getattr(ctx.cursor, "_env", "development")
        return (
            f"{env}:{_ORIGIN_KEY_PREFIX}:"
            f"{_encode(ctx.exchange_id)}:{_encode(symbol)}:{_encode(timeframe)}"
        )

    def _backfill_key(self, ctx: PipelineContext, symbol: str, timeframe: str) -> str:
        env = getattr(ctx.cursor, "_env", "development")
        return (
            f"{env}:{_BACKFILL_KEY_PREFIX}:"
            f"{_encode(ctx.exchange_id)}:{_encode(symbol)}:{_encode(timeframe)}"
        )
