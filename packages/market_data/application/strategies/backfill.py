# -*- coding: utf-8 -*-
"""
market_data/application/strategies/backfill.py
================================================

Strategy de backfill histórico completo hacia atrás.

Orquesta: fetcher (REST) · cursor (Redis) · storage (Iceberg) ·
          kafka_producer · quality gate · exchange_quirks.

No contiene lógica de negocio pura — esa vive en domain/policies/base.py.

Principios: SRP · DIP (depende de ports, no de infra concreta) · SafeOps
"""
from __future__ import annotations

import asyncio
import time
import uuid as _uuid
from typing import Optional

import pandas as pd
from ocm.observability import bind_pipeline

# ── Domain (tipos y contratos) ───────────────────────────────────────────────
from market_data.domain.constants import DEFAULT_CHUNK_LIMIT, MAX_BACKFILL_CHUNKS
from market_data.domain.policies.base import (
    PairResult,
    PipelineContext,
    PipelineMode,
    StrategyMixin,
)
from market_data.domain.value_objects.timeframe import timeframe_to_ms

# ── Adapters (detalles de exchange — solo lo que application necesita) ────────
from market_data.adapters.outbound.exchange.exchange_quirks import (
    get_origin_fallback_ms,
    get_quirks,
)

# ── Infrastructure (Kafka wire format) ───────────────────────────────────────
from market_data.infrastructure.kafka.payloads import (
    EventPayload,
    KafkaOHLCVBar as OHLCVBar,
    DATASOURCE_BACKFILL,
    PAYLOAD_SCHEMA_VERSION,
)
from market_data.infrastructure.kafka.serializer import serialize, make_routing_key
from market_data.infrastructure.observability.metrics import ROWS_INGESTED, PIPELINE_ERRORS

# ── Ports (constantes de protocolo Kafka) ────────────────────────────────────
from market_data.ports.outbound.kafka_producer import (
    TOPIC_OHLCV_RAW,
    HEADER_SOURCE,
    HEADER_VERSION,
    HEADER_RUN_ID,
)

# ── OCM platform ─────────────────────────────────────────────────────────────
from ocm.runtime.state.encoding import encode_redis_key as _encode

_log = bind_pipeline("backfill")

_BACKFILL_TTL_SECONDS: int = 30 * 86_400
_ORIGIN_KEY_PREFIX:    str = "origin"
_BACKFILL_KEY_PREFIX:  str = "backfill"


# ---------------------------------------------------------------------------
# Kafka publish helper — Kappa: backfill entra a ohlcv.raw igual que live
# ---------------------------------------------------------------------------

async def _publish_chunk_to_kafka(
    ctx:       "PipelineContext",
    symbol:    str,
    timeframe: str,
    df:        "pd.DataFrame",
) -> bool:
    """
    Publica un chunk de backfill a ohlcv.raw.

    Wire format: EventPayload con source=DATASOURCE_BACKFILL.
    Routing key: "{exchange}:{symbol}:{timeframe}" — orden por par garantizado.
    Headers Kappa: x-ocm-source, x-ocm-version, x-ocm-run-id.

    Fail-Fast: lanza si df está vacío (bug del caller, no error de red).
    SafeOps:   retorna False si el producer falla — el caller decide el fallback.
    """
    if df is None or df.empty:
        raise ValueError("_publish_chunk_to_kafka: df vacío — bug del caller")

    bars = [
        OHLCVBar(
            ts     = int(row["timestamp"].timestamp() * 1000),
            open   = float(row["open"]),
            high   = float(row["high"]),
            low    = float(row["low"]),
            close  = float(row["close"]),
            volume = float(row["volume"]),
        )
        for _, row in df.iterrows()
    ]

    event = EventPayload(
        event_id       = str(_uuid.uuid4()),
        exchange       = ctx.exchange_id,
        symbol         = symbol,
        timeframe      = timeframe,
        batch_start_ts = int(df["timestamp"].min().timestamp() * 1000),
        bars           = bars,
        source         = DATASOURCE_BACKFILL,
        run_id         = getattr(ctx, "run_id", ""),
    )

    payload_bytes = serialize(event)
    routing_key   = make_routing_key(ctx.exchange_id, symbol, timeframe)
    headers       = {
        HEADER_SOURCE:  DATASOURCE_BACKFILL,
        HEADER_VERSION: str(PAYLOAD_SCHEMA_VERSION),
        HEADER_RUN_ID:  getattr(ctx, "run_id", ""),
    }

    assert ctx.kafka_producer is not None, "_publish_chunk_to_kafka: kafka_producer es None"
    return await ctx.kafka_producer.send_async(
        topic   = TOPIC_OHLCV_RAW,
        value   = payload_bytes,
        key     = routing_key,
        headers = headers,
    )


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
            ROWS_INGESTED.labels(
                exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe,
            ).inc(total_rows)

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

            # Sanity check: exchanges que ignoran since=1 devuelven la vela más
            # reciente. Si origin_ms está dentro de las últimas 24h, usar fallback.
            _now_ms     = int(time.time() * 1000)
            _one_day_ms = 86_400_000
            if origin_ms > _now_ms - _one_day_ms:
                _log.bind(
                    exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe,
                ).info(
                    "Origin discovery: exchange returned near-now ts — "
                    "since=1 not supported, falling back to exchange origin",
                    returned_origin=pd.Timestamp(origin_ms, unit="ms", tz="UTC").isoformat(),
                )
                origin_ms = get_origin_fallback_ms(ctx.exchange_id, ctx.market_type)

            try:
                ctx.cursor.set_raw(cache_key, str(origin_ms), _BACKFILL_TTL_SECONDS)
                _log.bind(
                    exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe,
                ).debug(
                    "Origin cached",
                    origin=pd.Timestamp(origin_ms, unit="ms", tz="UTC").isoformat(),
                )
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
        """
        Resuelve el timestamp desde el cual continuar el backfill.

        start_date actúa como floor: el backfill nunca retrocede más allá de
        la fecha configurada, independientemente de lo que ofrezca el exchange.
        """
        start_date_ms = self._parse_start_date_ms(ctx.start_date, origin_ms)

        # A. Cursor Redis — reanuda desde donde se detuvo, respetando floor
        try:
            bk_key = self._backfill_key(ctx, symbol, timeframe)
            raw = ctx.cursor.get_raw(bk_key)
            if raw:
                ts_ms = max(int(raw), start_date_ms)
                _log.bind(
                    exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe,
                ).debug(
                    "Backfill cursor hit",
                    ts=pd.Timestamp(ts_ms, unit="ms", tz="UTC").isoformat(),
                )
                return ts_ms
        except Exception as exc:
            _log.bind(
                exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe,
            ).debug("Backfill cursor read failed (non-critical)", error=str(exc))

        # B. Oldest timestamp en Silver — respetando floor
        oldest = await asyncio.to_thread(
            self._get_oldest_silver_ts, ctx, symbol, timeframe,
        )
        if oldest is not None:
            return max(int(oldest.timestamp() * 1000), start_date_ms)

        # C. Cold start: paginar desde now hacia origin
        now_ms           = int(pd.Timestamp.utcnow().timestamp() * 1000)
        effective_origin = max(start_date_ms, origin_ms)
        _log.bind(
            exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe,
        ).info(
            "Backfill cold start — paginando desde now hacia origin",
            start_date=ctx.start_date,
            effective_origin=pd.Timestamp(effective_origin, unit="ms", tz="UTC").isoformat(),
            now=pd.Timestamp(now_ms, unit="ms", tz="UTC").isoformat(),
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
        """Convierte start_date ISO 8601 a milliseconds epoch.

        Si start_date == 'auto', resuelve al inicio del exchange (origin_ms).
        Requiere origin_ms cuando start_date == 'auto'.
        """
        if start_date == "auto":
            if origin_ms is None:
                raise ValueError(
                    "start_date='auto' requiere origin_ms — "
                    "llamar después de _discover_origin()."
                )
            return origin_ms
        return int(pd.Timestamp(start_date, tz="UTC").value // 1_000_000)

    def _get_oldest_silver_ts(
        self,
        ctx:       PipelineContext,
        symbol:    str,
        timeframe: str,
    ) -> Optional[pd.Timestamp]:
        """Delega al Protocol OHLCVStorage — IcebergStorage."""
        try:
            return ctx.storage.get_oldest_timestamp(symbol, timeframe)
        except Exception as exc:
            _log.bind(symbol=symbol, timeframe=timeframe).warning(
                "Oldest silver ts failed", error=str(exc),
            )
            return None

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
                    current_end=current_end, chunk_start=chunk_start,
                )
                break

            log.debug(
                "Backfill chunk",
                chunk=chunks + 1,
                range_start=pd.Timestamp(
                    chunk_start, unit="ms", tz="UTC",
                ).strftime("%Y-%m-%d %H:%M"),
                range_end=pd.Timestamp(
                    current_end, unit="ms", tz="UTC",
                ).strftime("%Y-%m-%d %H:%M"),
            )

            try:
                _quirks = get_quirks(ctx.exchange_id)
                if _quirks.backward_pagination:
                    raw = await ctx.fetcher.fetch_chunk(
                        symbol=symbol, timeframe=timeframe,
                        since=None, limit=chunk_limit,
                        end_ms=current_end,
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
                    chunk_start=pd.Timestamp(
                        chunk_start, unit="ms", tz="UTC",
                    ).strftime("%Y-%m-%d %H:%M"),
                    current_end=pd.Timestamp(
                        current_end, unit="ms", tz="UTC",
                    ).strftime("%Y-%m-%d %H:%M"),
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
                raw,
                columns=["timestamp", "open", "high", "low", "close", "volume"],
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
                    if ctx.kafka_producer is not None:
                        ok = await _publish_chunk_to_kafka(
                            ctx=ctx, symbol=symbol,
                            timeframe=timeframe, df=qres.df,
                        )
                        if not ok:
                            log.warning(
                                "Backfill chunk kafka publish failed — "
                                "cursor NO avanzado",
                                chunk=chunks + 1,
                                oldest=pd.Timestamp(
                                    oldest_in_chunk, unit="ms", tz="UTC",
                                ).isoformat(),
                            )
                            PIPELINE_ERRORS.labels(
                                exchange=ctx.exchange_id,
                                error_type="transient",
                            ).inc()
                            current_end = oldest_in_chunk
                            continue
                    else:
                        log.warning(
                            "kafka_producer=None — modo degradado: "
                            "escribiendo directo a Iceberg",
                            chunk=chunks + 1,
                        )
                        ctx.storage.save_ohlcv(
                            df=qres.df, symbol=symbol, timeframe=timeframe,
                            skip_versioning=True,
                        )
                    total_rows += len(qres.df)
                    self._update_backfill_cursor(
                        symbol, timeframe, oldest_in_chunk, ctx,
                    )
                except Exception as exc:
                    PIPELINE_ERRORS.labels(
                        exchange=ctx.exchange_id, error_type="fatal",
                    ).inc()
                    log.error(
                        "Backfill chunk save failed — cursor NO avanzado, abortando",
                        error=str(exc), chunk=chunks + 1,
                        oldest=pd.Timestamp(
                            oldest_in_chunk, unit="ms", tz="UTC",
                        ).isoformat(),
                    )
                    raise
            else:
                log.warning(
                    "Backfill chunk rechazado por calidad — ventana NO avanzada",
                    score=round(qres.score, 1), chunk=chunks + 1,
                    oldest=pd.Timestamp(
                        oldest_in_chunk, unit="ms", tz="UTC",
                    ).isoformat(),
                )
                current_end = oldest_in_chunk
                continue

            chunks  += 1
            last_end = oldest_in_chunk

            current_end = oldest_in_chunk

            log.debug(
                "Backfill progress",
                chunk=chunks, rows_chunk=len(df), total_rows=total_rows,
                oldest=pd.Timestamp(
                    current_end, unit="ms", tz="UTC",
                ).strftime("%Y-%m-%d"),
            )

            if current_end <= effective_origin_pg:
                log.info(
                    "Backfill alcanzó el origen",
                    effective_origin=pd.Timestamp(
                        effective_origin_pg, unit="ms", tz="UTC",
                    ).isoformat(),
                )
                break

        # Commit final: versión consolidada en latest.json tras la paginación.
        if total_rows > 0:
            try:
                ctx.storage.commit_version(
                    symbol=symbol, timeframe=timeframe,
                    run_id=getattr(ctx, "run_id", None),
                )
            except Exception as exc:
                _log.bind(
                    exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe,
                ).warning(
                    "Backfill commit_version failed (non-critical)",
                    error=str(exc),
                )

        return total_rows, chunks

    # ----------------------------------------------------------
    # Key helpers
    # ----------------------------------------------------------

    def _origin_key(
        self, ctx: PipelineContext, symbol: str, timeframe: str,
    ) -> str:
        env = getattr(ctx.cursor, "_env", "development")
        return (
            f"{env}:{_ORIGIN_KEY_PREFIX}:"
            f"{_encode(ctx.exchange_id)}:{_encode(symbol)}:{_encode(timeframe)}"
        )

    def _backfill_key(
        self, ctx: PipelineContext, symbol: str, timeframe: str,
    ) -> str:
        env = getattr(ctx.cursor, "_env", "development")
        return (
            f"{env}:{_BACKFILL_KEY_PREFIX}:"
            f"{_encode(ctx.exchange_id)}:{_encode(symbol)}:{_encode(timeframe)}"
        )
