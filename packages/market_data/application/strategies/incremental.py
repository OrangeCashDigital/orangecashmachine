# -*- coding: utf-8 -*-
"""
market_data/application/strategies/incremental.py
===================================================

Strategy de ingestión incremental hacia adelante.

Orquesta: fetcher · quality gate · kafka_producer · storage · cursor · throttle.

Principios: SRP · DIP · SafeOps · KISS
"""
from __future__ import annotations

import asyncio
import random

from loguru import logger

# ── Domain (tipos y contratos) ────────────────────────────────────────────────
from market_data.domain.policies.base import (
    PairResult,
    PipelineContext,
    PipelineMode,
    StrategyMixin,
)

# ── Application (helper DRY — SSOT para publicar a ohlcv.raw) ────────────────
from market_data.application.strategies.backfill import _publish_chunk_to_kafka

# ── Adapters ──────────────────────────────────────────────────────────────────
from market_data.adapters.outbound.exchange.throttle import get_or_create_throttle

# ── Infrastructure ────────────────────────────────────────────────────────────
from market_data.infrastructure.kafka.payloads import DATASOURCE_LIVE
from market_data.infrastructure.observability.metrics import (
    PAIR_DURATION,
    PIPELINE_ERRORS,
    QUALITY_DECISIONS,
    ROWS_INGESTED,
)

# Parámetros del retry OCC — SSOT a nivel de módulo (DRY, KISS).
_BRONZE_MAX_RETRIES: int   = 5
_BRONZE_BASE_WAIT_S: float = 0.1   # backoff base: 100ms × 2^(intento-1) ± 25% jitter


class IncrementalStrategy(StrategyMixin):
    _mode = PipelineMode.INCREMENTAL

    async def _run(
        self,
        symbol:    str,
        timeframe: str,
        idx:       int,
        total:     int,
        ctx:       PipelineContext,
        result:    PairResult,
    ) -> None:

        logger.debug(
            "Incremental [{}/{}] | exchange={} symbol={} timeframe={}",
            idx, total, ctx.exchange_id, symbol, timeframe,
        )

        df = await ctx.fetcher.download_data(
            symbol     = symbol,
            timeframe  = timeframe,
            start_date = ctx.start_date,
        )

        if df is None or df.empty:
            result.skipped = True
            logger.debug(
                "Sin datos nuevos [{}/{}] | symbol={} timeframe={}",
                idx, total, symbol, timeframe,
            )
            return

        # ── Quality gate ──────────────────────────────────────────────────────
        qres = ctx.quality.run(
            df=df, symbol=symbol,
            timeframe=timeframe, exchange=ctx.exchange_id,
        )

        QUALITY_DECISIONS.labels(
            exchange=ctx.exchange_id, market_type=ctx.market_type,
            symbol=symbol, timeframe=timeframe, decision=qres.tier.value,
        ).inc()

        if not qres.accepted:
            logger.warning(
                "Par rechazado por calidad [{}/{}] | exchange={} symbol={} "
                "timeframe={} score={:.1f} "
                "(datos descartados — no publicados a Kafka ni a Iceberg)",
                idx, total, ctx.exchange_id, symbol, timeframe, qres.score,
            )
            result.skipped = True
            return

        # ── Kappa router ──────────────────────────────────────────────────────
        if ctx.kafka_producer is not None:
            ok = await _publish_chunk_to_kafka(
                ctx=ctx, symbol=symbol,
                timeframe=timeframe, df=qres.df,
            )
            if not ok:
                logger.warning(
                    "Incremental kafka publish failed — cursor NO actualizado "
                    "[{}/{}] | symbol={} timeframe={}",
                    idx, total, symbol, timeframe,
                )
                PIPELINE_ERRORS.labels(
                    exchange=ctx.exchange_id, error_type="transient",
                ).inc()
                result.skipped = True
                return
        else:
            # Degraded path: sin Kafka → Bronze + Silver directo (modo Lambda)
            logger.warning(
                "kafka_producer=None — modo degradado: Bronze+Silver directo "
                "[{}/{}] | symbol={} timeframe={}",
                idx, total, symbol, timeframe,
            )
            run_id = await self._append_bronze_with_retry(
                df=df, symbol=symbol, timeframe=timeframe, ctx=ctx,
            )
            async with ctx.silver_commit_lock:
                ctx.storage.save_ohlcv(
                    df=qres.df, symbol=symbol,
                    timeframe=timeframe, run_id=run_id,
                )

        # ── Cursor update ─────────────────────────────────────────────────────
        last_ts_ms = int(qres.df["timestamp"].max().timestamp() * 1000)
        ctx.cursor.update(ctx.exchange_id, symbol, timeframe, last_ts_ms)

        result.rows = len(qres.df)

        ROWS_INGESTED.labels(
            exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe,
        ).inc(result.rows)
        PAIR_DURATION.labels(
            exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe,
        ).observe(result.duration_ms / 1000)

        logger.info(
            "Incremental completado [{}/{}] | exchange={} market={} "
            "symbol={} timeframe={} rows={} duration={}ms",
            idx, total, ctx.exchange_id, ctx.market_type,
            symbol, timeframe, result.rows, result.duration_ms,
        )

    # ── helpers privados ──────────────────────────────────────────────────────

    async def _append_bronze_with_retry(
        self,
        df,
        symbol:    str,
        timeframe: str,
        ctx:       PipelineContext,
    ) -> str:
        """
        Append a Bronze con retry OCC y backoff exponencial con jitter.

        Usado SOLO en modo degradado (ctx.kafka_producer is None).
        En modo Kappa, Bronze se escribe via KafkaBronzeWriter.
        """
        throttle = get_or_create_throttle(
            exchange_id = ctx.exchange_id,
            market_type = ctx.market_type,
            dataset     = "ohlcv",
            initial     = 5,
            maximum     = 20,
        )

        last_occ_wait: float = 0.0

        for attempt in range(1, _BRONZE_MAX_RETRIES + 1):
            async with ctx.bronze_commit_lock:
                try:
                    return ctx.bronze.append(
                        df=df, symbol=symbol, timeframe=timeframe,
                    )
                except Exception as exc:
                    msg    = str(exc).lower()
                    is_occ = (
                        "branch main has changed"               in msg
                        or "requirement failed"                 in msg
                        or "has been updated by another process" in msg
                    )

                    if not is_occ or attempt >= _BRONZE_MAX_RETRIES:
                        raise

                    throttle.record_occ_conflict()

                    last_occ_wait  = _BRONZE_BASE_WAIT_S * (2 ** (attempt - 1))
                    last_occ_wait *= 1 + random.uniform(-0.25, 0.25)

                    logger.warning(
                        "Bronze OCC conflict — retry {}/{} | {}/{} wait={:.0f}ms",
                        attempt, _BRONZE_MAX_RETRIES,
                        symbol, timeframe,
                        last_occ_wait * 1000,
                    )

            await asyncio.sleep(last_occ_wait)

        raise RuntimeError(
            f"Bronze OCC: reintentos agotados para {symbol}/{timeframe}",
        )
