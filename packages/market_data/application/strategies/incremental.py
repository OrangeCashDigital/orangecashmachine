# -*- coding: utf-8 -*-
"""
market_data/application/strategies/incremental.py
===================================================

Strategy de ingestión incremental hacia adelante.

Orquesta: fetcher · quality gate · publisher (port) · storage · cursor · throttle (port).

Capas permitidas: domain/ · ports/ · ocm/
Prohibidas (DIP): adapters/ · infrastructure/

Principios: SRP · DIP · SafeOps · KISS
"""
from __future__ import annotations

import asyncio

from loguru import logger

# ── Domain ───────────────────────────────────────────────────────────────────
from market_data.domain.policies.base import (
    PairResult,
    PipelineContext,
    PipelineMode,
    StrategyMixin,
)

# ── Ports ─────────────────────────────────────────────────────────────────────
from market_data.ports.outbound.publisher import SOURCE_LIVE

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

        ctx.metrics.quality_decisions_inc(
            exchange    = ctx.exchange_id,
            market_type = ctx.market_type,
            symbol      = symbol,
            timeframe   = timeframe,
            decision    = qres.tier.value,
        )

        if not qres.accepted:
            logger.warning(
                "Par rechazado por calidad [{}/{}] | exchange={} symbol={} "
                "timeframe={} score={:.1f}",
                idx, total, ctx.exchange_id, symbol, timeframe, qres.score,
            )
            result.skipped = True
            return

        # ── Kappa router ──────────────────────────────────────────────────────
        if ctx.publisher is not None:
            ok = await ctx.publisher.publish_chunk(
                exchange_id = ctx.exchange_id,
                symbol      = symbol,
                timeframe   = timeframe,
                df          = qres.df,
                source      = SOURCE_LIVE,
                run_id      = getattr(ctx, "run_id", ""),
            )
            if not ok:
                logger.warning(
                    "Incremental kafka publish failed — cursor NO actualizado "
                    "[{}/{}] | symbol={} timeframe={}",
                    idx, total, symbol, timeframe,
                )
                ctx.metrics.pipeline_errors_inc(
                    exchange=ctx.exchange_id, error_type="transient",
                )
                result.skipped = True
                return
        else:
            # Kappa: publisher es obligatorio. Sin publisher → error fatal.
            # No hay path Lambda — el productor nunca escribe al lago directamente.
            logger.error(
                "publisher=None — Kappa requiere publisher. "
                "Verificar KAFKA_ENABLED y broker. Abortando par. "
                "[{}/{}] | symbol={} timeframe={}",
                idx, total, symbol, timeframe,
            )
            ctx.metrics.pipeline_errors_inc(
                exchange=ctx.exchange_id, error_type="fatal",
            )
            result.skipped = True
            return

        # ── Cursor update ─────────────────────────────────────────────────────
        last_ts_ms = int(qres.df["timestamp"].max().timestamp() * 1000)
        ctx.cursor.update(ctx.exchange_id, symbol, timeframe, last_ts_ms)

        result.rows = len(qres.df)

        ctx.metrics.rows_ingested_inc(
            exchange=ctx.exchange_id, timeframe=timeframe, delta=result.rows,
        )
        ctx.metrics.pair_duration_observe(
            exchange=ctx.exchange_id, symbol=symbol,
            timeframe=timeframe, seconds=result.duration_ms / 1000,
        )

        logger.info(
            "Incremental completado [{}/{}] | exchange={} market={} "
            "symbol={} timeframe={} rows={} duration={}ms",
            idx, total, ctx.exchange_id, ctx.market_type,
            symbol, timeframe, result.rows, result.duration_ms,
        )

    # ── helpers privados ──────────────────────────────────────────────────────
    # _append_bronze_with_retry eliminado — Kappa: Bronze lo escribe el consumer,
    # no el productor. IncrementalStrategy solo publica a Kafka.
