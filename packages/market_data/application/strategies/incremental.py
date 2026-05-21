"""
market_data/application/strategies/incremental.py
===================================================

Strategy de ingestión incremental hacia adelante.

Orquesta: fetcher · quality gate · publisher (port) · cursor · throttle (port).

Capas permitidas: domain/ · ports/ · ocm/ · adapters/
Prohibidas (DIP): infrastructure/

Principios: SRP · DIP · SafeOps · KISS · Clean Architecture
"""

from __future__ import annotations

from loguru import logger

from market_data.domain.policies.base import (
    PairResult,
    PipelineContext,
    PipelineMode,
    StrategyMixin,
)
from market_data.ports.outbound.publisher import SOURCE_LIVE
from market_data.ports.outbound.chunk_converter import OHLCVChunkConverterPort


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

        # ── Kappa router — dominio preservado hasta el publisher ─────────────
        if ctx.publisher is not None:
            converter = ctx.get_chunk_converter()  # fail-fast si no inyectado
            chunk = converter.to_chunk(
                df        = qres.df,
                exchange  = ctx.exchange_id,
                symbol    = symbol,
                timeframe = timeframe,
                source    = SOURCE_LIVE,
                run_id    = getattr(ctx, "run_id", ""),
            )
            ok = await ctx.publisher.publish_chunk(chunk)
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

        # ── Cursor update y métricas desde el dominio ────────────────────────
        last_ts_ms = chunk.candles[-1].timestamp_ms
        ctx.cursor.update(ctx.exchange_id, symbol, timeframe, last_ts_ms)

        result.rows = chunk.count

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
