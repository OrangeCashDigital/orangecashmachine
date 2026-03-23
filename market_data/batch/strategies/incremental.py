"""
market_data/batch/strategies/incremental.py
============================================

Strategy de ingestión incremental hacia adelante.
El boilerplate de timing/errores/métricas vive en StrategyMixin.
"""

from __future__ import annotations

import asyncio

from loguru import logger

from market_data.batch.strategies.base import (
    PairResult,
    PipelineContext,
    PipelineMode,
    StrategyMixin,
)
from services.observability.metrics import (
    PAIR_DURATION,
    QUALITY_DECISIONS,
    ROWS_INGESTED,
)


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

        run_id = ctx.bronze.append(df=df, symbol=symbol, timeframe=timeframe)

        qres = ctx.quality.run(
            df=df, symbol=symbol, timeframe=timeframe, exchange=ctx.exchange_id,
        )

        QUALITY_DECISIONS.labels(
            exchange=ctx.exchange_id, market_type=ctx.market_type,
            symbol=symbol, timeframe=timeframe, decision=qres.tier.value,
        ).inc()

        if not qres.accepted:
            logger.warning(
                "Par rechazado por calidad [{}/{}] | exchange={} symbol={} timeframe={} score={:.1f}",
                idx, total, ctx.exchange_id, symbol, timeframe, qres.score,
            )
            result.skipped = True
            return

        ctx.storage.save_ohlcv(
            df=qres.df, symbol=symbol, timeframe=timeframe, run_id=run_id,
        )

        # _normalize_dataframe garantiza que timestamp es datetime64[ns, UTC]
        # .timestamp() es siempre válido aquí — sin necesidad de hasattr
        last_ts_ms = int(df["timestamp"].max().timestamp() * 1000)
        ctx.cursor.update(ctx.exchange_id, symbol, timeframe, last_ts_ms)

        result.rows = len(df)

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
