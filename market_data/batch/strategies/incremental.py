"""
market_data/batch/strategies/incremental.py
============================================

Strategy de ingestión incremental hacia adelante.
Extrae exactamente la lógica de HistoricalPipelineAsync._process_pair.
"""

from __future__ import annotations

import asyncio
import time

from loguru import logger

from market_data.batch.strategies.base import (
    PairResult,
    PipelineContext,
    PipelineMode,
)
from services.observability.metrics import (
    PAIR_DURATION,
    PIPELINE_ERRORS,
    QUALITY_DECISIONS,
    ROWS_INGESTED,
)


class IncrementalStrategy:

    async def execute_pair(
        self,
        symbol:    str,
        timeframe: str,
        idx:       int,
        total:     int,
        ctx:       PipelineContext,
    ) -> PairResult:

        result     = PairResult(symbol=symbol, timeframe=timeframe, mode=PipelineMode.INCREMENTAL)
        pair_start = time.monotonic()

        try:
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
                result.skipped     = True
                result.duration_ms = int((time.monotonic() - pair_start) * 1000)
                logger.debug(
                    "Sin datos nuevos [{}/{}] | symbol={} timeframe={}",
                    idx, total, symbol, timeframe,
                )
                return result

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
                result.skipped     = True
                result.duration_ms = int((time.monotonic() - pair_start) * 1000)
                return result

            ctx.storage.save_ohlcv(
                df=qres.df, symbol=symbol, timeframe=timeframe, run_id=run_id,
            )

            last_ts_ms = int(
                df["timestamp"].max().timestamp() * 1000
                if hasattr(df["timestamp"].max(), "timestamp")
                else df["timestamp"].max()
            )
            ctx.cursor.update(ctx.exchange_id, symbol, timeframe, last_ts_ms)

            result.rows        = len(df)
            result.duration_ms = int((time.monotonic() - pair_start) * 1000)

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

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            result.error       = str(exc)
            result.duration_ms = int((time.monotonic() - pair_start) * 1000)
            error_type = "transient" if result.is_transient_error else "fatal"
            PIPELINE_ERRORS.labels(
                exchange=ctx.exchange_id, error_type=error_type,
            ).inc()
            logger.error(
                "Incremental fallido [{}/{}] | exchange={} symbol={} "
                "timeframe={} error={} duration={}ms",
                idx, total, ctx.exchange_id, symbol, timeframe,
                exc, result.duration_ms,
            )

        return result
