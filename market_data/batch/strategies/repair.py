"""
market_data/batch/strategies/repair.py
=======================================

Strategy de reparación de huecos temporales en Silver.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import List, Optional

import pandas as pd
from loguru import logger

from market_data.batch.strategies.base import (
    PairResult,
    PipelineContext,
    PipelineMode,
    StrategyMixin,
)
from market_data.batch.fetchers.fetcher import _timeframe_to_ms
from services.observability.metrics import ROWS_INGESTED, PIPELINE_ERRORS


@dataclass(frozen=True)
class GapRange:
    start_ms: int
    end_ms:   int
    expected: int

    def __str__(self) -> str:
        start = pd.Timestamp(self.start_ms, unit="ms", tz="UTC").isoformat()
        end   = pd.Timestamp(self.end_ms,   unit="ms", tz="UTC").isoformat()
        return f"Gap[{start} → {end} expected={self.expected}]"


def scan_gaps(df: pd.DataFrame, timeframe: str, tolerance: int = 0) -> List[GapRange]:
    """
    Detecta huecos temporales en un DataFrame OHLCV ordenado.
    Retorna lista de GapRange. Lista vacía = sin huecos.
    """
    if df is None or df.empty or len(df) < 2:
        return []

    tf_ms     = _timeframe_to_ms(timeframe)
    threshold = tf_ms * (tolerance + 2)

    df_sorted = df.sort_values("timestamp").reset_index(drop=True)
    # Vectorizado: evita .apply(lambda) en loops críticos con millones de filas
    ts_col = df_sorted["timestamp"]
    if hasattr(ts_col.dtype, "tz"):
        ts_ms = (ts_col.astype("int64") // 1_000_000).values
    else:
        ts_ms = ts_col.astype("int64").values

    gaps: List[GapRange] = []
    for i in range(1, len(ts_ms)):
        delta = ts_ms[i] - ts_ms[i - 1]
        if delta >= threshold:
            expected = int(delta // tf_ms) - 1
            gaps.append(GapRange(
                start_ms = ts_ms[i - 1] + tf_ms,
                end_ms   = ts_ms[i]     - tf_ms,
                expected = expected,
            ))

    if gaps:
        logger.debug(
            "Gaps detected | timeframe={} total_gaps={} missing_candles={}",
            timeframe, len(gaps), sum(g.expected for g in gaps),
        )

    return gaps


class RepairStrategy(StrategyMixin):
    _mode = PipelineMode.REPAIR

    def __init__(self, gap_tolerance: int = 0) -> None:
        self._tolerance = gap_tolerance

    async def execute_pair(
        self,
        symbol:    str,
        timeframe: str,
        idx:       int,
        total:     int,
        ctx:       PipelineContext,
    ) -> PairResult:

        result     = PairResult(symbol=symbol, timeframe=timeframe, mode=PipelineMode.REPAIR)
        pair_start = time.monotonic()

        try:
            logger.debug(
                "Repair scan [{}/{}] | exchange={} symbol={} timeframe={}",
                idx, total, ctx.exchange_id, symbol, timeframe,
            )

            df_existing = self._read_silver(ctx, symbol, timeframe, columns_only=["timestamp"])

            if df_existing is None or df_existing.empty:
                result.skipped     = True
                result.duration_ms = int((time.monotonic() - pair_start) * 1000)
                logger.debug(
                    "Repair skip — sin datos en Silver | symbol={} timeframe={}",
                    symbol, timeframe,
                )
                return result

            gaps = scan_gaps(df_existing, timeframe, tolerance=self._tolerance)
            result.gaps_found = len(gaps)

            if not gaps:
                result.skipped     = True
                result.duration_ms = int((time.monotonic() - pair_start) * 1000)
                logger.debug(
                    "Repair OK — sin huecos | symbol={} timeframe={} rows={}",
                    symbol, timeframe, len(df_existing),
                )
                return result

            logger.info(
                "Repair [{}/{}] | exchange={} symbol={} timeframe={} "
                "gaps={} missing_candles={}",
                idx, total, ctx.exchange_id, symbol, timeframe,
                len(gaps), sum(g.expected for g in gaps),
            )

            total_healed_rows = 0
            healed_count      = 0

            for gap in gaps:
                healed, rows = await self._heal_gap(
                    gap=gap, symbol=symbol, timeframe=timeframe, ctx=ctx,
                )
                if healed:
                    healed_count      += 1
                    total_healed_rows += rows

            result.gaps_healed = healed_count
            result.rows        = total_healed_rows
            result.duration_ms = int((time.monotonic() - pair_start) * 1000)

            logger.info(
                "Repair completado [{}/{}] | exchange={} symbol={} timeframe={} "
                "gaps_found={} gaps_healed={} rows={} duration={}ms",
                idx, total, ctx.exchange_id, symbol, timeframe,
                result.gaps_found, result.gaps_healed,
                result.rows, result.duration_ms,
            )

            if total_healed_rows > 0:
                ROWS_INGESTED.labels(
                    exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe,
                ).inc(total_healed_rows)

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            result.error       = str(exc)
            result.duration_ms = int((time.monotonic() - pair_start) * 1000)
            error_type = "transient" if result.is_transient_error else "fatal"
            PIPELINE_ERRORS.labels(exchange=ctx.exchange_id, error_type=error_type).inc()
            logger.error(
                "Repair fallido [{}/{}] | exchange={} symbol={} timeframe={} "
                "error={} duration={}ms",
                idx, total, ctx.exchange_id, symbol, timeframe,
                exc, result.duration_ms,
            )

        return result

    def _read_silver(
        self,
        ctx:       PipelineContext,
        symbol:    str,
        timeframe: str,
        columns_only: Optional[list] = None,
    ) -> Optional[pd.DataFrame]:
        """Lee particiones Silver.

        Parameters
        ----------
        columns_only : list, optional
            Si se especifica, lee solo esas columnas (útil para scan de gaps
            con datasets grandes — ej. 2.5M filas de 1m).
        """
        try:
            files = ctx.storage.find_partition_files(symbol, timeframe)
            if not files:
                return None

            parts = []
            for f in files:
                try:
                    df = pd.read_parquet(f, columns=columns_only)
                    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
                    parts.append(df)
                except Exception as exc:
                    logger.warning("Repair: parquet read failed | file={} error={}", f, exc)

            if not parts:
                return None

            return (
                pd.concat(parts, ignore_index=True)
                .sort_values("timestamp")
                .drop_duplicates(subset="timestamp", keep="last")
                .reset_index(drop=True)
            )

        except Exception as exc:
            logger.warning(
                "Repair: silver read failed | symbol={} timeframe={} error={}",
                symbol, timeframe, exc,
            )
            return None

    async def _heal_gap(
        self,
        gap:       GapRange,
        symbol:    str,
        timeframe: str,
        ctx:       PipelineContext,
    ) -> tuple[bool, int]:
        try:
            logger.debug("Healing gap | symbol={} timeframe={} {}", symbol, timeframe, gap)

            # Fetch quirúrgico: solo las velas del gap, sin traer historial completo.
            # limit = expected + 2 de margen (overlap mínimo).
            tf_ms  = _timeframe_to_ms(timeframe)
            limit  = min(gap.expected + 2, 1000)
            raw    = await ctx.fetcher.fetch_chunk(
                symbol    = symbol,
                timeframe = timeframe,
                since     = gap.start_ms - tf_ms,  # un tick antes para overlap
                limit     = limit,
            )

            if not raw:
                logger.warning(
                    "Gap heal: no data | symbol={} timeframe={} {}", symbol, timeframe, gap,
                )
                return False, 0

            df = pd.DataFrame(raw, columns=["timestamp","open","high","low","close","volume"])
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)

            gap_start = pd.Timestamp(gap.start_ms, unit="ms", tz="UTC")
            gap_end   = pd.Timestamp(gap.end_ms,   unit="ms", tz="UTC")
            df        = df[(df["timestamp"] >= gap_start) & (df["timestamp"] <= gap_end)]

            if df.empty:
                return False, 0

            qres = ctx.quality.run(
                df=df, symbol=symbol, timeframe=timeframe, exchange=ctx.exchange_id,
            )

            if not qres.accepted:
                logger.warning(
                    "Gap heal rechazado por calidad | symbol={} timeframe={} score={:.1f}",
                    symbol, timeframe, qres.score,
                )
                return False, 0

            ctx.storage.save_ohlcv(df=qres.df, symbol=symbol, timeframe=timeframe)

            logger.info(
                "Gap healed | symbol={} timeframe={} {} rows={}",
                symbol, timeframe, gap, len(qres.df),
            )
            return True, len(qres.df)

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            from market_data.batch.fetchers.fetcher import ChunkFetchError
            if isinstance(exc, ChunkFetchError):
                logger.warning(
                    "Gap heal: fetch transitorio | symbol={} timeframe={} {} error={}",
                    symbol, timeframe, gap, exc,
                )
            else:
                logger.error(
                    "Gap heal: error inesperado | symbol={} timeframe={} {} "
                    "error_type={} error={}",
                    symbol, timeframe, gap, type(exc).__name__, exc,
                )
            return False, 0
