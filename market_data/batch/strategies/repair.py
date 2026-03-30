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
from core.logging.setup import bind_pipeline
from market_data.batch.strategies.base import (
    PairResult,
    PipelineContext,
    PipelineMode,
    StrategyMixin,
)
from market_data.batch.schemas.timeframe import timeframe_to_ms
from services.observability.metrics import (
    ROWS_INGESTED, PIPELINE_ERRORS,
    REPAIR_GAPS_FOUND, REPAIR_GAPS_HEALED, REPAIR_GAPS_SKIPPED,
)

_log = bind_pipeline("repair")


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

    tf_ms     = timeframe_to_ms(timeframe)
    threshold = tf_ms * (tolerance + 2)

    df_sorted = df.sort_values("timestamp").reset_index(drop=True)
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
        _log.bind(timeframe=timeframe).debug(
            "Gaps detected",
            total_gaps=len(gaps),
            missing_candles=sum(g.expected for g in gaps),
        )

    return gaps


# Guardrail: gaps más grandes que esto se logean y se skipean.
# 43200 velas = 30 días en 1m. Gaps más grandes indican exchange sin datos históricos.
_MAX_HEALABLE_GAP_CANDLES: int = 43_200
# Máximo de gaps procesados en paralelo — evita saturar el exchange.
_GAP_CONCURRENCY: int = 4


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
        log        = _log.bind(exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe, mode="repair")

        try:
            log.debug("Repair scan", idx=idx, total=total)

            df_existing = self._read_silver(ctx, symbol, timeframe, columns_only=["timestamp"])

            if df_existing is None or df_existing.empty:
                result.skipped     = True
                result.duration_ms = int((time.monotonic() - pair_start) * 1000)
                log.debug("Repair skip — sin datos en Silver")
                return result

            gaps = scan_gaps(df_existing, timeframe, tolerance=self._tolerance)
            result.gaps_found = len(gaps)
            if gaps:
                REPAIR_GAPS_FOUND.labels(
                    exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe,
                ).inc(len(gaps))

            if not gaps:
                result.skipped     = True
                result.duration_ms = int((time.monotonic() - pair_start) * 1000)
                log.debug("Repair OK — sin huecos", rows=len(df_existing))
                return result

            log.info("Repair iniciando",
                idx=idx, total=total,
                gaps=len(gaps),
                missing_candles=sum(g.expected for g in gaps),
            )

            total_healed_rows = 0
            healed_count      = 0
            sem = asyncio.Semaphore(_GAP_CONCURRENCY)

            async def _heal_with_sem(gap: GapRange) -> tuple[bool, int]:
                if gap.expected > _MAX_HEALABLE_GAP_CANDLES:
                    log.warning("Gap demasiado grande — skip",
                        expected=gap.expected, max=_MAX_HEALABLE_GAP_CANDLES,
                    )
                    REPAIR_GAPS_SKIPPED.labels(
                        exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe,
                    ).inc()
                    return False, 0
                async with sem:
                    return await self._heal_gap(
                        gap=gap, symbol=symbol, timeframe=timeframe, ctx=ctx,
                    )

            heal_results = await asyncio.gather(
                *[_heal_with_sem(gap) for gap in gaps],
                return_exceptions=False,
            )
            for healed, rows in heal_results:
                if healed:
                    healed_count      += 1
                    total_healed_rows += rows
                    REPAIR_GAPS_HEALED.labels(
                        exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe,
                    ).inc()

            result.gaps_healed = healed_count
            result.rows        = total_healed_rows
            result.duration_ms = int((time.monotonic() - pair_start) * 1000)

            log.info("Repair completado",
                idx=idx, total=total,
                gaps_found=result.gaps_found, gaps_healed=result.gaps_healed,
                rows=result.rows, duration_ms=result.duration_ms,
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
            log.error("Repair fallido",
                idx=idx, total=total,
                error=str(exc), duration_ms=result.duration_ms,
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
                    _log.bind(symbol=symbol, timeframe=timeframe).warning(
                        "Repair: parquet read failed", file=str(f), error=str(exc),
                    )

            if not parts:
                return None

            return (
                pd.concat(parts, ignore_index=True)
                .sort_values("timestamp")
                .drop_duplicates(subset="timestamp", keep="last")
                .reset_index(drop=True)
            )

        except Exception as exc:
            _log.bind(symbol=symbol, timeframe=timeframe).warning(
                "Repair: silver read failed", error=str(exc),
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
            log = _log.bind(
                exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe,
                gap_start=gap.start_ms, gap_end=gap.end_ms, expected=gap.expected,
            )
            log.debug("Healing gap")

            tf_ms     = timeframe_to_ms(timeframe)
            gap_start = pd.Timestamp(gap.start_ms, unit="ms", tz="UTC")
            gap_end   = pd.Timestamp(gap.end_ms,   unit="ms", tz="UTC")

            _CHUNK = 1000
            since  = gap.start_ms - tf_ms  # un tick antes para overlap
            collected_raw: list = []
            _MAX_GAP_CHUNKS = 200  # techo de seguridad: 200k velas por gap

            for _chunk_idx in range(_MAX_GAP_CHUNKS):
                raw_chunk = await ctx.fetcher.fetch_chunk(
                    symbol    = symbol,
                    timeframe = timeframe,
                    since     = since,
                    limit     = _CHUNK,
                )
                if not raw_chunk:
                    break
                collected_raw.extend(raw_chunk)
                last_ts = raw_chunk[-1][0]  # timestamp ms del último item
                if last_ts >= gap.end_ms or len(raw_chunk) < _CHUNK:
                    break  # llegamos al final del gap o el exchange no tiene más
                since = last_ts  # avanzar sin overlap — estamos en rango exacto

            if not collected_raw:
                log.warning("Gap heal: no data", gap=str(gap))
                return False, 0

            df = pd.DataFrame(
                collected_raw,
                columns=["timestamp", "open", "high", "low", "close", "volume"],
            )
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
            df = df[(df["timestamp"] >= gap_start) & (df["timestamp"] <= gap_end)]
            df = df.drop_duplicates(subset="timestamp", keep="last").sort_values("timestamp")

            if df.empty:
                return False, 0

            if gap.expected > 1000:
                log.info("Gap heal: gap grande paginado",
                    expected=gap.expected, fetched=len(df), chunks=_chunk_idx + 1,
                )

            qres = ctx.quality.run(
                df=df, symbol=symbol, timeframe=timeframe, exchange=ctx.exchange_id,
            )

            if not qres.accepted:
                log.warning("Gap heal rechazado por calidad", score=round(qres.score, 1))
                return False, 0

            ctx.storage.save_ohlcv(df=qres.df, symbol=symbol, timeframe=timeframe)

            log.info("Gap healed", rows=len(qres.df))
            return True, len(qres.df)

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            from market_data.batch.fetchers.fetcher import ChunkFetchError
            if isinstance(exc, ChunkFetchError):
                log.warning("Gap heal: fetch transitorio", error=str(exc))
            else:
                log.error("Gap heal: error inesperado", error_type=type(exc).__name__, error=str(exc))
            return False, 0
