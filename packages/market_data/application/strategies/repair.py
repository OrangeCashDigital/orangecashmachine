# -*- coding: utf-8 -*-
"""
market_data/application/strategies/repair.py
=============================================

Strategy de reparación de huecos temporales en Silver.

Orquesta: storage (Silver) · fetcher · quality gate · gap_registry ·
          exchange_quirks · métricas Prometheus.

Principios: SRP · DIP · SafeOps (gaps independientes — fail-soft por gap)
"""
from __future__ import annotations

import asyncio
import time
from typing import Optional

import pandas as pd
from ocm.observability import bind_pipeline

# ── Domain (tipos y contratos) ────────────────────────────────────────────────
from market_data.ports.outbound.metrics import RepairMetricsPort
from market_data.domain.policies.base import (
    PairResult,
    PipelineContext,
    PipelineMode,
    StrategyMixin,
    classify_error,
)
from market_data.domain.exceptions import (
    ChunkFetchError,
    NoDataAvailableError,
)
from market_data.domain.value_objects.timeframe import timeframe_to_ms
from market_data.domain.value_objects.gap_utils     import GapRange, scan_gaps
from market_data.domain.value_objects.exchange_quirks import get_quirks  # domain VO — BC-05

# ── Infrastructure (métricas — import local por BC-05: application aislada de infrastructure) ──
# Los imports concretos ocurren dentro de los métodos que los usan (ver execute_pair, _heal_gap).

# ── Adapters ──────────────────────────────────────────────────────────────────
# get_quirks: import local en execute_pair() — BC-05

# ── Infrastructure ────────────────────────────────────────────────────────────
# métricas repair: import local en execute_pair() — BC-05

_log = bind_pipeline("repair")

# ── Módulo-level constants — SSOT ─────────────────────────────────────────────
# Guardrail: gaps más grandes que esto se logean y se skipean.
# 43200 velas = 30 días en 1m.
_MAX_HEALABLE_GAP_CANDLES: int = 43_200
# Máximo de gaps procesados en paralelo — evita saturar el exchange.
_GAP_CONCURRENCY:          int = 4
# Umbral de cobertura para considerar un gap "completamente sanado".
_FULL_HEAL_THRESHOLD:      float = 0.95
# Tamaño de chunk por llamada al exchange.
_FETCH_CHUNK_SIZE:         int = 1_000
# Techo de seguridad: máximo de chunks paginados por gap.
_MAX_GAP_CHUNKS:           int = 200
# Gap con más de este número de velas esperadas dispara log.info de diagnóstico.
_LARGE_GAP_LOG_THRESHOLD:  int = 1_000


class RepairStrategy(StrategyMixin):
    _mode = PipelineMode.REPAIR

    def __init__(
        self,
        gap_tolerance: int = 0,
        metrics: "RepairMetricsPort | None" = None,
    ) -> None:
        # Fail-fast: metrics es obligatorio — inyectar desde composition root.
        # RepairStrategy no puede importar infrastructure/ (DIP — BC-05).
        if metrics is None:
            raise TypeError(
                "RepairStrategy: 'metrics' es obligatorio. "
                "Inyectar PrometheusRepairMetrics desde el composition root "
                "(market_data.factories.pipeline_factory o OCMContainer)."
            )
        self._tolerance = gap_tolerance
        self._metrics: RepairMetricsPort = metrics

    async def execute_pair(
        self,
        symbol:    str,
        timeframe: str,
        idx:       int,
        total:     int,
        ctx:       PipelineContext,
    ) -> PairResult:

        _m = self._metrics  # RepairMetricsPort — inyectado en __init__
        result     = PairResult(
            symbol=symbol, timeframe=timeframe, mode=PipelineMode.REPAIR,
        )
        pair_start = time.monotonic()
        log        = _log.bind(
            exchange=ctx.exchange_id, symbol=symbol,
            timeframe=timeframe, mode="repair",
        )

        try:
            log.debug("Repair scan", idx=idx, total=total)

            df_existing = self._read_silver(
                ctx, symbol, timeframe, columns_only=["timestamp"],
            )

            if df_existing is None or df_existing.empty:
                result.skipped     = True
                result.duration_ms = int((time.monotonic() - pair_start) * 1000)
                log.debug("Repair skip — sin datos en Silver")
                return result

            gaps              = scan_gaps(df_existing, timeframe, tolerance=self._tolerance)
            result.gaps_found = len(gaps)
            if gaps:
                _m.repair_gaps_found_inc(exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe, count=len(gaps))

            if not gaps:
                result.skipped     = True
                result.duration_ms = int((time.monotonic() - pair_start) * 1000)
                log.debug("Repair OK — sin huecos", rows=len(df_existing))
                return result

            log.info(
                "Repair iniciando",
                idx=idx, total=total,
                gaps=len(gaps),
                missing_candles=sum(g.expected for g in gaps),
            )

            total_healed_rows = 0
            healed_count      = 0
            sem               = asyncio.Semaphore(_GAP_CONCURRENCY)

            async def _heal_with_sem(gap: GapRange) -> tuple[bool, int, float]:
                if gap.expected > _MAX_HEALABLE_GAP_CANDLES:
                    log.warning(
                        "Gap demasiado grande — skip",
                        expected=gap.expected, max=_MAX_HEALABLE_GAP_CANDLES,
                    )
                    _m.repair_gaps_skipped_inc(exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe)
                    return False, 0, 0.0

                try:
                    _reg = ctx.gap_registry
                    if _reg is not None and _reg.is_irrecoverable(
                        exchange=ctx.exchange_id, symbol=symbol,
                        timeframe=timeframe, start_ms=gap.start_ms,
                    ):
                        log.debug(
                            "Gap skip — conocido irrecuperable",
                            gap=str(gap),
                        )
                        _m.repair_gaps_skipped_inc(exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe)
                        return False, 0, 0.0
                except Exception as _irr_exc:
                    log.debug(
                        "is_irrecoverable check skipped", error=str(_irr_exc),
                    )

                async with sem:
                    return await self._heal_gap(
                        gap=gap, symbol=symbol, timeframe=timeframe, ctx=ctx,
                    )

            heal_results = await asyncio.gather(
                *[_heal_with_sem(gap) for gap in gaps],
                return_exceptions=True,
            )
            partial_count = 0
            for _res in heal_results:
                if isinstance(_res, BaseException):
                    if isinstance(_res, asyncio.CancelledError):
                        raise _res
                    _log.bind(
                        exchange=ctx.exchange_id,
                        symbol=symbol, timeframe=timeframe,
                    ).error(
                        "Gap heal: excepción inesperada en gather",
                        error_type=type(_res).__name__, error=str(_res),
                    )
                    ctx.metrics.pipeline_errors_inc(
                        exchange=ctx.exchange_id, error_type="fatal",
                    )
                    continue

                healed, rows, fill_ratio = _res
                if healed:
                    total_healed_rows += rows
                    if fill_ratio >= _FULL_HEAL_THRESHOLD:
                        healed_count += 1
                        _m.repair_gaps_healed_inc(exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe)
                    else:
                        partial_count += 1
                        log.warning(
                            "Gap parcialmente sanado",
                            rows=rows, fill_ratio=round(fill_ratio, 3),
                        )

            result.gaps_healed  = healed_count
            result.rows         = total_healed_rows
            result.duration_ms  = int((time.monotonic() - pair_start) * 1000)
            result.gaps_partial = partial_count

            log.info(
                "Repair completado",
                idx=idx, total=total,
                gaps_found=result.gaps_found,
                gaps_healed=result.gaps_healed,
                gaps_partial=partial_count,
                rows=result.rows, duration_ms=result.duration_ms,
            )

            if total_healed_rows > 0:
                _m.rows_ingested_inc(exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe, count=total_healed_rows)

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            result.error       = str(exc) or type(exc).__name__
            result.duration_ms = int((time.monotonic() - pair_start) * 1000)
            error_type         = "transient" if result.is_transient_error else "fatal"
            ctx.metrics.pipeline_errors_inc(
                exchange=ctx.exchange_id, error_type=error_type,
            )
            log.error(
                "Repair fallido",
                idx=idx, total=total,
                error=str(exc), duration_ms=result.duration_ms,
            )

        return result

    def _read_silver(
        self,
        ctx:          PipelineContext,
        symbol:       str,
        timeframe:    str,
        columns_only: list | None = None,
    ) -> Optional[pd.DataFrame]:
        """Lee datos Silver delegando al Protocol OHLCVStorage."""
        try:
            df = ctx.storage.load_ohlcv(symbol=symbol, timeframe=timeframe)
            if df is None or df.empty:
                return None
            if columns_only:
                cols = [c for c in columns_only if c in df.columns]
                df   = df[cols]
            return df
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
    ) -> tuple[bool, int, float]:
        try:
            log = _log.bind(
                exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe,
                gap_start=gap.start_ms, gap_end=gap.end_ms, expected=gap.expected,
            )
            log.debug("Healing gap")

            tf_ms     = timeframe_to_ms(timeframe)
            gap_start = pd.Timestamp(gap.start_ms, unit="ms", tz="UTC")
            gap_end   = pd.Timestamp(gap.end_ms,   unit="ms", tz="UTC")

            collected_raw: list = []
            _quirks = get_quirks(ctx.exchange_id)

            if _quirks.backward_pagination:
                current_end_ms = gap.end_ms + tf_ms
                for _chunk_idx in range(_MAX_GAP_CHUNKS):
                    raw_chunk = await ctx.fetcher.fetch_chunk(
                        symbol=symbol, timeframe=timeframe,
                        since=None, limit=_FETCH_CHUNK_SIZE,
                        end_ms=current_end_ms,
                    )
                    if not raw_chunk:
                        break
                    collected_raw.extend(raw_chunk)
                    first_ts_chunk = raw_chunk[0][0]
                    if first_ts_chunk <= gap.start_ms:
                        break
                    if len(raw_chunk) < _FETCH_CHUNK_SIZE:
                        break
                    current_end_ms = first_ts_chunk
            else:
                since = gap.start_ms - tf_ms
                for _chunk_idx in range(_MAX_GAP_CHUNKS):
                    raw_chunk = await ctx.fetcher.fetch_chunk(
                        symbol=symbol, timeframe=timeframe,
                        since=since, limit=_FETCH_CHUNK_SIZE,
                        end_ms=gap.end_ms + tf_ms,
                    )
                    if not raw_chunk:
                        break
                    collected_raw.extend(raw_chunk)
                    last_ts = raw_chunk[-1][0]
                    if last_ts >= gap.end_ms or len(raw_chunk) < _FETCH_CHUNK_SIZE:
                        break
                    since = last_ts

            if not collected_raw:
                log.warning(
                    "Gap heal: exchange sin datos para el rango", gap=str(gap),
                )
                raise NoDataAvailableError(
                    f"Exchange returned no data for gap {gap} "
                    f"(symbol={symbol}, timeframe={timeframe})"
                )

            df = pd.DataFrame(
                collected_raw,
                columns=["timestamp", "open", "high", "low", "close", "volume"],
            )
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
            df = df[
                (df["timestamp"] >= gap_start) & (df["timestamp"] <= gap_end)
            ]
            df = (
                df.drop_duplicates(subset="timestamp", keep="last")
                  .sort_values("timestamp")
            )

            if df.empty:
                _log.bind(symbol=symbol, timeframe=timeframe).warning(
                    "Gap heal: df vacio tras filtro — marcando irrecuperable",
                    gap_start=str(gap_start), gap_end=str(gap_end),
                    collected_raw=len(collected_raw),
                )
                raise NoDataAvailableError(
                    f"Exchange returned data but none within gap window {gap} "
                    f"(symbol={symbol}, timeframe={timeframe})"
                )

            if gap.expected > _LARGE_GAP_LOG_THRESHOLD:
                log.info(
                    "Gap heal: gap grande paginado",
                    expected=gap.expected, fetched=len(df),
                    chunks=_chunk_idx + 1,
                )

            qres = ctx.quality.run(
                df=df, symbol=symbol,
                timeframe=timeframe, exchange=ctx.exchange_id,
            )

            if not qres.accepted:
                log.warning(
                    "Gap heal rechazado por calidad", score=round(qres.score, 1),
                )
                return False, 0, 0.0

            ctx.storage.save_ohlcv(
                df=qres.df, symbol=symbol, timeframe=timeframe,
            )

            fill_ratio = len(qres.df) / gap.expected if gap.expected > 0 else 1.0
            heal_type  = "FULL" if fill_ratio >= _FULL_HEAL_THRESHOLD else "PARTIAL"
            log.info(
                "Gap healed",
                rows=len(qres.df), expected=gap.expected,
                fill_ratio=round(fill_ratio, 3), heal_type=heal_type,
            )

            try:
                _registry = ctx.gap_registry
                if _registry is not None:
                    _registry.mark_healed(
                        exchange=ctx.exchange_id, symbol=symbol,
                        timeframe=timeframe, start_ms=gap.start_ms,
                    )
            except Exception as _reg_exc:
                log.debug("GapRegistry.mark_healed skipped", error=str(_reg_exc))

            return True, len(qres.df), fill_ratio

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            if isinstance(exc, NoDataAvailableError):
                log.warning(
                    "Gap heal: sin datos en exchange — marcando irrecuperable",
                    gap=str(gap),
                )
                try:
                    _registry = ctx.gap_registry
                    if _registry is not None:
                        _registry.mark_healed(
                            exchange=ctx.exchange_id, symbol=symbol,
                            timeframe=timeframe, start_ms=gap.start_ms,
                            irreversible=True,
                        )
                except Exception as _reg_exc:
                    log.debug(
                        "GapRegistry.mark_healed(irreversible) skipped",
                        error=str(_reg_exc),
                    )
            elif isinstance(exc, ChunkFetchError):
                log.warning(
                    "Gap heal: fetch transitorio",
                    error=str(exc), is_transient=True,
                )
                self._metrics.pipeline_errors_inc(exchange=ctx.exchange_id, error_type="transient",)
            else:
                _is_transient = classify_error(exc)
                log.error(
                    "Gap heal: error inesperado",
                    error_type=type(exc).__name__,
                    error=str(exc),
                    is_transient=_is_transient,
                )
                self._metrics.pipeline_errors_inc(exchange=ctx.exchange_id, error_type="transient" if _is_transient else "fatal",)
            return False, 0, 0.0
