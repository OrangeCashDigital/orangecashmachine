"""
market_data/processing/strategies/repair.py
=======================================

Strategy de reparación de huecos temporales en Silver.
"""

from __future__ import annotations

import asyncio
import time
from typing import Optional

import pandas as pd
from core.logging import bind_pipeline
from market_data.processing.strategies.base import (
    PairResult,
    PipelineContext,
    PipelineMode,
    StrategyMixin,
)
from market_data.processing.utils.timeframe import timeframe_to_ms
from market_data.adapters.exchange.exchange_quirks import get_quirks
from market_data.observability.metrics import (
    ROWS_INGESTED, PIPELINE_ERRORS,
    REPAIR_GAPS_FOUND, REPAIR_GAPS_HEALED, REPAIR_GAPS_SKIPPED,
)

from market_data.processing.utils.gap_utils import GapRange, scan_gaps  # noqa: F401 — re-export

_log = bind_pipeline("repair")



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

            async def _heal_with_sem(gap: GapRange) -> tuple[bool, int, float]:
                if gap.expected > _MAX_HEALABLE_GAP_CANDLES:
                    log.warning("Gap demasiado grande — skip",
                        expected=gap.expected, max=_MAX_HEALABLE_GAP_CANDLES,
                    )
                    REPAIR_GAPS_SKIPPED.labels(
                        exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe,
                    ).inc()
                    return False, 0, 0.0
                # Filtrar gaps irrecuperables (NoDataAvailableError previo).
                # SafeOps: is_irrecoverable retorna False ante fallo Redis —
                # el gap se reintentará, nunca se omite un gap sano por error.
                try:
                    from infra.state.factories import build_gap_registry
                    _reg = build_gap_registry()
                    if _reg is not None and _reg.is_irrecoverable(
                        exchange=ctx.exchange_id, symbol=symbol,
                        timeframe=timeframe, start_ms=gap.start_ms,
                    ):
                        log.debug(
                            "Gap skip — conocido irrecuperable (sin datos en exchange)",
                            gap=str(gap),
                        )
                        REPAIR_GAPS_SKIPPED.labels(
                            exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe,
                        ).inc()
                        return False, 0, 0.0
                except Exception as _irr_exc:
                    log.debug("is_irrecoverable check skipped", error=str(_irr_exc))
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
                    _log.bind(
                        exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe,
                    ).error("Gap heal: excepción inesperada en gather",
                        error_type=type(_res).__name__, error=str(_res),
                    )
                    PIPELINE_ERRORS.labels(
                        exchange=ctx.exchange_id, error_type="fatal",
                    ).inc()
                    continue
                healed, rows, fill_ratio = _res
                if healed:
                    total_healed_rows += rows
                    if fill_ratio >= 0.95:
                        healed_count += 1
                        REPAIR_GAPS_HEALED.labels(
                            exchange=ctx.exchange_id, symbol=symbol, timeframe=timeframe,
                        ).inc()
                    else:
                        partial_count += 1
                        log.warning(
                            "Gap parcialmente sanado",
                            rows=rows, fill_ratio=round(fill_ratio, 3),
                        )

            result.gaps_healed = healed_count
            result.rows        = total_healed_rows
            result.duration_ms = int((time.monotonic() - pair_start) * 1000)

            result.gaps_partial = partial_count
            log.info("Repair completado",
                idx=idx, total=total,
                gaps_found=result.gaps_found, gaps_healed=result.gaps_healed,
                gaps_partial=partial_count,
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
        ctx:          PipelineContext,
        symbol:       str,
        timeframe:    str,
        columns_only: list | None = None,
    ) -> Optional[pd.DataFrame]:
        """Lee datos Silver delegando al Protocol (parquet o Iceberg).

        columns_only se ignora — load_ohlcv retorna el dataset completo.
        Ambas implementaciones aplican sort + dedup internamente.
        """
        try:
            df = ctx.storage.load_ohlcv(symbol=symbol, timeframe=timeframe)
            if df is None or df.empty:
                return None
            if columns_only:
                cols = [c for c in columns_only if c in df.columns]
                df = df[cols]
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

            _CHUNK = 1000
            collected_raw: list = []
            _MAX_GAP_CHUNKS = 200  # techo de seguridad: 200k velas por gap

            # La estrategia de paginación depende del exchange:
            # - backward (ej. KuCoin): decrementar end_ms en cada chunk.
            # - forward (ej. Bybit): incrementar since en cada chunk.
            _quirks = get_quirks(ctx.exchange_id)

            if _quirks.backward_pagination:
                # Backward pagination: empezar desde gap.end_ms y retroceder.
                # KuCoin ignora since cuando hay endAt — NO pasar since (None)
                # para evitar que el adapter entre en la rama incorrecta.
                current_end_ms = gap.end_ms + tf_ms
                for _chunk_idx in range(_MAX_GAP_CHUNKS):
                    raw_chunk = await ctx.fetcher.fetch_chunk(
                        symbol    = symbol,
                        timeframe = timeframe,
                        since     = None,  # KuCoin ignora since con endAt; omitir
                        limit     = _CHUNK,
                        end_ms    = current_end_ms,
                    )
                    if not raw_chunk:
                        break
                    collected_raw.extend(raw_chunk)
                    first_ts_chunk = raw_chunk[0][0]   # timestamp del primer item
                    _              = raw_chunk[-1][0]  # last_ts_chunk: calculado, no usado
                    # Filtrar ya aquí lo que está fuera del gap para no acumular basura
                    if first_ts_chunk <= gap.start_ms:
                        break  # ya cubrimos el inicio del gap
                    if len(raw_chunk) < _CHUNK:
                        break  # el exchange no tiene más datos hacia atrás
                    # Retroceder end_ms al primer timestamp del chunk actual
                    current_end_ms = first_ts_chunk
            else:
                # Forward pagination estándar para Bybit y otros exchanges
                since = gap.start_ms - tf_ms
                for _chunk_idx in range(_MAX_GAP_CHUNKS):
                    raw_chunk = await ctx.fetcher.fetch_chunk(
                        symbol    = symbol,
                        timeframe = timeframe,
                        since     = since,
                        limit     = _CHUNK,
                        end_ms    = gap.end_ms + tf_ms,
                    )
                    if not raw_chunk:
                        break
                    collected_raw.extend(raw_chunk)
                    last_ts = raw_chunk[-1][0]
                    if last_ts >= gap.end_ms or len(raw_chunk) < _CHUNK:
                        break
                    since = last_ts

            if not collected_raw:
                # El exchange no tiene datos para este rango — no es un fallo de red.
                # Distinguir de ChunkFetchError para no contaminar métricas de error.
                from market_data.processing.exceptions import NoDataAvailableError
                log.warning("Gap heal: exchange sin datos para el rango", gap=str(gap))
                raise NoDataAvailableError(
                    f"Exchange returned no data for gap {gap} "
                    f"(symbol={symbol}, timeframe={timeframe})"
                )

            df = pd.DataFrame(
                collected_raw,
                columns=["timestamp", "open", "high", "low", "close", "volume"],
            )
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
            df = df[(df["timestamp"] >= gap_start) & (df["timestamp"] <= gap_end)]
            df = df.drop_duplicates(subset="timestamp", keep="last").sort_values("timestamp")

            if df.empty:
                _log.bind(symbol=symbol, timeframe=timeframe).warning(
                    "Gap heal: df vacio tras filtro — marcando irrecuperable",
                    gap_start=str(gap_start), gap_end=str(gap_end),
                    collected_raw=len(collected_raw),
                )
                from market_data.processing.exceptions import NoDataAvailableError
                raise NoDataAvailableError(
                    f"Exchange returned data but none within gap window {gap} "
                    f"(symbol={symbol}, timeframe={timeframe})"
                )

            if gap.expected > 1000:
                log.info("Gap heal: gap grande paginado",
                    expected=gap.expected, fetched=len(df), chunks=_chunk_idx + 1,
                )

            qres = ctx.quality.run(
                df=df, symbol=symbol, timeframe=timeframe, exchange=ctx.exchange_id,
            )

            if not qres.accepted:
                log.warning("Gap heal rechazado por calidad", score=round(qres.score, 1))
                return False, 0, 0.0

            ctx.storage.save_ohlcv(df=qres.df, symbol=symbol, timeframe=timeframe)

            fill_ratio = len(qres.df) / gap.expected if gap.expected > 0 else 1.0
            heal_type  = "FULL" if fill_ratio >= 0.95 else "PARTIAL"
            log.info(
                "Gap healed",
                rows=len(qres.df), expected=gap.expected,
                fill_ratio=round(fill_ratio, 3), heal_type=heal_type,
            )
            # Notificar al GapRegistry que este gap fue sanado.
            # SafeOps: si Redis no está disponible, degrada silenciosamente.
            try:
                from infra.state.factories import build_gap_registry
                _registry = build_gap_registry()
                if _registry is not None:
                    _registry.mark_healed(
                        exchange  = ctx.exchange_id,
                        symbol    = symbol,
                        timeframe = timeframe,
                        start_ms  = gap.start_ms,
                    )
            except Exception as _reg_exc:
                log.debug("GapRegistry.mark_healed skipped", error=str(_reg_exc))

            return True, len(qres.df), fill_ratio

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            from market_data.processing.exceptions import (
                ChunkFetchError, NoDataAvailableError,
            )
            from market_data.processing.strategies.base import classify_error
            if isinstance(exc, NoDataAvailableError):
                # No es un fallo — el exchange no tiene datos para este rango
                # (p.ej. gap pre-origin: el exchange no existía aún).
                # Marcar como irrecuperable para suprimir retries infinitos
                # que saturarían workers y contaminarían métricas de error.
                # SafeOps: si Redis no está disponible, degrada silenciosamente.
                log.warning(
                    "Gap heal: sin datos en exchange — marcando irrecuperable",
                    gap=str(gap),
                )
                try:
                    from infra.state.factories import build_gap_registry
                    _registry = build_gap_registry()
                    if _registry is not None:
                        _registry.mark_healed(
                            exchange     = ctx.exchange_id,
                            symbol       = symbol,
                            timeframe    = timeframe,
                            start_ms     = gap.start_ms,
                            irreversible = True,
                        )
                except Exception as _reg_exc:
                    log.debug(
                        "GapRegistry.mark_healed(irreversible) skipped",
                        error=str(_reg_exc),
                    )
            elif isinstance(exc, ChunkFetchError):
                log.warning("Gap heal: fetch transitorio",
                    error=str(exc), is_transient=True)
                PIPELINE_ERRORS.labels(
                    exchange=ctx.exchange_id, error_type="transient"
                ).inc()
            else:
                log.error("Gap heal: error inesperado",
                    error_type=type(exc).__name__,
                    error=str(exc),
                    is_transient=classify_error(exc),
                )
                PIPELINE_ERRORS.labels(
                    exchange=ctx.exchange_id,
                    error_type="transient" if classify_error(exc) else "fatal",
                ).inc()
            return False, 0, 0.0
