"""
market_data/processing/strategies/incremental.py
============================================

Strategy de ingestión incremental hacia adelante.
El boilerplate de timing/errores/métricas vive en StrategyMixin.
"""

from __future__ import annotations

import asyncio
import random

from loguru import logger

from market_data.processing.strategies.base import (
    PairResult,
    PipelineContext,
    PipelineMode,
    StrategyMixin,
)
from market_data.observability.metrics import (
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

        # Serialización + retry OCC para Bronze.
        #
        # El lock garantiza que solo un worker commitea a la vez — elimina la
        # mayoría de conflictos OCC. El retry con backoff exponencial cubre el
        # caso residual: escrituras desde otro proceso externo al pipeline.
        #
        # Backoff: 100ms × 2^(intento-1) ± 25% jitter
        # Máximo: ~1.6s en el intento 4 → total máximo ~3.1s
        # El refresh de self._table dentro de BronzeStorage.append() garantiza
        # que cada intento lee el snapshot más reciente antes del commit.
        _BRONZE_MAX_RETRIES = 5
        _BRONZE_BASE_WAIT_S = 0.1
        for _attempt in range(1, _BRONZE_MAX_RETRIES + 1):
            async with ctx.bronze_commit_lock:
                try:
                    run_id = ctx.bronze.append(df=df, symbol=symbol, timeframe=timeframe)
                    break  # commit exitoso
                except Exception as _exc:
                    _msg = str(_exc).lower()
                    _is_occ = (
                        "branch main has changed" in _msg
                        or "requirement failed" in _msg
                        or "has been updated by another process" in _msg
                    )
                    if _is_occ and _attempt < _BRONZE_MAX_RETRIES:
                        _wait = _BRONZE_BASE_WAIT_S * (2 ** (_attempt - 1))
                        _wait *= 1 + random.uniform(-0.25, 0.25)
                        logger.warning(
                            "Bronze OCC conflict — retry {}/{} | {}/{} wait={:.0f}ms",
                            _attempt, _BRONZE_MAX_RETRIES, symbol, timeframe,
                            _wait * 1000,
                        )
                        # Liberar el lock durante el sleep — permite que otro
                        # worker avance mientras esperamos.
                    else:
                        raise  # error no-OCC o reintentos agotados
            if _attempt < _BRONZE_MAX_RETRIES:
                # Sleep fuera del lock — no bloquea a otros workers.
                await asyncio.sleep(_wait)

        qres = ctx.quality.run(
            df=df, symbol=symbol, timeframe=timeframe, exchange=ctx.exchange_id,
        )

        QUALITY_DECISIONS.labels(
            exchange=ctx.exchange_id, market_type=ctx.market_type,
            symbol=symbol, timeframe=timeframe, decision=qres.tier.value,
        ).inc()

        if not qres.accepted:
            # Bronze ya contiene los datos crudos (append anterior).
            # Silver NO recibirá este lote — trazabilidad explícita con run_id
            # para correlacionar Bronze rechazado con el log de quality.
            logger.warning(
                "Par rechazado por calidad [{}/{}] | exchange={} symbol={} "
                "timeframe={} score={:.1f} bronze_run_id={} "
                "(datos en Bronze, NO escritos en Silver)",
                idx, total, ctx.exchange_id, symbol, timeframe,
                qres.score, run_id,
            )
            result.skipped = True
            return

        # Lock de serialización de commits Silver (tabla distinta → lock distinto).
        # bronze_commit_lock ya fue liberado — no hay riesgo de deadlock.
        async with ctx.silver_commit_lock:
            ctx.storage.save_ohlcv(
                df=qres.df, symbol=symbol, timeframe=timeframe, run_id=run_id,
            )

        # _normalize_dataframe garantiza que timestamp es datetime64[ns, UTC]
        # .timestamp() es siempre válido aquí — sin necesidad de hasattr
        last_ts_ms = int(qres.df["timestamp"].max().timestamp() * 1000)
        await ctx.cursor.update(ctx.exchange_id, symbol, timeframe, last_ts_ms)

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
