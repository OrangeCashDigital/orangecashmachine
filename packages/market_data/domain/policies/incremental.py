"""
market_data/domain/policies/incremental.py
=================================================
Strategy de ingestión incremental hacia adelante.
El boilerplate de timing/errores/métricas vive en StrategyMixin.
"""
from __future__ import annotations

import asyncio
import random

from loguru import logger

from market_data.adapters.outbound.exchange.throttle import get_or_create_throttle
from market_data.domain.policies.base import (
    PairResult,
    PipelineContext,
    PipelineMode,
    StrategyMixin,
)
from market_data.infrastructure.observability.metrics import (
    PAIR_DURATION,
    QUALITY_DECISIONS,
    ROWS_INGESTED,
    PIPELINE_ERRORS,
)
# DRY: helper Kappa es SSOT en backfill.py — reutilizado aquí, no duplicado.
# Principio: un solo lugar define cómo serializar y publicar a ohlcv.raw.
from market_data.domain.policies.backfill import _publish_chunk_to_kafka
from market_data.infrastructure.kafka.payloads import DATASOURCE_LIVE

# Parámetros del retry OCC — extraídos como constantes de módulo
# para facilitar ajuste y legibilidad (DRY, KISS).
_BRONZE_MAX_RETRIES: int   = 5
_BRONZE_BASE_WAIT_S: float = 0.1   # backoff base: 100ms × 2^(intento-1) ± 25% jitter
                                    # máximo ~1.6s en intento 4 → total máximo ~3.1s


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
            df=df, symbol=symbol, timeframe=timeframe, exchange=ctx.exchange_id,
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

        # ── Kappa router: publish → ohlcv.raw | degraded → Bronze+Silver directo
        if ctx.kafka_producer is not None:
            # ── Kappa path: live → ohlcv.raw → KafkaBronzeWriter → Bronze → Silver
            # BronzeWriter consume ohlcv.raw y escribe a Bronze con dedup por event_id.
            # Silver se materializa desde Bronze via Dagster job (medallion).
            # source=DATASOURCE_LIVE distingue live de backfill en los consumers.
            ok = await _publish_chunk_to_kafka(
                ctx       = ctx,
                symbol    = symbol,
                timeframe = timeframe,
                df        = qres.df,
            )
            if not ok:
                # SafeOps: send_async=False → broker caído o topic inexistente.
                # No actualizar cursor — se reintentará en el próximo run.
                logger.warning(
                    "Incremental kafka publish failed — cursor NO actualizado [{}/{}] "
                    "| symbol={} timeframe={}",
                    idx, total, symbol, timeframe,
                )
                PIPELINE_ERRORS.labels(
                    exchange=ctx.exchange_id, error_type="transient",
                ).inc()
                result.skipped = True
                return
        else:
            # ── Degraded path: sin Kafka → Bronze + Silver directo (modo Lambda)
            # Activo cuando KAFKA_BOOTSTRAP_SERVERS no está configurado o el
            # producer no pudo iniciar en OHLCVPipeline._build_kafka_producer_safe().
            # El pipeline no se detiene — SafeOps.
            logger.warning(
                "kafka_producer=None — modo degradado: Bronze+Silver directo [{}/{}] "
                "| symbol={} timeframe={}",
                idx, total, symbol, timeframe,
            )
            run_id = await self._append_bronze_with_retry(
                df=df, symbol=symbol, timeframe=timeframe, ctx=ctx,
            )
            async with ctx.silver_commit_lock:
                ctx.storage.save_ohlcv(
                    df=qres.df, symbol=symbol, timeframe=timeframe, run_id=run_id,
                )

        # ── Cursor update — siempre tras escritura exitosa (Kappa o degradado) ──
        # _normalize_dataframe garantiza timestamp es datetime64[ns, UTC].
        # .timestamp() es válido — sin necesidad de hasattr.
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
        En modo Kappa, Bronze se escribe via KafkaBronzeWriter desde ohlcv.raw.

        Serialización + retry OCC para Bronze:
          - El lock garantiza que solo un worker commitea a la vez — elimina la
            mayoría de conflictos OCC.
          - El retry con backoff exponencial cubre el caso residual: escrituras
            desde otro proceso externo al pipeline.
          - Backoff: base × 2^(intento-1) ± 25% jitter
          - El refresh de self._table dentro de BronzeStorage.append() garantiza
            que cada intento lee el snapshot más reciente antes del commit.

        Integración con throttle:
          - Cada OCC conflict notifica al AdaptiveThrottle del exchange.
          - Si la tasa de OCC supera occ_threshold, el throttle reduce
            la concurrencia suavemente (factor 0.8) en el siguiente ciclo.

        Returns
        -------
        run_id : str
            Identificador del commit Bronze, para correlación con Silver.

        Raises
        ------
        Exception
            Si el error no es OCC, o si se agotan los reintentos OCC.
        """
        throttle = get_or_create_throttle(
            exchange_id = ctx.exchange_id,
            market_type = ctx.market_type,
            dataset     = "ohlcv",
            initial     = 5,
            maximum     = 20,
        )

        last_occ_wait: float = 0.0   # evita UnboundLocalError en el sleep final

        for attempt in range(1, _BRONZE_MAX_RETRIES + 1):
            async with ctx.bronze_commit_lock:
                try:
                    return ctx.bronze.append(
                        df=df, symbol=symbol, timeframe=timeframe,
                    )
                except Exception as exc:
                    msg    = str(exc).lower()
                    is_occ = (
                        "branch main has changed"          in msg
                        or "requirement failed"            in msg
                        or "has been updated by another process" in msg
                    )

                    if not is_occ or attempt >= _BRONZE_MAX_RETRIES:
                        raise   # error no-OCC o reintentos agotados

                    # OCC confirmado — notificar al throttle y preparar retry
                    throttle.record_occ_conflict()

                    last_occ_wait = _BRONZE_BASE_WAIT_S * (2 ** (attempt - 1))
                    last_occ_wait *= 1 + random.uniform(-0.25, 0.25)

                    logger.warning(
                        "Bronze OCC conflict — retry {}/{} | {}/{} wait={:.0f}ms",
                        attempt, _BRONZE_MAX_RETRIES,
                        symbol, timeframe,
                        last_occ_wait * 1000,
                    )
                # Lock liberado al salir del bloque async with

            # Sleep fuera del lock — permite que otros workers avancen
            # mientras esperamos antes del siguiente intento.
            await asyncio.sleep(last_occ_wait)

        # Rama inalcanzable — el raise dentro del loop siempre propaga
        # si se agotan los reintentos. Explícita para satisfacer type checkers.
        raise RuntimeError(
            f"Bronze OCC: reintentos agotados para {symbol}/{timeframe}",
        )
