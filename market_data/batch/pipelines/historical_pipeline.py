"""
market_data/batch/flows/historical_pipeline.py
===============================================

Pipeline profesional de ingestión OHLCV histórico.

Responsabilidades
-----------------
• Orquestar descarga histórica incremental por par (símbolo × timeframe)
• Controlar backpressure mediante semáforo acotado
• Persistir datos validados en el Data Lake
• Emitir métricas de observabilidad (throughput, latencia, progreso)
• Garantizar shutdown limpio ante cancelación o error

Principios aplicados
--------------------
SOLID   – SRP: cada clase tiene una única responsabilidad
DRY     – lógica de concurrencia y logging centralizada
KISS    – sin abstracciones innecesarias
SafeOps – fallos aislados por par, cierre seguro de recursos,
          backpressure explícito, cancelación limpia
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from loguru import logger

from market_data.batch.fetchers.fetcher import HistoricalFetcherAsync
from market_data.batch.schemas.timeframe import timeframe_to_ms
from services.state.cursor_store import CursorStore, InMemoryCursorStore, build_cursor_store_from_config
from market_data.batch.storage.bronze_storage import BronzeStorage
from market_data.batch.storage.silver_storage import SilverStorage
from market_data.batch.transformers.transformer import OHLCVTransformer
from market_data.batch.pipelines.quality_pipeline import QualityPipeline
from services.exchange.base import ExchangeAdapter
from services.observability.metrics import record_pipeline_pair_metrics


# ==========================================================
# Constantes
# ==========================================================

DEFAULT_MAX_CONCURRENCY: int  = 6

_TRANSIENT_ERRORS: tuple = (TimeoutError, ConnectionError, OSError)


# ==========================================================
# Tracking de resultados
# ==========================================================

@dataclass
class PairResult:
    symbol:      str
    timeframe:   str
    rows:        int  = 0
    skipped:     bool = False
    error:       str  = ""
    duration_ms: int  = 0

    @property
    def success(self) -> bool:
        return not self.error and not self.skipped

    @property
    def is_transient_error(self) -> bool:
        return any(t.__name__ in self.error for t in _TRANSIENT_ERRORS)

    def __str__(self) -> str:
        if self.error:
            tag = "TRANSIENT" if self.is_transient_error else "FATAL"
            return f"{self.symbol}/{self.timeframe} ERROR[{tag}]: {self.error}"
        if self.skipped:
            return f"{self.symbol}/{self.timeframe} SKIPPED (sin datos nuevos)"
        return f"{self.symbol}/{self.timeframe} OK rows={self.rows} duration={self.duration_ms}ms"


@dataclass
class PipelineSummary:
    results:     List[PairResult] = field(default_factory=list)
    duration_ms: int              = 0

    @property
    def total(self)     -> int: return len(self.results)
    @property
    def succeeded(self) -> int: return sum(1 for r in self.results if r.success)
    @property
    def skipped(self)   -> int: return sum(1 for r in self.results if r.skipped)
    @property
    def failed(self)    -> int: return sum(1 for r in self.results if r.error)
    @property
    def total_rows(self) -> int: return sum(r.rows for r in self.results)

    @property
    def throughput_rows_per_sec(self) -> float:
        if not self.duration_ms:
            return 0.0
        return round(self.total_rows / (self.duration_ms / 1000), 2)

    def log(self) -> None:
        logger.info(
            "Pipeline summary | total={} ok={} skipped={} failed={} "
            "rows={} throughput={} rows/s duration={}ms",
            self.total, self.succeeded, self.skipped, self.failed,
            self.total_rows, self.throughput_rows_per_sec, self.duration_ms,
        )
        for r in self.results:
            if r.error:
                logger.warning("  ✗ {}", r)
            elif r.skipped:
                logger.debug("  ↷ {}", r)
            else:
                logger.debug("  ✓ {}", r)


# ==========================================================
# Pipeline principal
# ==========================================================

def _build_cursor_store_safe() -> CursorStore:
    """
    Construye CursorStore desde config centralizada.
    SafeOps: si Redis no está disponible, retorna InMemoryCursorStore.
    """
    try:
        store = build_cursor_store_from_config()
        if store.is_healthy():
            return store
        logger.warning("Redis no disponible — cursor store en memoria (fallback)")
        return InMemoryCursorStore()
    except Exception as exc:
        logger.warning("CursorStore init failed (fallback) | error={}", exc)
        return InMemoryCursorStore()


class HistoricalPipelineAsync:
    """
    Pipeline asíncrono de ingestión OHLCV histórica.

    Arquitectura de concurrencia
    ----------------------------
    • Semáforo acotado controla backpressure (max_concurrency activas).
    • asyncio.gather con return_exceptions=False propaga CancelledError
      limpiamente sin suprimir cancelaciones del sistema.

    Cursor store
    ------------
    get() y update() son async — siempre se usa await.
    El cursor se actualiza DESPUÉS de persistir en Silver (consistencia).
    Si Redis falla, SafeOps garantiza que no interrumpe el pipeline.
    """

    def __init__(
        self,
        symbols:           List[str],
        timeframes:        List[str],
        start_date:        str,
        max_concurrency:   int                   = DEFAULT_MAX_CONCURRENCY,
        exchange_client:   Optional[ExchangeAdapter] = None,
        cursor_store:      Optional[CursorStore] = None,
        fetch_all_history: bool                  = False,
        market_type:       str                   = "spot",
        backfill_mode:     bool                  = False,
    ) -> None:
        _validate_inputs(symbols, timeframes, start_date)

        if exchange_client is None:
            raise ValueError(
                "exchange_client es obligatorio. "
                "HistoricalPipelineAsync no decide qué exchange usar — "
                "esa responsabilidad pertenece al orchestrator (DIP)."
            )

        self.symbols         = symbols
        self.timeframes      = timeframes
        self.start_date      = _parse_start_date(start_date)
        self.max_concurrency = max_concurrency
        self.market_type     = market_type.lower()

        self._exchange_id    = getattr(exchange_client, "_exchange_id", "unknown")
        self._bronze_storage = BronzeStorage(exchange=self._exchange_id)
        self._silver_storage = SilverStorage(exchange=self._exchange_id, market_type=self.market_type)
        self._semaphore      = asyncio.Semaphore(max_concurrency)
        self._cursor: CursorStore = cursor_store or _build_cursor_store_safe()
        self._quality_pipeline    = QualityPipeline()

        self._fetcher = HistoricalFetcherAsync(
            storage           = self._silver_storage,
            transformer       = OHLCVTransformer(),
            exchange_client   = exchange_client,
            cursor_store      = self._cursor,
            market_type       = market_type,
            config_start_date = start_date,
            backfill_mode     = backfill_mode,
        )

    # ----------------------------------------------------------
    # API pública
    # ----------------------------------------------------------

    async def run(self) -> PipelineSummary:
        pairs       = [(s, tf) for s in self.symbols for tf in self.timeframes]
        total_pairs = len(pairs)

        await self._fetcher.ensure_exchange()

        pipeline_logger = logger.bind(exchange=self._exchange_id, dataset="ohlcv")
        pipeline_logger.info(
            "Pipeline iniciando | exchange={} market={} símbolos={} timeframes={} pares={} concurrencia_max={}",
            self._exchange_id, self.market_type,
            len(self.symbols), len(self.timeframes), total_pairs, self.max_concurrency,
        )

        pipeline_start = time.monotonic()

        try:
            results: List[PairResult] = await asyncio.gather(
                *[
                    self._process_pair(symbol, tf, idx, total_pairs)
                    for idx, (symbol, tf) in enumerate(pairs, 1)
                ],
                return_exceptions=False,
            )
        except asyncio.CancelledError:
            logger.warning("Pipeline cancelado externamente — cerrando recursos...")
            raise

        duration_ms = int((time.monotonic() - pipeline_start) * 1000)
        summary     = PipelineSummary(results=list(results), duration_ms=duration_ms)
        summary.log()

        if summary.failed:
            logger.warning(
                "Pipeline con errores | fallidos={}/{} transient={}",
                summary.failed, summary.total,
                sum(1 for r in summary.results if r.is_transient_error),
            )
        else:
            logger.success(
                "Pipeline completado | rows={} pares={} throughput={} rows/s duration={}ms",
                summary.total_rows, summary.total,
                summary.throughput_rows_per_sec, duration_ms,
            )

        return summary

    # ----------------------------------------------------------
    # Procesamiento de un par (privado)
    # ----------------------------------------------------------

    async def _process_pair(
        self,
        symbol:    str,
        timeframe: str,
        idx:       int,
        total:     int,
    ) -> PairResult:
        result     = PairResult(symbol=symbol, timeframe=timeframe)
        pair_start = time.monotonic()

        async with self._semaphore:
            try:
                logger.debug(
                    "Descargando [{}/{}] | symbol={} timeframe={}",
                    idx, total, symbol, timeframe,
                )

                df = await self._fetcher.download_data(
                    symbol     = symbol,
                    timeframe  = timeframe,
                    start_date = str(self.start_date.date()),
                )

                if df is None or df.empty:
                    result.skipped     = True
                    result.duration_ms = int((time.monotonic() - pair_start) * 1000)
                    logger.debug(
                        "Sin datos nuevos [{}/{}] | symbol={} timeframe={}",
                        idx, total, symbol, timeframe,
                    )
                    return result

                run_id = self._bronze_storage.append(
                    df        = df,
                    symbol    = symbol,
                    timeframe = timeframe,
                )

                qres = self._quality_pipeline.run(
                    df        = df,
                    symbol    = symbol,
                    timeframe = timeframe,
                    exchange  = self._exchange_id,
                )

                record_pipeline_pair_metrics(
                    exchange         = self._exchange_id,
                    symbol           = symbol,
                    timeframe        = timeframe,
                    market_type      = self.market_type,
                    rows             = 0,
                    duration_ms      = 0,
                    quality_decision = qres.tier.value,
                )

                if not qres.accepted:
                    logger.warning(
                        "Par rechazado por calidad [{}/{}] | exchange={} symbol={} timeframe={} score={:.1f}",
                        idx, total, self._exchange_id, symbol, timeframe, qres.score,
                    )
                    result.skipped     = True
                    result.duration_ms = int((time.monotonic() - pair_start) * 1000)
                    return result

                self._silver_storage.save_ohlcv(
                    df        = qres.df,
                    symbol    = symbol,
                    timeframe = timeframe,
                    run_id    = run_id,
                )

                # Cursor: await obligatorio — update es async.
                # Se actualiza DESPUÉS de persistir en Silver (consistencia garantizada).
                # Si Redis falla, SafeOps en cursor_store.update absorbe el error.
                if not df.empty:
                    tf_ms      = timeframe_to_ms(timeframe)
                    last_ts_ms = (
                        int(df["timestamp"].max().timestamp() * 1000)
                        if hasattr(df["timestamp"].max(), "timestamp")
                        else int(df["timestamp"].max())
                    )
                    await self._cursor.update(
                        self._exchange_id, symbol, timeframe, last_ts_ms + tf_ms
                    )

                result.rows        = len(df)
                result.duration_ms = int((time.monotonic() - pair_start) * 1000)

                record_pipeline_pair_metrics(
                    exchange    = self._exchange_id,
                    symbol      = symbol,
                    timeframe   = timeframe,
                    market_type = self.market_type,
                    rows        = result.rows,
                    duration_ms = result.duration_ms,
                )

                logger.info(
                    "Par completado [{}/{}] | exchange={} market={} symbol={} timeframe={} rows={} duration={}ms",
                    idx, total, self._exchange_id, self.market_type,
                    symbol, timeframe, result.rows, result.duration_ms,
                )

            except asyncio.CancelledError:
                raise

            except Exception as exc:
                result.error       = str(exc)
                result.duration_ms = int((time.monotonic() - pair_start) * 1000)
                error_type = "transient" if result.is_transient_error else "fatal"
                record_pipeline_pair_metrics(
                    exchange    = self._exchange_id,
                    symbol      = symbol,
                    timeframe   = timeframe,
                    market_type = self.market_type,
                    rows        = 0,
                    duration_ms = result.duration_ms,
                    error_type  = error_type,
                )
                logger.error(
                    "Par fallido [{}/{}] | exchange={} market={} symbol={} timeframe={} error={} duration={}ms",
                    idx, total, self._exchange_id, self.market_type,
                    symbol, timeframe, exc, result.duration_ms,
                )

        return result


# ==========================================================
# Validación de inputs
# ==========================================================

def _validate_inputs(symbols: List[str], timeframes: List[str], start_date: str) -> None:
    if not symbols:
        raise ValueError("La lista de símbolos no puede estar vacía.")
    if not timeframes:
        raise ValueError("La lista de timeframes no puede estar vacía.")
    if not start_date or not start_date.strip():
        raise ValueError("start_date es obligatorio y no puede estar vacío.")


def _parse_start_date(start_date: str) -> pd.Timestamp:
    try:
        ts = pd.Timestamp(start_date)
    except Exception:
        raise ValueError(
            f"start_date tiene formato inválido: '{start_date}'. "
            "Se esperaba ISO 8601, ej: '2022-01-01'."
        )
    if ts > pd.Timestamp.now(tz="UTC"):
        raise ValueError(
            f"start_date '{start_date}' está en el futuro. "
            "El pipeline histórico requiere una fecha pasada."
        )
    return ts
