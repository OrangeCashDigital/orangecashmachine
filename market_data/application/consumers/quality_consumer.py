# -*- coding: utf-8 -*-
"""
market_data/pipeline/consumers/quality_consumer.py
====================================================

Consumer que ejecuta el pipeline de calidad sobre lotes OHLCV.

Responsabilidad
---------------
Suscribirse a OHLCVBatchReceived y ejecutar la cadena:
  DataFrame → DataQualityChecker → AnomalyRegistry → LineageTracker

Principios
----------
SRP     — un consumer, una responsabilidad: calidad de datos
DIP     — recibe registry y tracker por inyección; testeable sin SQLite
Fail-soft — errores en _process() logueados, nunca propagados al bus
"""
from __future__ import annotations

from loguru import logger
import pandas as pd

from market_data.domain.events import (
    LineageEvent,
    LineageStatus,
    PipelineLayer,
)
from market_data.domain.events.ingestion import DomainEvent, OHLCVBatchReceived
from market_data.lineage.tracker import lineage_tracker as _default_tracker
from market_data.application.consumers.base import BaseConsumer
from market_data.ports.event_bus import EventBusPort
from market_data.quality.anomaly_registry import default_registry
from market_data.quality.validators.data_quality import DataQualityChecker


# Columnas del DataFrame OHLCV — SSOT con OHLCVBatch.candles
_CANDLE_COLS = ("timestamp", "open", "high", "low", "close", "volume")


class QualityPipelineConsumer(BaseConsumer):
    """
    Consumer de calidad para lotes OHLCV.

    Suscribe a: OHLCVBatchReceived
    Produce:    registros en AnomalyRegistry + LineageTracker

    Inyección de dependencias
    -------------------------
    registry : AnomalyRegistryPort  — default: default_registry (SQLite singleton)
    tracker  : LineageTracker       — default: lineage_tracker  (SQLite singleton)

    Ambos son sustituibles en tests por instancias con db_path=tmp_path.
    """

    event_type = OHLCVBatchReceived

    def __init__(
        self,
        bus: EventBusPort,
        *,
        registry=None,
        tracker=None,
    ) -> None:
        super().__init__(bus)
        self._registry = registry or default_registry
        self._tracker  = tracker  or _default_tracker

    # ----------------------------------------------------------
    # BaseConsumer contract
    # ----------------------------------------------------------

    def handle(self, event: DomainEvent) -> None:
        """
        Entry point del consumer. Fail-soft: loguea errores, nunca propaga.
        """
        if not isinstance(event, OHLCVBatchReceived):
            logger.warning(
                "QualityPipelineConsumer: received unexpected event type={} — ignoring",
                type(event).__name__,
            )
            return

        try:
            self._process(event)
        except Exception as exc:
            logger.error(
                "QualityPipelineConsumer: unhandled error | "
                "exchange={} symbol={} timeframe={} event_id={} err={}",
                event.batch.exchange, event.batch.symbol, event.batch.timeframe,
                event.event_id[:8], exc,
            )

    # ----------------------------------------------------------
    # Pipeline
    # ----------------------------------------------------------

    def _process(self, event: OHLCVBatchReceived) -> None:
        """
        Pipeline:  raw candles → DataFrame → quality check → lineage record.

        Fail-fast dentro del método: si el DataFrame está vacío, salir temprano.
        """
        df = self._to_dataframe(event)
        if df.empty:
            logger.debug(
                "QualityPipelineConsumer: empty batch — skipping | "
                "exchange={} symbol={} timeframe={}",
                event.batch.exchange, event.batch.symbol, event.batch.timeframe,
            )
            return

        # --- Quality check ---
        checker = DataQualityChecker(
            timeframe=event.batch.timeframe,
            exchange=event.batch.exchange,
            registry=self._registry,
        )
        report = checker.check(df, symbol=event.batch.symbol)

        # --- Lineage record ---
        run_id = event.batch.run_id or self._tracker.new_run_id()
        status = LineageStatus.SUCCESS if report.is_clean else LineageStatus.PARTIAL

        quality_score = getattr(report, "score", None)

        self._tracker.record(LineageEvent(
            run_id        = run_id,
            layer         = PipelineLayer.SILVER,
            exchange      = event.batch.exchange,
            symbol        = event.batch.symbol,
            timeframe     = event.batch.timeframe,
            rows_in       = len(df),
            rows_out      = len(df),
            status        = status,
            quality_score = quality_score,
            params        = {
                "source":      event.batch.source,
                "chunk_index": event.batch.chunk_index,
            },
        ))

        logger.info(
            "QualityPipelineConsumer: ✔ | {}/{} exchange={} rows={} "
            "clean={} score={} run={}",
            event.batch.symbol, event.batch.timeframe, event.batch.exchange,
            len(df), report.is_clean, quality_score, run_id[:8],
        )

    # ----------------------------------------------------------
    # Helpers
    # ----------------------------------------------------------

    @staticmethod
    def _to_dataframe(event: OHLCVBatchReceived) -> pd.DataFrame:
        """Convierte OHLCVBatch a DataFrame OHLCV estándar."""
        if event.batch.is_empty:
            return pd.DataFrame(columns=_CANDLE_COLS)
        return pd.DataFrame(
            [c.to_tuple() for c in event.batch.candles],
            columns=_CANDLE_COLS,
        )


__all__ = ["QualityPipelineConsumer"]
