# -*- coding: utf-8 -*-
"""
market_data/application/consumers/quality_consumer.py
====================================================

Consumer que ejecuta el pipeline de calidad sobre lotes OHLCV.

Responsabilidad
---------------
Suscribirse a OHLCVBatchReceived y ejecutar la cadena:
  DataFrame → DataQualityCheckerPort → AnomalyRegistry → LineageTracker

Principios
----------
SRP      — un consumer, una responsabilidad: calidad de datos
DIP      — checker inyectado via CheckerFactory (port), no instanciado directamente
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
from market_data.application.consumers.base import BaseConsumer
from market_data.ports.outbound.data_quality_checker import CheckerFactory
from market_data.application.quality.data_quality import native_checker_factory
from market_data.ports.outbound.event_bus import EventBusPort


# Columnas del DataFrame OHLCV — SSOT con OHLCVChunk.candles
_CANDLE_COLS = ("timestamp", "open", "high", "low", "close", "volume")


class QualityPipelineConsumer(BaseConsumer):
    """
    Consumer de calidad para lotes OHLCV.

    Suscribe a: OHLCVBatchReceived
    Produce:    registros en AnomalyRegistry + LineageTracker

    Inyección de dependencias
    -------------------------
    registry        : AnomalyRegistryPort — default: default_registry (SQLite)
    tracker         : LineageTracker      — obligatorio (DIP · BC-05)
    checker_factory : CheckerFactory      — default: native_checker_factory

    checker_factory permite sustituir el backend de validación (GE, nativo,
    mock) sin modificar este consumer (OCP · DIP).
    """

    event_type = OHLCVBatchReceived

    def __init__(
        self,
        bus: EventBusPort,
        *,
        registry        = None,
        tracker,                       # obligatorio — inyectar desde composition root
        checker_factory: CheckerFactory | None = None,
    ) -> None:
        # Fail-fast: tracker es obligatorio.
        # QualityPipelineConsumer no puede importar infrastructure/ (DIP — BC-05).
        if tracker is None:
            raise TypeError(
                "QualityPipelineConsumer: 'tracker' es obligatorio. "
                "Inyectar LineageTracker desde el composition root "
                "(OCMContainer o ConcretePipelineFactory)."
            )
        super().__init__(bus)
        # AnomalyRegistryPort inyectado desde pipeline_factory (Composition Root).
        # NullAnomalyRegistry si no se provee — fail-soft sin acoplamiento a infra.
        self._registry = registry if registry is not None else _null_registry()
        self._tracker         = tracker
        # DIP: checker inyectado — default = native (backward compat)
        self._checker_factory: CheckerFactory = checker_factory or native_checker_factory

    # ----------------------------------------------------------
    # BaseConsumer contract
    # ----------------------------------------------------------

    def handle(self, event: DomainEvent) -> None:
        """Entry point del consumer. Fail-soft: loguea errores, nunca propaga."""
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
        Pipeline: raw candles → DataFrame → quality check → lineage record.

        Fail-fast: si el DataFrame está vacío, salir temprano.
        """
        df = self._to_dataframe(event)
        if df.empty:
            logger.debug(
                "QualityPipelineConsumer: empty batch — skipping | "
                "exchange={} symbol={} timeframe={}",
                event.batch.exchange, event.batch.symbol, event.batch.timeframe,
            )
            return

        # --- Quality check — DIP: checker instanciado via factory inyectada ---
        checker = self._checker_factory(
            event.batch.timeframe,
            event.batch.exchange,
            0,  # rows_removed: consumer no tiene contexto de remoción upstream
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
        """Convierte OHLCVChunk a DataFrame OHLCV estándar."""
        if event.batch.is_empty:
            return pd.DataFrame(columns=_CANDLE_COLS)
        return pd.DataFrame(
            [c.to_tuple() for c in event.batch.candles],
            columns=_CANDLE_COLS,
        )


__all__ = ["QualityPipelineConsumer"]
