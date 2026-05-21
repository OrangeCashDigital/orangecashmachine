# -*- coding: utf-8 -*-
"""
market_data/domain/events/ingestion.py
========================================

Domain events de ingestión — Value Objects inmutables.

Catálogo completo
-----------------
CandleReceived      — tick individual desde WebSocket (cryptofeed)
OHLCVBatchReceived  — lote desde REST fetcher o replay
OHLCVBatchIngested  — Bronze confirmó escritura → dispara ohlcv.validated
QualityCheckPassed  — quality gate OK → dispara ohlcv.features
QualityCheckFailed  — quality gate KO → va a DLQ
SignalGenerated     — estrategia generó señal → dispara signals.raw

source field (Kappa)
--------------------
Todos los eventos llevan source para que los consumers puedan filtrar
sin conocer el contexto del emisor. SSOT: payloads.DATASOURCE_*.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from market_data.domain.events._base import DomainEvent
from market_data.domain.value_objects.ohlcv_chunk import OHLCVChunk


# ===========================================================================
# Ingestión — entrada al stream
# ===========================================================================


@dataclass(frozen=True)
class CandleReceived(DomainEvent):
    """
    Tick OHLCV desde cryptofeed WebSocket.

    source: siempre "live" — los ticks WS nunca son backfill.
    Publicado a: ohlcv.raw con header x-ocm-source: live
    """

    exchange: str = ""
    symbol: str = ""
    timeframe: str = ""
    timestamp_ms: int = 0
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: float = 0.0
    source: str = "live"
    run_id: str = ""


@dataclass(frozen=True)
class OHLCVBatchReceived(DomainEvent):
    """
    Lote de velas desde REST fetcher (backfill) o replay.

    source: "backfill" (REST histórico) | "replay" (seek_to_beginning)
    Publicado a: ohlcv.raw con header x-ocm-source: backfill|replay
    """

    batch: OHLCVChunk = field(default_factory=OHLCVChunk)
    source: str = "backfill"
    run_id: str = ""

    @property
    def row_count(self) -> int:
        return self.batch.count

    @property
    def is_last_chunk(self) -> bool:
        return self.batch.is_last_chunk


# ===========================================================================
# Procesamiento — downstream del stream
# ===========================================================================


@dataclass(frozen=True)
class OHLCVBatchIngested(DomainEvent):
    """
    KafkaBronzeWriter confirmó escritura a Bronze Iceberg.

    Disparado por: KafkaBronzeWriter al completar write()
    Publicado a:   ohlcv.validated (mismo source del evento original)
    Consumido por: FeatureConsumer

    rows_written : número de filas escritas en Bronze
    source       : propagado desde el EventPayload original (SSOT)
    """

    exchange: str = ""
    symbol: str = ""
    timeframe: str = ""
    rows_written: int = 0
    source: str = "live"
    run_id: str = ""


@dataclass(frozen=True)
class QualityCheckPassed(DomainEvent):
    """
    Quality gate validó el batch — datos aptos para features.

    Disparado por: QualityGateConsumer
    Publicado a:   ohlcv.features
    """

    exchange: str = ""
    symbol: str = ""
    timeframe: str = ""
    rows: int = 0
    source: str = "live"
    run_id: str = ""


@dataclass(frozen=True)
class QualityCheckFailed(DomainEvent):
    """
    Quality gate rechazó el batch — va a DLQ.

    Disparado por: QualityGateConsumer
    Publicado a:   dlq.ohlcv
    """

    exchange: str = ""
    symbol: str = ""
    timeframe: str = ""
    reason: str = ""
    source: str = "live"
    run_id: str = ""


@dataclass(frozen=True)
class SignalGenerated(DomainEvent):
    """
    Estrategia generó una señal de trading.

    Solo existe para eventos con source="live".
    StrategyConsumer nunca genera este evento para source="backfill".

    Disparado por: StrategyConsumer
    Publicado a:   signals.raw
    Consumido por: RiskGateConsumer
    """

    exchange: str = ""
    symbol: str = ""
    timeframe: str = ""
    signal: str = ""  # "buy" | "sell" | "hold"
    price: float = 0.0
    confidence: float = 1.0
    strategy: str = ""  # nombre de la estrategia
    run_id: str = ""


# ===========================================================================
# __all__
# ===========================================================================

__all__ = [
    "DomainEvent",
    "CandleReceived",
    "OHLCVBatchReceived",
    "OHLCVBatchIngested",
    "QualityCheckPassed",
    "QualityCheckFailed",
    "SignalGenerated",
]
