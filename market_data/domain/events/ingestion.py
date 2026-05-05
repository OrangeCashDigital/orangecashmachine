# -*- coding: utf-8 -*-
"""
market_data/domain/events/ingestion.py
========================================

Domain events de la capa de ingestión — Value Objects inmutables.

Responsabilidad
---------------
Representar, como datos puros, los momentos en que datos de mercado
llegan desde los adapters (REST fetcher, WebSocket adapter, replay).

Estos eventos son el contrato entre ingestión y procesamiento.
Ningún adapter ni consumer importa el otro — solo estos tipos compartidos.

Principios
----------
DDD    — domain events como ciudadanos de primera clase del modelo
SSOT   — única definición; adapters publican, consumers consumen
OCP    — nuevos tipos de evento sin modificar los existentes
KISS   — solo los campos que el dominio necesita; sin lógica
Immutability — frozen=True; el estado queda sellado en creación
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Tuple


# ===========================================================================
# Base
# ===========================================================================

@dataclass(frozen=True)
class DomainEvent:
    """
    Raíz común de todos los domain events.

    event_id    : UUID v4 — idempotencia y deduplicación downstream
    occurred_at : ISO-8601 UTC del momento de creación del evento
                  (≠ timestamp del dato; el evento se crea al llegar al bus)
    """
    event_id:    str = field(default_factory=lambda: str(uuid.uuid4()))
    occurred_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


# ===========================================================================
# Ingestion Events
# ===========================================================================

@dataclass(frozen=True)
class CandleReceived(DomainEvent):
    """
    Una vela OHLCV llegó desde un adapter de exchange.

    Publicado por  : WebSocket adapter (real-time tick-by-tick)
    Consumido por  : QualityPipelineConsumer, FeatureConsumer (futuro)

    Fields
    ------
    exchange      : identificador del exchange ("binance", "okx", …)
    symbol        : par de trading ("BTC/USDT")
    timeframe     : resolución canónica ("1m", "1h", …)
    timestamp_ms  : timestamp de la vela en epoch ms UTC
    open/high/low/close : precios OHLC
    volume        : volumen de la vela
    source        : "websocket" | "rest" | "replay"
    run_id        : correlación con LineageTracker (vacío si no aplica)
    """
    exchange:     str   = ""
    symbol:       str   = ""
    timeframe:    str   = ""
    timestamp_ms: int   = 0
    open:         float = 0.0
    high:         float = 0.0
    low:          float = 0.0
    close:        float = 0.0
    volume:       float = 0.0
    source:       str   = "websocket"   # "websocket" | "rest" | "replay"
    run_id:       str   = ""


# ---------------------------------------------------------------------------
# Candle tuple type alias
# (timestamp_ms, open, high, low, close, volume)
# ---------------------------------------------------------------------------
CandleTuple = Tuple[int, float, float, float, float, float]


@dataclass(frozen=True)
class OHLCVBatchReceived(DomainEvent):
    """
    Un lote de velas OHLCV llegó desde un adapter de exchange.

    Preferido sobre múltiples CandleReceived cuando el batch completo
    es la unidad semántica correcta: REST fetch, backfill chunk, replay.

    Publicado por  : REST fetcher, batch replay adapter
    Consumido por  : QualityPipelineConsumer, BatchConsumer

    Fields
    ------
    exchange      : identificador del exchange
    symbol        : par de trading
    timeframe     : resolución canónica
    candles       : tuplas (timestamp_ms, open, high, low, close, volume)
                    inmutables → tuple-of-tuples, no list
    source        : "rest" | "replay"
    run_id        : correlación con LineageTracker
    chunk_index   : índice del chunk en un backfill (0-based)
    total_chunks  : total de chunks esperados (None si desconocido)
    """
    exchange:     str                        = ""
    symbol:       str                        = ""
    timeframe:    str                        = ""
    candles:      Tuple[CandleTuple, ...]    = field(default_factory=tuple)
    source:       str                        = "rest"
    run_id:       str                        = ""
    chunk_index:  int                        = 0
    total_chunks: Optional[int]              = None

    @property
    def row_count(self) -> int:
        """Número de velas en el batch."""
        return len(self.candles)

    @property
    def is_last_chunk(self) -> bool:
        """True si este es el último chunk de un backfill."""
        if self.total_chunks is None:
            return False
        return self.chunk_index >= self.total_chunks - 1


# ===========================================================================
# __all__
# ===========================================================================

__all__ = [
    "DomainEvent",
    "CandleReceived",
    "CandleTuple",
    "OHLCVBatchReceived",
]
