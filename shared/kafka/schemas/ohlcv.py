# -*- coding: utf-8 -*-
"""
shared/kafka/schemas/ohlcv.py
==============================

Wire payloads para el dominio OHLCV.

Migración desde market_data/infrastructure/kafka/payloads.py
-------------------------------------------------------------
Este módulo es el NUEVO hogar de EventPayload y KafkaOHLCVBar.
El módulo original en market_data/ será deprecado y eliminado en Fase 2.

Durante la transición, market_data/infrastructure/kafka/payloads.py
re-exportará desde aquí para mantener compatibilidad sin duplicación.

Kappa source field
------------------
EventPayload.source discrimina el origen del evento en todo el pipeline:
  "live"     — WebSocket en tiempo real → StrategyConsumer LO PROCESA
  "backfill" — REST histórico           → StrategyConsumer LO IGNORA
  "replay"   — seek_to_beginning()      → StrategyConsumer LO IGNORA

Particionado
------------
Routing key: "{exchange}:{symbol}:{timeframe}" → orden por par/timeframe.
Mismo par+timeframe → misma partición → garantía de orden FIFO.

Retention policy (documentada, aplicar en Kafka admin)
-------------------------------------------------------
  ohlcv.raw       → delete, retention.ms = 604800000  (7 días)
  ohlcv.validated → delete, retention.ms = 259200000  (3 días)
  ohlcv.features  → delete, retention.ms = 86400000   (1 día)
  ohlcv.dlq       → delete, retention.ms = 2592000000 (30 días)

Schema version history
----------------------
  v1 (actual) — campos base: event_id, exchange, symbol, timeframe,
                batch_start_ts, bars, source, run_id, meta.
                Additive changes no requieren bump.

Principios: SSOT · DDD · Kappa · Fail-Fast · KISS
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional

from shared.kafka.schemas._base import BasePayload


# ---------------------------------------------------------------------------
# Constantes de schema
# ---------------------------------------------------------------------------

OHLCV_SCHEMA_VERSION: int = 1

DataSource = Literal["live", "backfill", "replay"]

DATASOURCE_LIVE:     DataSource = "live"
DATASOURCE_BACKFILL: DataSource = "backfill"
DATASOURCE_REPLAY:   DataSource = "replay"

_VALID_SOURCES = frozenset({DATASOURCE_LIVE, DATASOURCE_BACKFILL, DATASOURCE_REPLAY})


# ---------------------------------------------------------------------------
# SchemaVersionError — Fail-Fast en deserialización
# ---------------------------------------------------------------------------

class OHLCVSchemaVersionError(ValueError):
    """Schema version incompatible en EventPayload.from_dict()."""


# ---------------------------------------------------------------------------
# KafkaOHLCVBar — vela OHLCV inmutable en formato wire
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class KafkaOHLCVBar:
    """
    Vela OHLCV en formato wire.

    Identidad: (ts, open, high, low, close, volume).
    Inmutable: frozen=True garantiza que no se modifica en tránsito.
    ts en milisegundos UTC epoch.
    """
    ts:     int
    open:   float
    high:   float
    low:    float
    close:  float
    volume: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ts":     self.ts,
            "open":   self.open,
            "high":   self.high,
            "low":    self.low,
            "close":  self.close,
            "volume": self.volume,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "KafkaOHLCVBar":
        return cls(
            ts     = int(data["ts"]),
            open   = float(data["open"]),
            high   = float(data["high"]),
            low    = float(data["low"]),
            close  = float(data["close"]),
            volume = float(data["volume"]),
        )


# ---------------------------------------------------------------------------
# EventPayload — lote de velas con contexto de pipeline
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class EventPayload(BasePayload):
    """
    Lote de velas OHLCV con metadatos de contexto.

    Extiende BasePayload con campos específicos del dominio OHLCV.

    Campos de dominio
    -----------------
    exchange       : exchange de origen (e.g. "bybit")
    symbol         : par normalizado   (e.g. "BTC/USDT")
    timeframe      : resolución        (e.g. "1m", "1h")
    batch_start_ts : ms UTC de la primera vela del lote
    bars           : lista de velas OHLCV
    source         : Kappa discriminator (live | backfill | replay)
    run_id         : correlación con el run que generó el evento

    Kappa helpers
    -------------
    is_live, is_backfill, is_replay, should_generate_signal
    SSOT de la regla de filtrado — consumers usan estas propiedades.
    """
    exchange:       str                      = ""
    symbol:         str                      = ""
    timeframe:      str                      = ""
    batch_start_ts: int                      = 0
    bars:           List[KafkaOHLCVBar]      = field(default_factory=list)
    source:         DataSource               = DATASOURCE_LIVE
    run_id:         str                      = ""
    meta:           Optional[Dict[str, Any]] = None

    # ------------------------------------------------------------------
    # Kappa helpers — SSOT de reglas de filtrado
    # ------------------------------------------------------------------

    @property
    def is_live(self) -> bool:
        return self.source == DATASOURCE_LIVE

    @property
    def is_backfill(self) -> bool:
        return self.source == DATASOURCE_BACKFILL

    @property
    def is_replay(self) -> bool:
        return self.source == DATASOURCE_REPLAY

    @property
    def should_generate_signal(self) -> bool:
        """
        SSOT de la regla: solo source=live genera señales.
        StrategyConsumer y RiskGateConsumer usan esta propiedad.
        """
        return self.source == DATASOURCE_LIVE

    # ------------------------------------------------------------------
    # Serialización
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "event_version":  OHLCV_SCHEMA_VERSION,
            "exchange":       self.exchange,
            "symbol":         self.symbol,
            "timeframe":      self.timeframe,
            "batch_start_ts": self.batch_start_ts,
            "source":         self.source,
            "run_id":         self.run_id,
            "bars":           [b.to_dict() for b in self.bars],
            "meta":           self.meta,
        })
        return base

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EventPayload":
        version = int(data.get("event_version", 1))
        if version != OHLCV_SCHEMA_VERSION:
            raise OHLCVSchemaVersionError(
                f"EventPayload schema v{version} incompatible "
                f"con v{OHLCV_SCHEMA_VERSION} esperada."
            )

        bars_raw = data["bars"]
        if isinstance(bars_raw, str):
            bars_raw = json.loads(bars_raw)
        bars = [KafkaOHLCVBar.from_dict(b) for b in bars_raw]

        meta_raw = data.get("meta")
        if isinstance(meta_raw, str):
            meta_raw = json.loads(meta_raw)

        source = data.get("source", DATASOURCE_LIVE)
        if source not in _VALID_SOURCES:
            raise OHLCVSchemaVersionError(
                f"EventPayload.source desconocido: {source!r}. "
                f"Válidos: {sorted(_VALID_SOURCES)}. "
                "Enviar mensaje al DLQ — no asumir source=live en trading."
            )

        return cls(
            event_id       = str(data["event_id"]),
            event_version  = version,
            occurred_at    = str(data.get("occurred_at", "")),
            exchange       = str(data["exchange"]),
            symbol         = str(data["symbol"]),
            timeframe      = str(data["timeframe"]),
            batch_start_ts = int(data["batch_start_ts"]),
            bars           = bars,
            source         = source,
            run_id         = str(data.get("run_id", "")),
            meta           = meta_raw,
        )


__all__ = [
    "OHLCV_SCHEMA_VERSION",
    "DataSource",
    "DATASOURCE_LIVE",
    "DATASOURCE_BACKFILL",
    "DATASOURCE_REPLAY",
    "OHLCVSchemaVersionError",
    "KafkaOHLCVBar",
    "EventPayload",
]
