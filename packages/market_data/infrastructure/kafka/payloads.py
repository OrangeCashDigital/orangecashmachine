# -*- coding: utf-8 -*-
"""
market_data/infrastructure/kafka/payloads.py
=============================================

Wire format contract para el pipeline Kafka OHLCV.

Kappa source field
------------------
EventPayload.source discrimina el origen del evento en todo el pipeline:
  "live"     — cryptofeed WebSocket → StrategyConsumer LO PROCESA
  "backfill" — REST histórico       → StrategyConsumer LO IGNORA
  "replay"   — seek_to_beginning()  → StrategyConsumer LO IGNORA

Este campo viaja en el payload JSON Y como Kafka header "x-ocm-source"
para que los consumers puedan filtrar sin deserializar el body completo.

Schema version
--------------
PAYLOAD_SCHEMA_VERSION no se bumpeó al añadir source — tiene default
"live" que mantiene compatibilidad con mensajes anteriores (additive change).

Principios: DIP · SRP · SSOT · Immutability · Kappa · KISS
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional

from market_data.domain.exceptions import SchemaVersionError  # noqa: F401


# ---------------------------------------------------------------------------
# Versión de schema — SSOT
# ---------------------------------------------------------------------------

PAYLOAD_SCHEMA_VERSION: int = 1

# ---------------------------------------------------------------------------
# DataSource — SSOT del discriminador de origen (Kappa)
# ---------------------------------------------------------------------------

DataSource = Literal["live", "backfill", "replay"]

DATASOURCE_LIVE:     DataSource = "live"
DATASOURCE_BACKFILL: DataSource = "backfill"
DATASOURCE_REPLAY:   DataSource = "replay"


# ---------------------------------------------------------------------------
# KafkaOHLCVBar — vela OHLCV inmutable
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class KafkaOHLCVBar:
    """
    Vela OHLCV en formato wire.

    Identidad: (ts, open, high, low, close, volume) — valor completo.
    Inmutable: frozen=True garantiza que no se modifica en tránsito.
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
class EventPayload:
    """
    Lote de velas OHLCV con metadatos de contexto.

    source (Kappa discriminator)
    ----------------------------
    Viaja en el payload JSON y como Kafka header "x-ocm-source".
    Los consumers usan este campo para decidir si procesan el evento:

      KafkaBronzeWriter    → procesa TODOS los sources (almacena siempre)
      QualityGateConsumer  → procesa TODOS los sources
      FeatureConsumer      → procesa TODOS los sources
      StrategyConsumer     → procesa SOLO source="live"
      RiskGateConsumer     → procesa SOLO source="live"
      ExecutionConsumer    → procesa SOLO source="live"

    Compatibilidad
    --------------
    source tiene default "live" — mensajes sin source son tratados como live.
    No requirió bump de PAYLOAD_SCHEMA_VERSION (additive change con default).
    """
    event_id:       str
    exchange:       str
    symbol:         str
    timeframe:      str
    batch_start_ts: int
    bars:           List[KafkaOHLCVBar]
    event_version:  int                      = PAYLOAD_SCHEMA_VERSION
    source:         DataSource               = DATASOURCE_LIVE
    run_id:         str                      = ""
    meta:           Optional[Dict[str, Any]] = None

    # ------------------------------------------------------------------
    # Kappa helpers
    # ------------------------------------------------------------------

    @property
    def is_live(self) -> bool:
        """True si el evento viene de WebSocket en tiempo real."""
        return self.source == DATASOURCE_LIVE

    @property
    def is_backfill(self) -> bool:
        """True si el evento viene de REST histórico."""
        return self.source == DATASOURCE_BACKFILL

    @property
    def is_replay(self) -> bool:
        """True si el evento es un replay de Kafka (seek_to_beginning)."""
        return self.source == DATASOURCE_REPLAY

    @property
    def should_generate_signal(self) -> bool:
        """
        True si los consumers de señal deben procesar este evento.

        SSOT de la regla de filtrado Kappa — un solo punto de decisión.
        StrategyConsumer y RiskGateConsumer deben usar esta propiedad.
        """
        return self.source == DATASOURCE_LIVE

    # ------------------------------------------------------------------
    # Serialización
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_version":  self.event_version,
            "event_id":       self.event_id,
            "exchange":       self.exchange,
            "symbol":         self.symbol,
            "timeframe":      self.timeframe,
            "batch_start_ts": self.batch_start_ts,
            "source":         self.source,
            "run_id":         self.run_id,
            "bars":           [b.to_dict() for b in self.bars],
            "meta":           self.meta,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EventPayload":
        version = int(data.get("event_version", 1))
        if version != PAYLOAD_SCHEMA_VERSION:
            raise SchemaVersionError(
                f"EventPayload schema v{version} incompatible "
                f"con v{PAYLOAD_SCHEMA_VERSION} esperada."
            )

        bars_raw = data["bars"]
        if isinstance(bars_raw, str):
            bars_raw = json.loads(bars_raw)
        bars = [KafkaOHLCVBar.from_dict(b) for b in bars_raw]

        meta_raw = data.get("meta")
        if isinstance(meta_raw, str):
            meta_raw = json.loads(meta_raw)

        source = data.get("source", DATASOURCE_LIVE)
        if source not in (DATASOURCE_LIVE, DATASOURCE_BACKFILL, DATASOURCE_REPLAY):
            source = DATASOURCE_LIVE  # fail-soft: source desconocido → live

        return cls(
            event_id       = str(data["event_id"]),
            exchange       = str(data["exchange"]),
            symbol         = str(data["symbol"]),
            timeframe      = str(data["timeframe"]),
            batch_start_ts = int(data["batch_start_ts"]),
            bars           = bars,
            event_version  = version,
            source         = source,
            run_id         = str(data.get("run_id", "")),
            meta           = meta_raw,
        )


__all__ = [
    "KafkaOHLCVBar",
    "EventPayload",
    "SchemaVersionError",
    "PAYLOAD_SCHEMA_VERSION",
    "DataSource",
    "DATASOURCE_LIVE",
    "DATASOURCE_BACKFILL",
    "DATASOURCE_REPLAY",
]
