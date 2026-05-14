# -*- coding: utf-8 -*-
"""
market_data/infrastructure/kafka/payloads.py
=============================================

Wire format contract para el pipeline Kafka OHLCV.

Responsabilidad
---------------
Definir los tipos de datos que viajan por Kafka entre el productor
(pipeline de ingestión) y los stream processors (KafkaBronzeWriter,
KafkaSilverWriter, etc.).

Wire format
-----------
Kafka entrega el value como bytes JSON. `from_dict()` normaliza
el dict deserializado — los callers no conocen el encoding interno
(SRP · Tell Don't Ask).

Tipos
-----
OHLCVBar     — vela OHLCV inmutable: (ts, open, high, low, close, volume)
EventPayload — lote de velas con metadatos de contexto (exchange, symbol, tf)

Versión de schema
-----------------
PAYLOAD_SCHEMA_VERSION — versión actual del wire format de EventPayload.
SSOT: cualquier bumping de versión ocurre aquí y solo aquí.

Principios
----------
SRP         — solo define tipos wire; no valida reglas de negocio
Immutability — frozen=True en ambos dataclasses
DIP         — SchemaVersionError importada desde domain (no definida aquí)
SSOT        — versión de schema declarada como constante de módulo
KISS        — sin dependencias externas más allá de stdlib
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from market_data.domain.exceptions import SchemaVersionError  # noqa: F401


# ---------------------------------------------------------------------------
# Versión de schema — SSOT
# ---------------------------------------------------------------------------

PAYLOAD_SCHEMA_VERSION: int = 1


# ---------------------------------------------------------------------------
# OHLCVBar — vela OHLCV inmutable
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class OHLCVBar:
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
    def from_dict(cls, data: Dict[str, Any]) -> "OHLCVBar":
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

    Serialización
    -------------
    to_dict()   → dict Python (para JSON Kafka o tests)
    from_dict() → instancia (desde bytes deserializados o dict Python)

    Compatibilidad
    --------------
    from_dict() acepta tanto valores string (legacy Redis Streams)
    como tipos nativos Python ya deserializados. La normalización
    ocurre aquí, no en los callers (SRP · Tell Don't Ask).

    Fail-Fast
    ---------
    from_dict() lanza SchemaVersionError si la versión es incompatible.
    No intenta migrar versiones desconocidas — eso es responsabilidad
    del consumer con un migrator explícito.
    """
    event_id:       str
    exchange:       str
    symbol:         str
    timeframe:      str
    batch_start_ts: int
    bars:           List[OHLCVBar]
    event_version:  int                      = PAYLOAD_SCHEMA_VERSION
    meta:           Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_version":  self.event_version,
            "event_id":       self.event_id,
            "exchange":       self.exchange,
            "symbol":         self.symbol,
            "timeframe":      self.timeframe,
            "batch_start_ts": self.batch_start_ts,
            "bars":           [b.to_dict() for b in self.bars],
            "meta":           self.meta,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EventPayload":
        version = int(data.get("event_version", 1))
        if version != PAYLOAD_SCHEMA_VERSION:
            raise SchemaVersionError(
                f"EventPayload schema v{version} incompatible "
                f"con v{PAYLOAD_SCHEMA_VERSION} esperada. "
                "Actualizar consumer o migrar el evento."
            )

        bars_raw = data["bars"]
        if isinstance(bars_raw, str):
            bars_raw = json.loads(bars_raw)
        bars = [OHLCVBar.from_dict(b) for b in bars_raw]

        meta_raw = data.get("meta")
        if isinstance(meta_raw, str):
            meta_raw = json.loads(meta_raw)

        return cls(
            event_id       = str(data["event_id"]),
            exchange       = str(data["exchange"]),
            symbol         = str(data["symbol"]),
            timeframe      = str(data["timeframe"]),
            batch_start_ts = int(data["batch_start_ts"]),
            bars           = bars,
            event_version  = version,
            meta           = meta_raw,
        )


__all__ = [
    "OHLCVBar",
    "EventPayload",
    "SchemaVersionError",
    "PAYLOAD_SCHEMA_VERSION",
]
