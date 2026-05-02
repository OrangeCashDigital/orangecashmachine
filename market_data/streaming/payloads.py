from __future__ import annotations

"""Event payload definitions for streaming ingestion.

Wire format contract
--------------------
Redis Streams entrega todos los campos como strings. from_dict() es
responsable de normalizar el wire format — los callers (StreamSource,
tests) no deben conocer el encoding interno.

Principios: SRP · immutability · wire/domain separation
"""

import json
from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass(frozen=True)
class OHLCVBar:
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


# --------------------------------------------------
# Versiones de schema — SSoT de compatibilidad
# --------------------------------------------------
PAYLOAD_SCHEMA_VERSION: int = 1
CONTEXT_SCHEMA_VERSION: int = 2


class SchemaVersionError(ValueError):
    """El payload tiene una versión de schema incompatible."""


@dataclass(frozen=True)
class EventPayload:
    event_id:       str
    exchange:       str
    symbol:         str
    timeframe:      str
    batch_start_ts: int
    bars:           List[OHLCVBar]
    event_version:  int                  = PAYLOAD_SCHEMA_VERSION
    meta:           Dict[str, Any] | None = None

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
        """
        Construye un EventPayload desde un dict.

        Acepta tanto wire format (Redis Streams — valores string) como
        dict Python ya deserializado. La normalización ocurre aquí,
        no en los callers.
        """
        version = int(data.get("event_version", 1))
        if version != PAYLOAD_SCHEMA_VERSION:
            raise SchemaVersionError(
                f"EventPayload schema v{version} incompatible "
                f"con v{PAYLOAD_SCHEMA_VERSION} esperada. "
                "Actualizar consumer o migrar el evento."
            )

        # bars: wire format → JSON string; domain format → list[dict]
        bars_raw = data["bars"]
        if isinstance(bars_raw, str):
            bars_raw = json.loads(bars_raw)
        bars = [OHLCVBar.from_dict(b) for b in bars_raw]

        # meta: wire format → JSON string o "null"; domain → dict | None
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
