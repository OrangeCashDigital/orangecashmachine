# -*- coding: utf-8 -*-
"""
market_data/streaming/payloads.py
===================================

Wire format contract para streaming de ingestión OHLCV.

Responsabilidad
---------------
Definir los tipos de datos que viajan por Redis Streams entre el productor
(pipeline de ingestión) y el consumidor (streaming layer).

Wire format
-----------
Redis Streams entrega todos los campos como strings. `from_dict()` normaliza
el wire format — los callers (StreamSource, tests) no conocen el encoding
interno (SRP · Tell Don't Ask).

Tipos
-----
OHLCVBar     — vela OHLCV inmutable: (ts, open, high, low, close, volume)
EventPayload — lote de velas con metadatos de contexto (exchange, symbol, tf)

Versiones de schema
-------------------
PAYLOAD_SCHEMA_VERSION — versión actual del wire format de EventPayload
CONTEXT_SCHEMA_VERSION — versión del contexto de streaming (StreamingContext)

Principios
----------
SRP         — solo define tipos wire; no valida reglas de negocio
Immutability — frozen=True en ambos dataclasses
DIP         — SchemaVersionError importada desde domain (no definida aquí)
SSOT        — versiones de schema declaradas como constantes de módulo
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

# SchemaVersionError migrada a domain/exceptions — importar desde allí (DIP · SSOT)
from market_data.domain.exceptions import SchemaVersionError  # noqa: F401


# ---------------------------------------------------------------------------
# Versiones de schema — SSOT
# ---------------------------------------------------------------------------

PAYLOAD_SCHEMA_VERSION: int = 1
CONTEXT_SCHEMA_VERSION: int = 2


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
# EventPayload — lote de velas con contexto de streaming
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class EventPayload:
    """
    Lote de velas OHLCV con metadatos de contexto.

    Serialización
    -------------
    to_dict()   → dict Python (para Redis Streams o tests)
    from_dict() → instancia (desde wire format o dict Python)

    Compatibilidad
    --------------
    from_dict() acepta tanto wire format (valores string de Redis Streams)
    como dict Python ya deserializado. La normalización ocurre aquí,
    no en los callers (SRP · Tell Don't Ask).
    """
    event_id:       str
    exchange:       str
    symbol:         str
    timeframe:      str
    batch_start_ts: int
    bars:           List[OHLCVBar]
    event_version:  int                   = PAYLOAD_SCHEMA_VERSION
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
        """
        Construye un EventPayload desde un dict.

        Fail-fast: lanza SchemaVersionError si la versión es incompatible.
        No intenta migrar versiones desconocidas — eso es responsabilidad
        del consumer con un migrator explícito.
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


__all__ = [
    "OHLCVBar",
    "EventPayload",
    "SchemaVersionError",       # re-export para backward-compat
    "PAYLOAD_SCHEMA_VERSION",
    "CONTEXT_SCHEMA_VERSION",
]
