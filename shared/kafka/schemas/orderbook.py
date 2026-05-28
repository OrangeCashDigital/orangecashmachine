# -*- coding: utf-8 -*-
"""
shared/kafka/schemas/orderbook.py
==================================

Wire payloads para L2 order book WebSocket.

Topología
---------
  WsOrderBookStream (cryptofeed BOOK channel)
      → [orderbook.raw]   → OrderBookSnapshotPayload (snapshot inicial)
      → [orderbook.raw]   → OrderBookDeltaPayload    (deltas incrementales)

  Consumers downstream:
      BookBuilder → book.snapshot / book.delta (estado L2 reconstruido)
      MicropriceEngine → microprice.rt

Routing key
-----------
  make_symbol_key(exchange, symbol) → b"bybit:BTC/USDT"
  Mismo símbolo → misma partición → FIFO garantizado para snapshot+deltas.

Kappa note
----------
orderbook.raw tiene retención de 1h (alta frecuencia).
Solo se usa para replay de corta ventana — no para backfill histórico.

Schema version history
----------------------
  v1 — snapshot + delta, side como Literal["bid"|"ask"].

Principios: SSOT · DDD · Kappa · Fail-Fast · KISS
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional, Tuple

from shared.kafka.schemas._base import BasePayload

# ---------------------------------------------------------------------------
# Constantes de schema
# ---------------------------------------------------------------------------

ORDERBOOK_SNAPSHOT_SCHEMA_VERSION: int = 1
ORDERBOOK_DELTA_SCHEMA_VERSION: int = 1

# PriceLevel: (price_str, size_str) — str para preservar precisión Decimal
PriceLevel = Tuple[str, str]

Side = Literal["bid", "ask"]

_VALID_SIDES: frozenset[str] = frozenset({"bid", "ask"})


class OrderBookSchemaVersionError(ValueError):
    """Schema version incompatible."""


# ---------------------------------------------------------------------------
# OrderBookSnapshotPayload — snapshot L2 completo → orderbook.raw
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OrderBookSnapshotPayload(BasePayload):
    """
    Snapshot L2 completo del order book.

    Publicado cuando el stream conecta por primera vez o tras reconexión.
    El BookBuilder downstream descarta estado anterior y reconstruye.

    Campos
    ------
    exchange     : exchange de origen
    symbol       : par normalizado (ej. "BTC/USDT")
    timestamp_ms : Unix epoch ms UTC del snapshot
    bids         : lista de (price_str, size_str) ordenada desc por precio
    asks         : lista de (price_str, size_str) ordenada asc por precio
    depth        : niveles por lado en este snapshot
    checksum     : checksum del exchange si disponible (None si no aplica)
    """

    exchange: str = ""
    symbol: str = ""
    timestamp_ms: int = 0
    bids: List[PriceLevel] = field(default_factory=list)
    asks: List[PriceLevel] = field(default_factory=list)
    depth: int = 0
    checksum: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update(
            {
                "event_version": ORDERBOOK_SNAPSHOT_SCHEMA_VERSION,
                "payload_type": "snapshot",
                "exchange": self.exchange,
                "symbol": self.symbol,
                "timestamp_ms": self.timestamp_ms,
                "bids": list(self.bids),
                "asks": list(self.asks),
                "depth": self.depth,
                "checksum": self.checksum,
            }
        )
        return base

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OrderBookSnapshotPayload":
        version = int(data.get("event_version", 1))
        if version != ORDERBOOK_SNAPSHOT_SCHEMA_VERSION:
            raise OrderBookSchemaVersionError(
                f"OrderBookSnapshotPayload schema v{version} "
                f"incompatible con v{ORDERBOOK_SNAPSHOT_SCHEMA_VERSION} esperada."
            )
        return cls(
            event_id=str(data["event_id"]),
            event_version=version,
            occurred_at=str(data.get("occurred_at", "")),
            exchange=str(data["exchange"]),
            symbol=str(data["symbol"]),
            timestamp_ms=int(data["timestamp_ms"]),
            bids=[tuple(lvl) for lvl in data.get("bids", [])],
            asks=[tuple(lvl) for lvl in data.get("asks", [])],
            depth=int(data.get("depth", 0)),
            checksum=data.get("checksum"),
        )


# ---------------------------------------------------------------------------
# OrderBookDeltaPayload — delta incremental → orderbook.raw
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OrderBookDeltaPayload(BasePayload):
    """
    Delta incremental del order book.

    Publicado por cada actualización tras el snapshot inicial.
    El BookBuilder aplica el delta al estado L2 en memoria.

    Campos
    ------
    exchange     : exchange de origen
    symbol       : par normalizado
    timestamp_ms : Unix epoch ms UTC del delta
    side         : "bid" | "ask"
    price        : nivel de precio afectado (str)
    size         : nueva cantidad en ese nivel (str). "0" = eliminar nivel.
    """

    exchange: str = ""
    symbol: str = ""
    timestamp_ms: int = 0
    side: Side = "bid"
    price: str = "0"
    size: str = "0"

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update(
            {
                "event_version": ORDERBOOK_DELTA_SCHEMA_VERSION,
                "payload_type": "delta",
                "exchange": self.exchange,
                "symbol": self.symbol,
                "timestamp_ms": self.timestamp_ms,
                "side": self.side,
                "price": self.price,
                "size": self.size,
            }
        )
        return base

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OrderBookDeltaPayload":
        version = int(data.get("event_version", 1))
        if version != ORDERBOOK_DELTA_SCHEMA_VERSION:
            raise OrderBookSchemaVersionError(
                f"OrderBookDeltaPayload schema v{version} incompatible con v{ORDERBOOK_DELTA_SCHEMA_VERSION} esperada."
            )
        side = data.get("side", "bid")
        if side not in _VALID_SIDES:
            raise OrderBookSchemaVersionError(
                f"OrderBookDeltaPayload.side desconocido: {side!r}. Válidos: {sorted(_VALID_SIDES)}."
            )
        return cls(
            event_id=str(data["event_id"]),
            event_version=version,
            occurred_at=str(data.get("occurred_at", "")),
            exchange=str(data["exchange"]),
            symbol=str(data["symbol"]),
            timestamp_ms=int(data["timestamp_ms"]),
            side=side,
            price=str(data["price"]),
            size=str(data["size"]),
        )


__all__ = [
    "ORDERBOOK_SNAPSHOT_SCHEMA_VERSION",
    "ORDERBOOK_DELTA_SCHEMA_VERSION",
    "PriceLevel",
    "Side",
    "OrderBookSchemaVersionError",
    "OrderBookSnapshotPayload",
    "OrderBookDeltaPayload",
]
