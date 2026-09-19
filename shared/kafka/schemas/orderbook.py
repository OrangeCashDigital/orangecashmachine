# -*- coding: utf-8 -*-
"""
shared/kafka/schemas/orderbook.py
==================================

Wire payloads para L2 order book WebSocket.

Topología
---------
  WsOrderBookStream (cryptofeed BOOK channel)
      → [orderbook.raw]   → OrderBookSnapshotPayload (snapshot inicial)
      → [orderbook.raw]   → OrderBookDeltaPayload    (delta incremental atómico)

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
  v1 — snapshot + delta, side como Literal["bid"|"ask"], delta de un solo nivel.
  v2 — BREAKING (ver _base.py política de compatibilidad):
         * Delta pasa de "un nivel por mensaje" (side/price/size) a un delta
           ATÓMICO multinivel (bids/asks listas completas del mensaje Bybit).
           Esto preserva la atomicidad multinivel observada en el wire de
           Bybit (D-7a): un mensaje wire = un delta atómico.
         * Añade update_id / cross_seq / cts_ms (u/seq/cts de Bybit) a
           snapshot y delta. El campo de continuidad es update_id ('u'):
           gaps se detectan por 'u', NO por 'seq'+1 (D-7b).
         * Precios/cantidades siguen siendo str (preservan precisión Decimal,
           D-7c) — ver PriceLevel.
  v2 snapshot: additive sobre v1 en TÉRMINOS de campos, pero se bumpa a 2
  junto al delta para mantener un event_version coherente en orderbook.raw
  y evitar un estado mixto snap=1/delta=2 en la misma partición.

Principios: SSOT · DDD · Kappa · Fail-Fast · KISS
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, ClassVar, Dict, List, Optional, Tuple

from shared.kafka.schemas._base import BasePayload, SchemaVersionError

# ---------------------------------------------------------------------------
# Constantes de schema
# ---------------------------------------------------------------------------

# PriceLevel: (price_str, size_str) — str para preservar precisión Decimal (D-7c).
PriceLevel = Tuple[str, str]

# Alias de compatibilidad — el canónico es SchemaVersionError (_base.py).
OrderBookSchemaVersionError = SchemaVersionError


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
    update_id    : 'u' de Bybit — token de continuidad monótono. El BookBuilder
                   descarta estado anterior al recibir un snapshot y usa este
                   valor como base de la detección de gaps en deltas posteriores.
    cross_seq    : 'seq' de Bybit (secuencia cruzada) — NO es base de gaps (D-7b).
    cts_ms       : 'cts' de Bybit — timestamp del exchange en ms (puede diferir de ts).
    bids         : lista de (price_str, size_str) ordenada desc por precio
    asks         : lista de (price_str, size_str) ordenada asc por precio
    depth        : niveles por lado en este snapshot
    checksum     : checksum del exchange si disponible (None si no aplica)
    """

    SCHEMA_VERSION: ClassVar[int] = 2

    exchange: str = ""
    symbol: str = ""
    timestamp_ms: int = 0
    update_id: int = 0
    cross_seq: Optional[int] = None
    cts_ms: Optional[int] = None
    bids: List[PriceLevel] = field(default_factory=list)
    asks: List[PriceLevel] = field(default_factory=list)
    depth: int = 0
    checksum: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update(
            {
                "payload_type": "snapshot",
                "exchange": self.exchange,
                "symbol": self.symbol,
                "timestamp_ms": self.timestamp_ms,
                "update_id": self.update_id,
                "cross_seq": self.cross_seq,
                "cts_ms": self.cts_ms,
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
        if version != cls.SCHEMA_VERSION:
            raise SchemaVersionError(
                f"OrderBookSnapshotPayload schema v{version} incompatible con v{cls.SCHEMA_VERSION} esperada."
            )
        return cls(
            event_id=str(data["event_id"]),
            occurred_at=str(data.get("occurred_at", "")),
            exchange=str(data["exchange"]),
            symbol=str(data["symbol"]),
            timestamp_ms=int(data["timestamp_ms"]),
            update_id=int(data.get("update_id", 0)),
            cross_seq=data.get("cross_seq"),
            cts_ms=data.get("cts_ms"),
            bids=[tuple(lvl) for lvl in data.get("bids", [])],
            asks=[tuple(lvl) for lvl in data.get("asks", [])],
            depth=int(data.get("depth", 0)),
            checksum=data.get("checksum"),
        )


# ---------------------------------------------------------------------------
# OrderBookDeltaPayload — delta atómico multinivel → orderbook.raw
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OrderBookDeltaPayload(BasePayload):
    """
    Delta incremental del order book — ATÓMICO y MULTINIVEL (D-7a/D-7b).

    Un mensaje wire = un único mensaje Bybit (snapshot-type: delta), que trae
    TODOS los niveles bid y ask que el exchange actualiza en esa actualización
    (observado en P0: hasta 88 niveles por mensaje, 75.1% multinivel). Publicar
    cada nivel por separado (v1) rompía la atomicidad: un crash a mitad reventaba
    la coherencia del libro. En v2 el BookBuilder aplica el delta completo de un
    mensaje de forma atómica.

    Continuidad / gaps (D-7b)
    -------------------------
    update_id  : 'u' de Bybit — monótono +1 entre mensajes consecutivos en el
                 mismo stream. El BookBuilder detecta gaps con update_id:
                     next.update_id != last.update_id + 1  → gap → invalidar
                                               y pedir snapshot/recovery.
                 NO se usa 'seq' (cross_seq) para continuidad: en P0 se observó
                 que 'seq' tiene huecos no atómicos (min gap 9, max 7593).
    cross_seq  : 'seq' de Bybit — metadata de correlación cruzada, no de gaps.
    cts_ms     : 'cts' de Bybit — timestamp del exchange en ms.

    Protocolo de borrado de nivel
    -----------------------------
    Un nivel (price, "0") dentro de bids/asks significa "eliminar el nivel":
    es el protocolo estándar de Bybit. El BookBuilder lo aplica con
    update_id como token de continuidad.

    Campos
    ------
    exchange     : exchange de origen
    symbol       : par normalizado
    timestamp_ms : Unix epoch ms UTC del delta
    update_id    : 'u' — token de continuidad monótono (base de gap detection)
    cross_seq    : 'seq' — metadata (no base de gaps)
    cts_ms       : 'cts' — timestamp del exchange
    bids         : niveles bid afectados por esta actualización (atómico)
    asks         : niveles ask afectados por esta actualización (atómico)
    """

    SCHEMA_VERSION: ClassVar[int] = 2

    exchange: str = ""
    symbol: str = ""
    timestamp_ms: int = 0
    update_id: int = 0
    cross_seq: Optional[int] = None
    cts_ms: Optional[int] = None
    bids: List[PriceLevel] = field(default_factory=list)
    asks: List[PriceLevel] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update(
            {
                "payload_type": "delta",
                "exchange": self.exchange,
                "symbol": self.symbol,
                "timestamp_ms": self.timestamp_ms,
                "update_id": self.update_id,
                "cross_seq": self.cross_seq,
                "cts_ms": self.cts_ms,
                "bids": list(self.bids),
                "asks": list(self.asks),
            }
        )
        return base

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OrderBookDeltaPayload":
        version = int(data.get("event_version", 1))
        if version != cls.SCHEMA_VERSION:
            raise SchemaVersionError(
                f"OrderBookDeltaPayload schema v{version} incompatible con v{cls.SCHEMA_VERSION} esperada."
            )
        return cls(
            event_id=str(data["event_id"]),
            occurred_at=str(data.get("occurred_at", "")),
            exchange=str(data["exchange"]),
            symbol=str(data["symbol"]),
            timestamp_ms=int(data["timestamp_ms"]),
            update_id=int(data.get("update_id", 0)),
            cross_seq=data.get("cross_seq"),
            cts_ms=data.get("cts_ms"),
            bids=[tuple(lvl) for lvl in data.get("bids", [])],
            asks=[tuple(lvl) for lvl in data.get("asks", [])],
        )


# Alias de compatibilidad — SSOT de versión: SCHEMA_VERSION de cada clase.
ORDERBOOK_SNAPSHOT_SCHEMA_VERSION: int = OrderBookSnapshotPayload.SCHEMA_VERSION
ORDERBOOK_DELTA_SCHEMA_VERSION: int = OrderBookDeltaPayload.SCHEMA_VERSION

assert ORDERBOOK_SNAPSHOT_SCHEMA_VERSION == 2, "orderbook snapshot schema must be v2"
assert ORDERBOOK_DELTA_SCHEMA_VERSION == 2, "orderbook delta schema must be v2"


__all__ = [
    "ORDERBOOK_SNAPSHOT_SCHEMA_VERSION",
    "ORDERBOOK_DELTA_SCHEMA_VERSION",
    "PriceLevel",
    "OrderBookSchemaVersionError",
    "OrderBookSnapshotPayload",
    "OrderBookDeltaPayload",
]
