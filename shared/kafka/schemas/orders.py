# -*- coding: utf-8 -*-
"""
shared/kafka/schemas/orders.py
================================

Wire payloads para el ciclo de vida de órdenes.

Topología
---------
  ExecutionConsumer/OMS → [orders.filled]   → OrderFilledPayload
  ExecutionConsumer/OMS → [orders.rejected] → OrderRejectedPayload

Separación dominio / wire
--------------------------
  shared/types/order_events.py → OrderFilled, OrderRejected (domain events)
  shared/kafka/schemas/orders.py → OrderFilledPayload (wire)

Exactly-once semántics — nota de diseño
----------------------------------------
orders.filled es el topic más crítico del sistema. Procesar una orden
dos veces puede duplicar posiciones. El consumer (PortfolioConsumer)
DEBE implementar idempotencia via Redis SETNX con event_id:

    if not redis.setnx(f"order:seen:{event_id}", 1):
        return  # ya procesado — skip silencioso

El wire payload provee event_id (UUID v4) para este propósito.

Routing key
-----------
  "{exchange}:{symbol}" — orden por símbolo, sin timeframe.

Retention policy (aplicar en Kafka admin)
-----------------------------------------
  orders.filled   → delete, retention.ms = 2592000000 (30 días, auditoría)
  orders.rejected → delete, retention.ms = 2592000000 (30 días, auditoría)

Compaction policy
-----------------
  NO usar compaction en orders — todas las órdenes son relevantes,
  no solo la última por key. Usar delete retention.

Schema version history
----------------------
  v1 (actual) — campos base del ciclo de vida de órdenes.

Principios: SSOT · DDD · Fail-Fast · KISS · exactly-once awareness
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Literal

from shared.kafka.schemas._base import BasePayload


ORDER_FILLED_SCHEMA_VERSION:   int = 1
ORDER_REJECTED_SCHEMA_VERSION: int = 1

OrderSide = Literal["buy", "sell"]


class OrderSchemaVersionError(ValueError):
    """Schema version incompatible en OrderFilledPayload.from_dict()."""


# ---------------------------------------------------------------------------
# OrderFilledPayload — orden ejecutada → orders.filled
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class OrderFilledPayload(BasePayload):
    """
    Orden ejecutada exitosamente.

    Publicado a: orders.filled
    Consumido por: PortfolioConsumer → TradeTracker, analytics

    Idempotencia
    ------------
    PortfolioConsumer DEBE usar event_id para dedup via Redis SETNX.
    fill_price es el precio real de ejecución (puede diferir del signal price).

    Campos
    ------
    order_id   : ID único de la orden en el OMS
    exchange   : exchange donde se ejecutó
    symbol     : par de trading (e.g. "BTC/USDT")
    side       : "buy" | "sell"
    fill_price : precio de ejecución real
    size_pct   : % del capital asignado ∈ (0, 1]
    filled_at  : ISO-8601 UTC de ejecución
    signal_event_id : event_id del ApprovedSignalPayload que originó la orden
    run_id     : correlación con el run
    """
    order_id:        str       = ""
    exchange:        str       = ""
    symbol:          str       = ""
    side:            OrderSide = "buy"
    fill_price:      float     = 0.0
    size_pct:        float     = 0.0
    filled_at:       str       = ""   # ISO-8601 UTC
    signal_event_id: str       = ""
    run_id:          str       = ""

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "event_version":   ORDER_FILLED_SCHEMA_VERSION,
            "order_id":        self.order_id,
            "exchange":        self.exchange,
            "symbol":          self.symbol,
            "side":            self.side,
            "fill_price":      self.fill_price,
            "size_pct":        self.size_pct,
            "filled_at":       self.filled_at,
            "signal_event_id": self.signal_event_id,
            "run_id":          self.run_id,
        })
        return base

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OrderFilledPayload":
        version = int(data.get("event_version", 1))
        if version != ORDER_FILLED_SCHEMA_VERSION:
            raise OrderSchemaVersionError(
                f"OrderFilledPayload schema v{version} incompatible "
                f"con v{ORDER_FILLED_SCHEMA_VERSION} esperada."
            )
        return cls(
            event_id        = str(data["event_id"]),
            event_version   = version,
            occurred_at     = str(data.get("occurred_at", "")),
            order_id        = str(data["order_id"]),
            exchange        = str(data["exchange"]),
            symbol          = str(data["symbol"]),
            side            = data["side"],
            fill_price      = float(data["fill_price"]),
            size_pct        = float(data["size_pct"]),
            filled_at       = str(data.get("filled_at", "")),
            signal_event_id = str(data.get("signal_event_id", "")),
            run_id          = str(data.get("run_id", "")),
        )


# ---------------------------------------------------------------------------
# OrderRejectedPayload — orden rechazada → orders.rejected
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class OrderRejectedPayload(BasePayload):
    """
    Orden rechazada por el executor o exchange.

    Publicado a: orders.rejected
    Consumido por: observability, alerting, analytics
    """
    order_id:        str       = ""
    exchange:        str       = ""
    symbol:          str       = ""
    side:            OrderSide = "buy"
    reason:          str       = ""
    signal_event_id: str       = ""
    run_id:          str       = ""

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "event_version":   ORDER_REJECTED_SCHEMA_VERSION,
            "order_id":        self.order_id,
            "exchange":        self.exchange,
            "symbol":          self.symbol,
            "side":            self.side,
            "reason":          self.reason,
            "signal_event_id": self.signal_event_id,
            "run_id":          self.run_id,
        })
        return base

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OrderRejectedPayload":
        version = int(data.get("event_version", 1))
        if version != ORDER_REJECTED_SCHEMA_VERSION:
            raise OrderSchemaVersionError(
                f"OrderRejectedPayload schema v{version} incompatible "
                f"con v{ORDER_REJECTED_SCHEMA_VERSION} esperada."
            )
        return cls(
            event_id        = str(data["event_id"]),
            event_version   = version,
            occurred_at     = str(data.get("occurred_at", "")),
            order_id        = str(data["order_id"]),
            exchange        = str(data["exchange"]),
            symbol          = str(data["symbol"]),
            side            = data["side"],
            reason          = str(data.get("reason", "")),
            signal_event_id = str(data.get("signal_event_id", "")),
            run_id          = str(data.get("run_id", "")),
        )


__all__ = [
    "ORDER_FILLED_SCHEMA_VERSION",
    "ORDER_REJECTED_SCHEMA_VERSION",
    "OrderSide",
    "OrderSchemaVersionError",
    "OrderFilledPayload",
    "OrderRejectedPayload",
]
