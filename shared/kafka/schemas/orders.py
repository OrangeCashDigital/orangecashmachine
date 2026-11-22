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
from typing import Any, Dict, Optional

from shared.kafka.schemas._base import BasePayload, OrderSide, SchemaVersionError

# Alias de compatibilidad — el canónico es SchemaVersionError (_base.py).
OrderSchemaVersionError = SchemaVersionError


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
    filled_qty : cantidad ejecutada real (Order.filled_qty del exchange)
    fee_amount_usd : coste de comisiones en USD; None = UNKNOWN (ADR-0026,
                     UNKNOWN ≠ 0 — no se asume cero)
    fee_currency   : moneda de las comisiones ("" si no disponible)
    fee_status     : known | unknown | provisional | final (ADR-0026)
    timeframe      : timeframe de la señal que originó la orden
    closed_qty     : cantidad cerrada por este fill SELL (0.0 en BUY)
    avg_entry_price: WAC antes del cierre (None en BUY / precio UNKNOWN)
    remaining_qty  : cantidad que queda abierta tras el cierre
    filled_at  : ISO-8601 UTC de ejecución
    signal_event_id : event_id del ApprovedSignalPayload que originó la orden
    run_id     : correlación con el run

    Versionado
    ----------
    Los campos económicos son un cambio ADITIVO (defaults seguros): el
    SCHEMA_VERSION permanece en 1 según la política de ``_base.py``
    (additive → mismo SCHEMA_VERSION). Un payload v1 antiguo sin estos
    campos sigue siendo válido; un consumer no puede inferir cantidades
    que el productor no emitió.
    """

    order_id: str = ""
    exchange: str = ""
    symbol: str = ""
    side: OrderSide = "buy"
    fill_price: float = 0.0
    size_pct: float = 0.0
    filled_qty: float = 0.0
    fee_amount_usd: Optional[float] = None
    fee_currency: str = ""
    fee_status: str = "unknown"
    timeframe: str = ""
    closed_qty: float = 0.0
    avg_entry_price: Optional[float] = None
    remaining_qty: float = 0.0
    filled_at: str = ""  # ISO-8601 UTC
    signal_event_id: str = ""
    run_id: str = ""

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update(
            {
                "order_id": self.order_id,
                "exchange": self.exchange,
                "symbol": self.symbol,
                "side": self.side,
                "fill_price": self.fill_price,
                "size_pct": self.size_pct,
                "filled_qty": self.filled_qty,
                "fee_amount_usd": self.fee_amount_usd,
                "fee_currency": self.fee_currency,
                "fee_status": self.fee_status,
                "timeframe": self.timeframe,
                "closed_qty": self.closed_qty,
                "avg_entry_price": self.avg_entry_price,
                "remaining_qty": self.remaining_qty,
                "filled_at": self.filled_at,
                "signal_event_id": self.signal_event_id,
                "run_id": self.run_id,
            }
        )
        return base

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OrderFilledPayload":
        version = int(data.get("event_version", 1))
        if version != cls.SCHEMA_VERSION:
            raise SchemaVersionError(
                f"OrderFilledPayload schema v{version} incompatible con v{cls.SCHEMA_VERSION} esperada."
            )
        fee_amount = data.get("fee_amount_usd")
        avg_entry = data.get("avg_entry_price")
        return cls(
            event_id=str(data["event_id"]),
            occurred_at=str(data.get("occurred_at", "")),
            order_id=str(data["order_id"]),
            exchange=str(data["exchange"]),
            symbol=str(data["symbol"]),
            side=data["side"],
            fill_price=float(data["fill_price"]),
            size_pct=float(data["size_pct"]),
            filled_qty=float(data.get("filled_qty", 0.0)),
            fee_amount_usd=None if fee_amount is None else float(fee_amount),
            fee_currency=str(data.get("fee_currency", "")),
            fee_status=str(data.get("fee_status", "unknown")),
            timeframe=str(data.get("timeframe", "")),
            closed_qty=float(data.get("closed_qty", 0.0)),
            avg_entry_price=None if avg_entry is None else float(avg_entry),
            remaining_qty=float(data.get("remaining_qty", 0.0)),
            filled_at=str(data.get("filled_at", "")),
            signal_event_id=str(data.get("signal_event_id", "")),
            run_id=str(data.get("run_id", "")),
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

    order_id: str = ""
    exchange: str = ""
    symbol: str = ""
    side: OrderSide = "buy"
    reason: str = ""
    signal_event_id: str = ""
    run_id: str = ""

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update(
            {
                "order_id": self.order_id,
                "exchange": self.exchange,
                "symbol": self.symbol,
                "side": self.side,
                "reason": self.reason,
                "signal_event_id": self.signal_event_id,
                "run_id": self.run_id,
            }
        )
        return base

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OrderRejectedPayload":
        version = int(data.get("event_version", 1))
        if version != cls.SCHEMA_VERSION:
            raise SchemaVersionError(
                f"OrderRejectedPayload schema v{version} incompatible con v{cls.SCHEMA_VERSION} esperada."
            )
        return cls(
            event_id=str(data["event_id"]),
            occurred_at=str(data.get("occurred_at", "")),
            order_id=str(data["order_id"]),
            exchange=str(data["exchange"]),
            symbol=str(data["symbol"]),
            side=data["side"],
            reason=str(data.get("reason", "")),
            signal_event_id=str(data.get("signal_event_id", "")),
            run_id=str(data.get("run_id", "")),
        )


# Alias de compatibilidad — SSOT de versión: SCHEMA_VERSION de cada clase.
ORDER_FILLED_SCHEMA_VERSION: int = OrderFilledPayload.SCHEMA_VERSION
ORDER_REJECTED_SCHEMA_VERSION: int = OrderRejectedPayload.SCHEMA_VERSION


__all__ = [
    "ORDER_FILLED_SCHEMA_VERSION",
    "ORDER_REJECTED_SCHEMA_VERSION",
    "OrderSide",
    "OrderSchemaVersionError",
    "OrderFilledPayload",
    "OrderRejectedPayload",
]
