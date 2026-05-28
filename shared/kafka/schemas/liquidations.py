# -*- coding: utf-8 -*-
"""
shared/kafka/schemas/liquidations.py
======================================

Wire payload para liquidaciones forzadas de derivados.

Topología
---------
  WsLiquidationsStream (cryptofeed LIQUIDATIONS channel)
      → [liquidations.raw]  → LiquidationPayload

  Consumers downstream:
      LiquidationsProcessor → silver.liquidations
      RiskMonitor → alertas de cascada de liquidaciones

Routing key
-----------
  make_symbol_key(exchange, symbol) → b"bybit:BTC/USDT"

Nota sobre side
---------------
"buy"  = long liquidado (posición long forzada a cerrar)
"sell" = short liquidado (posición short forzada a cerrar)

Schema version history
----------------------
  v1 — campos base: price, quantity, side, order_type.

Principios: SSOT · DDD · Kappa · Fail-Fast · KISS
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Literal, Optional

from shared.kafka.schemas._base import BasePayload

LIQUIDATION_SCHEMA_VERSION: int = 1

LiquidationSide = Literal["buy", "sell"]
_VALID_SIDES: frozenset[str] = frozenset({"buy", "sell"})


class LiquidationSchemaVersionError(ValueError):
    """Schema version incompatible en LiquidationPayload."""


@dataclass(frozen=True)
class LiquidationPayload(BasePayload):
    """
    Evento de liquidación forzada en un contrato de derivados.

    Campos
    ------
    exchange     : exchange de origen
    symbol       : par normalizado (ej. "BTC/USDT")
    market_type  : "linear" | "inverse"
    timestamp_ms : Unix epoch ms UTC del evento
    price        : precio de liquidación (str, Decimal serializado)
    quantity     : cantidad liquidada en contratos (str)
    quantity_usd : valor USD aproximado liquidado (str). None si no disponible.
    side         : "buy" = long liquidado | "sell" = short liquidado
    order_type   : "market" (casi siempre) | "limit" (algunos exchanges)
    """

    exchange: str = ""
    symbol: str = ""
    market_type: str = "linear"
    timestamp_ms: int = 0
    price: str = "0"
    quantity: str = "0"
    quantity_usd: Optional[str] = None
    side: LiquidationSide = "buy"
    order_type: str = "market"

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update(
            {
                "event_version": LIQUIDATION_SCHEMA_VERSION,
                "exchange": self.exchange,
                "symbol": self.symbol,
                "market_type": self.market_type,
                "timestamp_ms": self.timestamp_ms,
                "price": self.price,
                "quantity": self.quantity,
                "quantity_usd": self.quantity_usd,
                "side": self.side,
                "order_type": self.order_type,
            }
        )
        return base

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LiquidationPayload":
        version = int(data.get("event_version", 1))
        if version != LIQUIDATION_SCHEMA_VERSION:
            raise LiquidationSchemaVersionError(
                f"LiquidationPayload schema v{version} incompatible con v{LIQUIDATION_SCHEMA_VERSION} esperada."
            )
        side = data.get("side", "buy")
        if side not in _VALID_SIDES:
            raise LiquidationSchemaVersionError(f"LiquidationPayload.side desconocido: {side!r}.")
        return cls(
            event_id=str(data["event_id"]),
            event_version=version,
            occurred_at=str(data.get("occurred_at", "")),
            exchange=str(data["exchange"]),
            symbol=str(data["symbol"]),
            market_type=str(data.get("market_type", "linear")),
            timestamp_ms=int(data["timestamp_ms"]),
            price=str(data["price"]),
            quantity=str(data["quantity"]),
            quantity_usd=data.get("quantity_usd"),
            side=side,
            order_type=str(data.get("order_type", "market")),
        )


__all__ = [
    "LIQUIDATION_SCHEMA_VERSION",
    "LiquidationSide",
    "LiquidationSchemaVersionError",
    "LiquidationPayload",
]
