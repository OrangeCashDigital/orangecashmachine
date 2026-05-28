# -*- coding: utf-8 -*-
"""
shared/kafka/schemas/oi.py
===========================

Wire payload para Open Interest snapshots de derivados.

Topología
---------
  OIProducer (WS Bybit) / REST polling (KuCoin Futures)
      → [oi.raw]  → OpenInterestPayload

  Consumers downstream:
      DerivativesProcessor → silver.open_interest

Routing key
-----------
  make_symbol_key(exchange, symbol) → b"bybit:BTC/USDT"

Nota sobre open_interest_value
-------------------------------
open_interest_value = open_interest_contracts × precio_mark
No todos los exchanges lo exponen directamente.
None si no está disponible.

Schema version history
----------------------
  v1 — campos base: contracts, value, mark_price.

Principios: SSOT · DDD · Kappa · Fail-Fast · KISS
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from shared.kafka.schemas._base import BasePayload

OPEN_INTEREST_SCHEMA_VERSION: int = 1


class OISchemaVersionError(ValueError):
    """Schema version incompatible en OpenInterestPayload."""


@dataclass(frozen=True)
class OpenInterestPayload(BasePayload):
    """
    Open interest snapshot para un contrato de derivados.

    Campos
    ------
    exchange                 : exchange de origen
    symbol                   : par normalizado (ej. "BTC/USDT")
    market_type              : "linear" | "inverse"
    timestamp_ms             : Unix epoch ms UTC del snapshot
    open_interest_contracts  : OI en contratos (str, Decimal serializado)
    open_interest_value      : OI en USD/quote (str). None si no disponible.
    mark_price               : precio mark en el momento del snapshot (str).
                               None si no disponible.
    """

    exchange: str = ""
    symbol: str = ""
    market_type: str = "linear"
    timestamp_ms: int = 0
    open_interest_contracts: str = "0"
    open_interest_value: Optional[str] = None
    mark_price: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update(
            {
                "event_version": OPEN_INTEREST_SCHEMA_VERSION,
                "exchange": self.exchange,
                "symbol": self.symbol,
                "market_type": self.market_type,
                "timestamp_ms": self.timestamp_ms,
                "open_interest_contracts": self.open_interest_contracts,
                "open_interest_value": self.open_interest_value,
                "mark_price": self.mark_price,
            }
        )
        return base

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OpenInterestPayload":
        version = int(data.get("event_version", 1))
        if version != OPEN_INTEREST_SCHEMA_VERSION:
            raise OISchemaVersionError(
                f"OpenInterestPayload schema v{version} incompatible con v{OPEN_INTEREST_SCHEMA_VERSION} esperada."
            )
        return cls(
            event_id=str(data["event_id"]),
            event_version=version,
            occurred_at=str(data.get("occurred_at", "")),
            exchange=str(data["exchange"]),
            symbol=str(data["symbol"]),
            market_type=str(data.get("market_type", "linear")),
            timestamp_ms=int(data["timestamp_ms"]),
            open_interest_contracts=str(data["open_interest_contracts"]),
            open_interest_value=data.get("open_interest_value"),
            mark_price=data.get("mark_price"),
        )


__all__ = [
    "OPEN_INTEREST_SCHEMA_VERSION",
    "OISchemaVersionError",
    "OpenInterestPayload",
]
