# -*- coding: utf-8 -*-
"""
shared/kafka/schemas/funding.py
================================

Wire payload para funding rates de derivados perpetuos.

Topología
---------
  FundingRateProducer (WS Bybit) / REST polling (KuCoin Futures)
      → [funding.raw]  → FundingRatePayload

  Consumers downstream:
      DerivativesProcessor → silver.funding_rate

Routing key
-----------
  make_symbol_key(exchange, symbol) → b"bybit:BTC/USDT"

Fuente por exchange
-------------------
  Bybit      : WebSocket (perpetuals) — canal fundingRate
  KuCoin Fut : REST polling cada ~8h (no tiene WS funding)

Schema version history
----------------------
  v1 — campos base: rate, next_funding_ms, interval_h.

Principios: SSOT · DDD · Kappa · Fail-Fast · KISS
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from shared.kafka.schemas._base import BasePayload

FUNDING_RATE_SCHEMA_VERSION: int = 1


class FundingSchemaVersionError(ValueError):
    """Schema version incompatible en FundingRatePayload."""


@dataclass(frozen=True)
class FundingRatePayload(BasePayload):
    """
    Funding rate snapshot para un perpetuo.

    Campos
    ------
    exchange         : exchange de origen
    symbol           : par normalizado (ej. "BTC/USDT")
    market_type      : "linear" | "inverse"
    timestamp_ms     : Unix epoch ms UTC del snapshot
    funding_rate     : tasa de funding como str (Decimal serializado).
                       Ejemplo: "0.0001" = 0.01%
    next_funding_ms  : Unix epoch ms UTC del próximo funding settlement.
                       None si el exchange no lo reporta.
    interval_h       : intervalo de funding en horas (típico: 8).
                       None si el exchange no lo reporta.
    predicted_rate   : tasa predicha para el próximo período (str).
                       None si el exchange no la expone.
    """

    exchange: str = ""
    symbol: str = ""
    market_type: str = "linear"
    timestamp_ms: int = 0
    funding_rate: str = "0"
    next_funding_ms: Optional[int] = None
    interval_h: Optional[int] = None
    predicted_rate: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update(
            {
                "event_version": FUNDING_RATE_SCHEMA_VERSION,
                "exchange": self.exchange,
                "symbol": self.symbol,
                "market_type": self.market_type,
                "timestamp_ms": self.timestamp_ms,
                "funding_rate": self.funding_rate,
                "next_funding_ms": self.next_funding_ms,
                "interval_h": self.interval_h,
                "predicted_rate": self.predicted_rate,
            }
        )
        return base

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FundingRatePayload":
        version = int(data.get("event_version", 1))
        if version != FUNDING_RATE_SCHEMA_VERSION:
            raise FundingSchemaVersionError(
                f"FundingRatePayload schema v{version} incompatible con v{FUNDING_RATE_SCHEMA_VERSION} esperada."
            )
        return cls(
            event_id=str(data["event_id"]),
            event_version=version,
            occurred_at=str(data.get("occurred_at", "")),
            exchange=str(data["exchange"]),
            symbol=str(data["symbol"]),
            market_type=str(data.get("market_type", "linear")),
            timestamp_ms=int(data["timestamp_ms"]),
            funding_rate=str(data["funding_rate"]),
            next_funding_ms=data.get("next_funding_ms"),
            interval_h=data.get("interval_h"),
            predicted_rate=data.get("predicted_rate"),
        )


__all__ = [
    "FUNDING_RATE_SCHEMA_VERSION",
    "FundingSchemaVersionError",
    "FundingRatePayload",
]
