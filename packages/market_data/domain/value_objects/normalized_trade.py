"""
market_data/domain/value_objects/normalized_trade.py
─────────────────────────────────────────────────────
Canonical normalized trade schema — SSOT for ALL market data consumers.

RULES:
  • No consumer or adapter imports cryptofeed types directly.
  • All exchange adapters MUST produce NormalizedTrade.
  • Use Decimal for price/amount — never float (financial precision).
  • This module has ZERO internal imports (no circular risk).
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Final, Literal

# ── side literals (mirrors cryptofeed.defines BUY/SELL = 'buy'/'sell') ──────
Side = Literal["buy", "sell"]

BUY: Final[str] = "buy"
SELL: Final[str] = "sell"


@dataclass(frozen=True, slots=True)
class NormalizedTrade:
    """Immutable canonical representation of a single trade execution.

    Produced by:  exchange adapters (market_data.adapters.inbound.*)
    Consumed by:  KafkaTradePublisher, research pipelines, paper-bot, tests.

    Attributes
    ----------
    exchange    : Lowercase exchange id  (e.g. 'bybit', 'kucoin').
    symbol      : Cryptofeed canonical symbol  (e.g. 'BTC-USDT-PERP').
    price       : Execution price — Decimal, never float.
    amount      : Base-asset quantity — Decimal, never float.
    side        : Taker direction: 'buy' or 'sell'.
    trade_id    : Exchange-assigned opaque identifier.
    timestamp   : Trade time reported by exchange (Unix epoch, float).
    received_at : Local ingestion wall-clock time (Unix epoch, float).
                  Excluded from equality checks (compare=False).
    """

    exchange: str
    symbol: str
    price: Decimal
    amount: Decimal
    side: Side
    trade_id: str
    timestamp: float
    received_at: float = field(default_factory=time.time, compare=False)

    # ── serialisation ────────────────────────────────────────────────────────

    def to_dict(self) -> dict[str, object]:
        """JSON-serialisable representation.  Decimal → str to avoid loss."""
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "price": str(self.price),
            "amount": str(self.amount),
            "side": self.side,
            "trade_id": self.trade_id,
            "timestamp": self.timestamp,
            "received_at": self.received_at,
        }

    def __repr__(self) -> str:
        return (
            f"NormalizedTrade({self.exchange} {self.symbol} "
            f"{self.side} {self.amount}@{self.price} "
            f"id={self.trade_id})"
        )
