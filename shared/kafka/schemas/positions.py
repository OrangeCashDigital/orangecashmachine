# -*- coding: utf-8 -*-
"""
shared/kafka/schemas/positions.py
===================================

Wire payloads para el ciclo de vida de posiciones.

Topología
---------
  PortfolioConsumer → [positions.opened] → PositionOpenedPayload
  PortfolioConsumer → [positions.closed] → PositionClosedPayload

Separación dominio / wire
--------------------------
  shared/types/position_events.py → PositionOpened, PositionClosed (domain)
  shared/kafka/schemas/positions.py → PositionOpenedPayload, PositionClosedPayload (wire)

Compaction policy
-----------------
  positions.opened → delete (todas relevantes, no compactar)
  positions.closed → delete, retention 90 días (base para analytics PnL)

Routing key
-----------
  "{exchange}:{symbol}" — estado de posición por símbolo.

Schema version history
----------------------
  v1 (actual) — campos base del ciclo de vida de posiciones.

Principios: SSOT · DDD · Fail-Fast · KISS
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Literal

from shared.kafka.schemas._base import BasePayload

POSITION_OPENED_SCHEMA_VERSION: int = 1
POSITION_CLOSED_SCHEMA_VERSION: int = 1

PositionSide = Literal["long", "short"]


class PositionSchemaVersionError(ValueError):
    """Schema version incompatible en PositionPayload.from_dict()."""


# ---------------------------------------------------------------------------
# PositionOpenedPayload → positions.opened
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PositionOpenedPayload(BasePayload):
    """
    Posición abierta tras un OrderFilled de lado BUY.

    Publicado a: positions.opened
    Consumido por: risk/ (exposure tracking), observability, alerting

    Campos
    ------
    order_id    : order_id del OrderFilledPayload que abrió la posición
    exchange    : exchange de la posición
    symbol      : par de trading
    side        : "long" | "short"
    entry_price : precio de entrada real (fill_price del order)
    size_pct    : % del capital asignado
    opened_at   : ISO-8601 UTC de apertura
    run_id      : correlación con el run
    """

    order_id: str = ""
    exchange: str = ""
    symbol: str = ""
    side: PositionSide = "long"
    entry_price: float = 0.0
    size_pct: float = 0.0
    opened_at: str = ""  # ISO-8601 UTC
    run_id: str = ""

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update(
            {
                "event_version": POSITION_OPENED_SCHEMA_VERSION,
                "order_id": self.order_id,
                "exchange": self.exchange,
                "symbol": self.symbol,
                "side": self.side,
                "entry_price": self.entry_price,
                "size_pct": self.size_pct,
                "opened_at": self.opened_at,
                "run_id": self.run_id,
            }
        )
        return base

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PositionOpenedPayload":
        version = int(data.get("event_version", 1))
        if version != POSITION_OPENED_SCHEMA_VERSION:
            raise PositionSchemaVersionError(
                f"PositionOpenedPayload schema v{version} incompatible con v{POSITION_OPENED_SCHEMA_VERSION} esperada."
            )
        return cls(
            event_id=str(data["event_id"]),
            event_version=version,
            occurred_at=str(data.get("occurred_at", "")),
            order_id=str(data["order_id"]),
            exchange=str(data["exchange"]),
            symbol=str(data["symbol"]),
            side=data.get("side", "long"),
            entry_price=float(data["entry_price"]),
            size_pct=float(data["size_pct"]),
            opened_at=str(data.get("opened_at", "")),
            run_id=str(data.get("run_id", "")),
        )


# ---------------------------------------------------------------------------
# PositionClosedPayload → positions.closed
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PositionClosedPayload(BasePayload):
    """
    Posición cerrada tras un OrderFilled de lado SELL.

    Publicado a: positions.closed
    Consumido por: analytics, TradeTracker, observability, research

    Campos clave
    ------------
    pnl_pct     : PnL realizado en % ∈ (-1, +∞)
    opened_at   : ISO-8601 UTC de apertura (para calcular hold time)
    closed_at   : ISO-8601 UTC de cierre
    """

    order_id: str = ""
    exchange: str = ""
    symbol: str = ""
    side: PositionSide = "long"
    entry_price: float = 0.0
    exit_price: float = 0.0
    size_pct: float = 0.0
    pnl_pct: float = 0.0
    opened_at: str = ""  # ISO-8601 UTC
    closed_at: str = ""  # ISO-8601 UTC
    run_id: str = ""

    @property
    def is_winner(self) -> bool:
        return self.pnl_pct > 0.0

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update(
            {
                "event_version": POSITION_CLOSED_SCHEMA_VERSION,
                "order_id": self.order_id,
                "exchange": self.exchange,
                "symbol": self.symbol,
                "side": self.side,
                "entry_price": self.entry_price,
                "exit_price": self.exit_price,
                "size_pct": self.size_pct,
                "pnl_pct": self.pnl_pct,
                "opened_at": self.opened_at,
                "closed_at": self.closed_at,
                "run_id": self.run_id,
            }
        )
        return base

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PositionClosedPayload":
        version = int(data.get("event_version", 1))
        if version != POSITION_CLOSED_SCHEMA_VERSION:
            raise PositionSchemaVersionError(
                f"PositionClosedPayload schema v{version} incompatible con v{POSITION_CLOSED_SCHEMA_VERSION} esperada."
            )
        return cls(
            event_id=str(data["event_id"]),
            event_version=version,
            occurred_at=str(data.get("occurred_at", "")),
            order_id=str(data["order_id"]),
            exchange=str(data["exchange"]),
            symbol=str(data["symbol"]),
            side=data.get("side", "long"),
            entry_price=float(data["entry_price"]),
            exit_price=float(data["exit_price"]),
            size_pct=float(data["size_pct"]),
            pnl_pct=float(data.get("pnl_pct", 0.0)),
            opened_at=str(data.get("opened_at", "")),
            closed_at=str(data.get("closed_at", "")),
            run_id=str(data.get("run_id", "")),
        )


__all__ = [
    "POSITION_OPENED_SCHEMA_VERSION",
    "POSITION_CLOSED_SCHEMA_VERSION",
    "PositionSide",
    "PositionSchemaVersionError",
    "PositionOpenedPayload",
    "PositionClosedPayload",
]
