# -*- coding: utf-8 -*-
"""
domain/events/order_events.py
===============================

Eventos de dominio relacionados con el ciclo de vida de órdenes.

Frozen dataclasses — inmutables, sin dependencias externas.
Principios: DDD · SSOT · Fail-Fast · KISS
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True)
class OrderFilled:
    """
    Orden ejecutada exitosamente.

    Publicado por: OMS._fill()
    Consumido por: PortfolioService, TradeTracker, analytics

    Campos
    ------
    order_id    : identificador único de la orden
    symbol      : par de trading  (e.g. "BTC/USDT")
    exchange    : exchange donde se ejecutó
    side        : "buy" | "sell"
    fill_price  : precio de ejecución real
    size_pct    : % del capital asignado ∈ (0, 1]
    filled_at   : timestamp UTC de ejecución
    """
    order_id:   str
    symbol:     str
    exchange:   str
    side:       str        # "buy" | "sell"
    fill_price: float
    size_pct:   float
    filled_at:  datetime

    @classmethod
    def now(
        cls,
        order_id:   str,
        symbol:     str,
        exchange:   str,
        side:       str,
        fill_price: float,
        size_pct:   float,
    ) -> "OrderFilled":
        """Factory con timestamp UTC automático."""
        return cls(
            order_id   = order_id,
            symbol     = symbol,
            exchange   = exchange,
            side       = side,
            fill_price = fill_price,
            size_pct   = size_pct,
            filled_at  = datetime.now(timezone.utc),
        )

    def __str__(self) -> str:
        return (
            f"OrderFilled({self.side.upper()} {self.symbol}"
            f" @ {self.fill_price:.4f} size={self.size_pct:.1%}"
            f" id={self.order_id})"
        )


@dataclass(frozen=True)
class OrderRejected:
    """
    Orden rechazada por riesgo o executor.

    Publicado por: OMS._reject()
    Consumido por: observability, alerting
    """
    order_id:    str
    symbol:      str
    exchange:    str
    side:        str
    reason:      str
    rejected_at: datetime

    @classmethod
    def now(
        cls,
        order_id: str,
        symbol:   str,
        exchange: str,
        side:     str,
        reason:   str,
    ) -> "OrderRejected":
        return cls(
            order_id    = order_id,
            symbol      = symbol,
            exchange    = exchange,
            side        = side,
            reason      = reason,
            rejected_at = datetime.now(timezone.utc),
        )


@dataclass(frozen=True)
class OrderCancelled:
    """
    Orden cancelada antes de ejecutarse.

    Publicado por: OMS.cancel()
    """
    order_id:     str
    symbol:       str
    cancelled_at: datetime

    @classmethod
    def now(cls, order_id: str, symbol: str) -> "OrderCancelled":
        return cls(
            order_id     = order_id,
            symbol       = symbol,
            cancelled_at = datetime.now(timezone.utc),
        )
