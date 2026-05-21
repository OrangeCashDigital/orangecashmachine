# -*- coding: utf-8 -*-
"""
shared/types/order_events.py
==============================

Eventos de dominio relacionados con el ciclo de vida de órdenes.

Frozen dataclasses — inmutables, sin dependencias externas.

DDD — Estos son eventos de dominio: representan hechos que ocurrieron,
no comandos. Son inmutables por definición (frozen=True).

Fail-Fast — Los factory methods validan ``side`` contra el Literal
permitido antes de construir el dataclass. Un side inválido indica
un bug en el caller — debe fallar en el punto de origen.

Principios: DDD · SSOT · Fail-Fast · KISS
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal

# SSOT: tipo canónico de lado de orden. Usado en el campo y en la validación.
OrderSide = Literal["buy", "sell"]
_VALID_ORDER_SIDES: frozenset[str] = frozenset({"buy", "sell"})


def _validate_order_side(side: str, caller: str) -> OrderSide:
    """
    Valida y retorna ``side`` como OrderSide.

    Fail-Fast: lanza ValueError si ``side`` no es "buy" | "sell".
    Centralizado aquí para que todos los factory methods sean DRY.

    Parameters
    ----------
    side   : Valor a validar.
    caller : Nombre del factory method (para mensaje de error contextual).

    Returns
    -------
    OrderSide validado.
    """
    normalized = side.lower().strip() if isinstance(side, str) else ""
    if normalized not in _VALID_ORDER_SIDES:
        raise ValueError(
            f"{caller}: side debe ser 'buy' | 'sell', "
            f"recibido: {side!r}. "
            "Verificar que el caller normalice el lado antes de crear el evento."
        )
    return normalized  # type: ignore[return-value]


@dataclass(frozen=True)
class OrderFilled:
    """
    Orden ejecutada exitosamente.

    Publicado por: OMS._fill()
    Consumido por: PortfolioService, TradeTracker, analytics

    Campos
    ------
    order_id    : identificador único de la orden
    symbol      : par de trading (e.g. "BTC/USDT")
    exchange    : exchange donde se ejecutó
    side        : "buy" | "sell"
    fill_price  : precio de ejecución real
    size_pct    : % del capital asignado ∈ (0, 1]
    filled_at   : timestamp UTC de ejecución
    """
    order_id:   str
    symbol:     str
    exchange:   str
    side:       OrderSide
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
        """
        Factory con timestamp UTC automático.

        Fail-Fast: valida ``side`` antes de construir.
        Acepta ``str`` para compatibilidad con callers externos (CCXT, etc.)
        que no conocen el Literal; normaliza internamente.
        """
        return cls(
            order_id   = order_id,
            symbol     = symbol,
            exchange   = exchange,
            side       = _validate_order_side(side, "OrderFilled.now"),
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
    side:        OrderSide
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
        """Factory con timestamp UTC automático. Fail-Fast: valida ``side``."""
        return cls(
            order_id    = order_id,
            symbol      = symbol,
            exchange    = exchange,
            side        = _validate_order_side(side, "OrderRejected.now"),
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
        """Factory con timestamp UTC automático."""
        return cls(
            order_id     = order_id,
            symbol       = symbol,
            cancelled_at = datetime.now(timezone.utc),
        )
