# -*- coding: utf-8 -*-
"""
trading/execution/order.py
===========================

Modelos de dominio del OMS: Order, OrderSide, OrderStatus.

Ciclo de vida:
  PENDING → SUBMITTED → FILLED
                      ↘ REJECTED
                      ↘ CANCELLED

SafeOps
-------
- Transiciones de estado validadas — grafo explícito, no se puede retroceder.
- size_pct validado en construcción (0, 1].
- frozen=False para permitir actualizar status/fill (necesario para OMS).

Principios: SOLID · KISS · SafeOps
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Literal, Optional

from trading.strategies.base import Signal


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class OrderSide(str, Enum):
    BUY  = "buy"
    SELL = "sell"


class OrderStatus(str, Enum):
    PENDING   = "pending"
    SUBMITTED = "submitted"
    FILLED    = "filled"
    REJECTED  = "rejected"
    CANCELLED = "cancelled"


# Grafo de transiciones válidas — fail-fast ante cualquier violación
_VALID_TRANSITIONS: dict[OrderStatus, set[OrderStatus]] = {
    OrderStatus.PENDING:   {OrderStatus.SUBMITTED, OrderStatus.CANCELLED},
    OrderStatus.SUBMITTED: {OrderStatus.FILLED, OrderStatus.REJECTED, OrderStatus.CANCELLED},
    OrderStatus.FILLED:    set(),
    OrderStatus.REJECTED:  set(),
    OrderStatus.CANCELLED: set(),
}


# ---------------------------------------------------------------------------
# Order
# ---------------------------------------------------------------------------

@dataclass
class Order:
    """
    Orden de trading — unidad atómica del OMS.

    Campos de identidad (inmutables en práctica):
      order_id, symbol, side, size_pct, signal, created_at

    Campos de estado (mutables via transition()):
      status, fill_price, fill_timestamp, reject_reason

    Nota sobre order_id
    -------------------
    Usa los primeros 8 caracteres de un UUID4. La probabilidad de
    colisión es despreciable para volúmenes de paper trading (<10^6
    órdenes). Para producción en volumen alto, usar uuid4() completo.
    """

    # Identidad
    symbol:     str
    side:       OrderSide
    size_pct:   float           # % del capital, rango (0.0, 1.0]
    signal:     Signal
    order_id:   str             = field(default_factory=lambda: str(uuid.uuid4())[:8])
    created_at: datetime        = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    # Estado — mutable via transition()
    status:         OrderStatus       = OrderStatus.PENDING
    fill_price:     Optional[float]   = None
    fill_timestamp: Optional[datetime] = None
    reject_reason:  Optional[str]     = None

    def __post_init__(self) -> None:
        """Validación de invariantes en construcción."""
        if not (0.0 < self.size_pct <= 1.0):
            raise ValueError(
                f"Order.size_pct debe estar en (0, 1], recibido: {self.size_pct}"
            )

    # ------------------------------------------------------------------
    # State machine
    # ------------------------------------------------------------------

    def transition(self, new_status: OrderStatus, **kwargs) -> None:
        """
        Avanza el estado validando la transición contra el grafo.

        kwargs aceptados:
          fill_price      (float)    — para FILLED
          fill_timestamp  (datetime) — para FILLED
          reject_reason   (str)      — para REJECTED

        Lanza ValueError si la transición no es válida.
        """
        allowed = _VALID_TRANSITIONS.get(self.status, set())
        if new_status not in allowed:
            raise ValueError(
                f"Order {self.order_id}: transición inválida "
                f"{self.status.value} → {new_status.value}. "
                f"Permitidas: {[s.value for s in allowed]}"
            )
        self.status = new_status

        if new_status == OrderStatus.FILLED:
            self.fill_price     = kwargs.get("fill_price", self.signal.price)
            self.fill_timestamp = kwargs.get(
                "fill_timestamp", datetime.now(timezone.utc)
            )
        elif new_status == OrderStatus.REJECTED:
            self.reject_reason = kwargs.get("reject_reason", "unknown")

    # ------------------------------------------------------------------
    # Convenience properties
    # ------------------------------------------------------------------

    @property
    def is_open(self) -> bool:
        return self.status in (OrderStatus.PENDING, OrderStatus.SUBMITTED)

    @property
    def is_terminal(self) -> bool:
        return self.status in (
            OrderStatus.FILLED, OrderStatus.REJECTED, OrderStatus.CANCELLED
        )

    @property
    def pnl_pct(self) -> Optional[float]:
        """
        P&L aproximado de la orden si está filled.

        Assumption: sistema opera posiciones long únicamente.
          - BUY  → apertura de posición, P&L no calculable aquí
                   (requiere precio de cierre futuro).
          - SELL → cierre de posición; compara fill_price contra
                   signal.price (precio de entrada implícito).

        Para soporte de short selling, añadir position_side al Order.
        """
        if self.status != OrderStatus.FILLED or self.fill_price is None:
            return None
        if self.side == OrderSide.SELL:
            entry = self.signal.price
            return (self.fill_price - entry) / entry if entry > 0 else None
        return None

    def __repr__(self) -> str:
        return (
            f"Order(id={self.order_id!r}, {self.side.value} {self.symbol}"
            f" size={self.size_pct:.1%} status={self.status.value})"
        )
