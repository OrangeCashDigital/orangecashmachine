# -*- coding: utf-8 -*-
"""
trading/execution/order.py
===========================

Modelos de dominio del OMS: Order, OrderSide, OrderStatus.

Ciclo de vida:
  PENDING → SUBMITTED → FILLED
                      ↘ REJECTED
                      ↘ CANCELLING → CANCELLED   (confirmado por exchange)
                                   → FILLED       (fill prevalece — vía OMS._fill)
                                   → REJECTED     (cancel rechazado / not found concluyente)

CANCELLING (ADR-0029 / B-MD-008)
--------------------------------
Estado transitorio: el estado local NUNCA decreta CANCELLED sin confirmación
del exchange. CANCELLED y FILLED siguen siendo terminales (no se relaja el
grafo). Si un fill real llega durante CANCELLING, el flujo OMS._fill existente
lo aplica y la orden termina en FILLED — el fill SIEMPRE prevalece.

P&L (criterio G / S1)
---------------------
Order NO calcula P&L. La vía paralela ``pnl_pct`` que comparaba
``fill_price`` contra ``signal.price`` fue eliminada (S1) — usaba el precio
de señal como entrada implícita y divergía del P&L real. El P&L realizado
se calcula únicamente en TradeTracker/TradeRecord desde los fills reales
(entry_order.fill_price vs exit fill_price), SSOT del dominio.

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
from typing import TYPE_CHECKING, Optional

from shared.types.signal import (
    Signal,
)  # DIP — order depende de domain, no de strategies

if TYPE_CHECKING:
    from trading.execution.settlement import Settlement

# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class OrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderStatus(str, Enum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    CANCELLING = "cancelling"  # transitorio — resolución CANCEL/FILL (ADR-0029)
    FILLED = "filled"
    REJECTED = "rejected"
    CANCELLED = "cancelled"


# Grafo de transiciones válidas — fail-fast ante cualquier violación
_VALID_TRANSITIONS: dict[OrderStatus, set[OrderStatus]] = {
    OrderStatus.PENDING: {OrderStatus.SUBMITTED, OrderStatus.CANCELLED},
    OrderStatus.SUBMITTED: {
        OrderStatus.FILLED,
        OrderStatus.REJECTED,
        OrderStatus.CANCELLING,
        OrderStatus.CANCELLED,
    },
    OrderStatus.CANCELLING: {
        OrderStatus.FILLED,
        OrderStatus.REJECTED,
        OrderStatus.CANCELLED,
    },
    OrderStatus.FILLED: set(),
    OrderStatus.REJECTED: set(),
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

    Nota sobre order_id (B-16 / H-08)
    ----------------------------------
    Usa el UUID4 completo. Es la clave de posición en PortfolioService
    (PositionStore — InMemory o Redis) y la clave del mapa _open del OMS.
    Un order_id truncado (antes [:8], 32 bits) eleva la probabilidad de
    colisión a volumen alto: colisión en el store = overwrite silencioso
    de una posición abierta por otra (riesgo de portfolio). UUID completo
    de 36 chars elimina el riesgo de colisión práctica.
    """

    # Identidad
    symbol: str
    side: OrderSide
    size_pct: float  # % del capital, rango (0, 1]
    signal: Signal
    order_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    # Cantidad TARGET pedida al exchange (F1): para SELL/cierre el OMS la
    # deriva de la cantidad económica disponible (Position.quantity) y la
    # clampa — NUNCA se pide más de lo disponible. None = sizing por capital
    # (size_pct × capital / precio). Es REQUESTED, nunca la ejecutada (la
    # ejecutada es filled_qty, del fill real del exchange).
    quantity: Optional[float] = None

    # Estado — mutable via transition()
    status: OrderStatus = OrderStatus.PENDING
    fill_price: Optional[float] = None
    fill_timestamp: Optional[datetime] = None
    reject_reason: Optional[str] = None
    filled_qty: Optional[float] = None
    fees: Optional[float] = None
    # ID real de la orden en el exchange (Bybit/CCXT) — distinto de order_id
    # (UUID interno de OCM). Poblado por OMS.submit() a partir del OrderState
    # devuelto por el executor, en cuanto está disponible (aunque la orden
    # aún no esté FILLED). Prerrequisito para manage_open_orders (B-MD-008
    # paso 5, ADR-0029): sin este ID, transport.cancel()/fetch_state() no
    # tienen con qué operar sobre una orden SUBMITTED/CANCELLING.
    exchange_order_id: Optional[str] = None
    # Settlement canónico calculado por OMS._fill al llenar una orden SELL.
    # Es la única vía de P&L realizada; downstream consumidores (TradeTracker,
    # TradeRecord, RiskManager, PerformanceEngine) deben usarlo y no recalcular
    # P&L de manera distinta. None cuando el fill no produce cierre económico
    # (BUY, SELL sin entrada WAC, precio UNKNOWN, etc.).
    settlement: Optional["Settlement"] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        """Validación de invariantes en construcción."""
        if not (0.0 < self.size_pct <= 1.0):
            raise ValueError(f"Order.size_pct debe estar en (0, 1], recibido: {self.size_pct}")
        if self.quantity is not None and self.quantity <= 0.0:
            raise ValueError(f"Order.quantity debe ser > 0 cuando se especifica, recibido: {self.quantity}")

    # ------------------------------------------------------------------
    # State machine
    # ------------------------------------------------------------------

    def transition(self, new_status: OrderStatus, **kwargs) -> None:
        """
        Avanza el estado validando la transición contra el grafo.

        kwargs aceptados:
          fill_price      (float)    — para FILLED
          fill_timestamp  (datetime) — para FILLED
          filled_qty      (float)    — para FILLED (cantidad ejecutada real)
          fees            (float)    — para FILLED (coste del fill en USD)
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
            # F1: fill_price se fija SOLO cuando el caller lo provee. Si el
            # fill carece de precio económico (state sin fill_price), queda
            # None (UNKNOWN) — NUNCA se sustituye por signal.price (INV-10,
            # ADR-0026). El caller (OMS._fill) aplica la política UNKNOWN.
            self.fill_price = kwargs.get("fill_price")
            self.fill_timestamp = kwargs.get("fill_timestamp", datetime.now(timezone.utc))
            filled_qty = kwargs.get("filled_qty")
            if filled_qty is not None:
                self.filled_qty = filled_qty
            fees = kwargs.get("fees")
            if fees is not None:
                self.fees = fees
        elif new_status == OrderStatus.REJECTED:
            self.reject_reason = kwargs.get("reject_reason", "unknown")

    # ------------------------------------------------------------------
    # Convenience properties
    # ------------------------------------------------------------------

    @property
    def is_open(self) -> bool:
        # CANCELLING es transitorio y visible: la orden sigue viva hasta la
        # resolución determinista CANCEL/FILL (ADR-0029).
        return self.status in (
            OrderStatus.PENDING,
            OrderStatus.SUBMITTED,
            OrderStatus.CANCELLING,
        )

    @property
    def is_terminal(self) -> bool:
        return self.status in (
            OrderStatus.FILLED,
            OrderStatus.REJECTED,
            OrderStatus.CANCELLED,
        )

    def __repr__(self) -> str:
        qty = f" qty={self.quantity}" if self.quantity is not None else ""
        return (
            f"Order(id={self.order_id!r}, {self.side.value} {self.symbol}"
            f" size={self.size_pct:.1%}{qty} status={self.status.value})"
        )
