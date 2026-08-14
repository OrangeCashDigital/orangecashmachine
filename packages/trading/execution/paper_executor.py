# -*- coding: utf-8 -*-
"""
trading/execution/paper_executor.py
=====================================

PaperExecutor — executor de paper trading.

Responsabilidad: simular ejecución de órdenes sin dinero real.
Siempre acepta la orden y loguea el fill al precio de señal.
"""

from __future__ import annotations

from typing import Optional

from loguru import logger

from trading.execution.order import Order
from trading.execution.transport import OrderResult, OrderState, OrderStatus


class PaperExecutor:
    """
    Executor paper — acepta toda orden y loguea el fill.

    Implementa el protocolo OrderExecutor.
    No tiene estado — una instancia puede usarse para múltiples órdenes.

    S1: execute() retorna OrderResult con OrderState (fill al precio de señal,
    sin fees). El OMS propaga ese fill — paper asume fill exacto al precio de
    señal, sin slippage ni costes.

    F4a (ADR-0025): paper reporta la cantidad económicamente ejecutada como
    fill completo del tamaño pedido (qty = capital × size_pct / precio de
    señal). Es el "fill real" del simulador: la posición se asienta con esa
    cantidad (INV-01), nunca con None. El modelo paper = fill completo al
    precio de señal (limitación documentada: sin slippage ni parciales).

    F1 (Execution Quantity): si `Order.quantity` está fijado (SELL/cierre —
    el OMS lo deriva de la posición y lo clampa), paper llena EXACTAMENTE esa
    cantidad (nunca el sizing por capital). Sin quantity (BUY por
    asignación), sigue usando capital × size_pct / precio de señal.
    """

    def __init__(self, capital_usd: float) -> None:
        if capital_usd <= 0:
            raise ValueError(f"PaperExecutor: capital_usd debe ser > 0, recibido {capital_usd}")
        self._capital_usd = float(capital_usd)

    def execute(self, order: Order) -> OrderResult:
        price = order.signal.price
        # F1: cantidad REQUESTED explícita (SELL/cierre, derivada por el OMS de
        # la posición) → fill completo de esa cantidad. Sin ella (BUY por
        # asignación) → sizing por capital (modelo paper F4a).
        filled_qty: Optional[float]
        if order.quantity is not None:
            filled_qty = order.quantity
        else:
            filled_qty = (self._capital_usd * order.size_pct) / price if price > 0 else None
        logger.info(
            "[PAPER] EXECUTE {} {} {} @ {:.4f} | size={:.1%} qty={} requested={}",
            order.order_id,
            order.side.value.upper(),
            order.symbol,
            price,
            order.size_pct,
            filled_qty,
            order.quantity,
        )
        return OrderResult(
            accepted=True,
            state=OrderState(
                order_id=order.order_id,
                status=OrderStatus.FILLED,
                fill_price=price,
                filled_qty=filled_qty,  # fill completo del tamaño pedido (paper)
                fees=0.0,
            ),
        )

    def __repr__(self) -> str:
        return f"PaperExecutor(capital_usd={self._capital_usd:.0f})"
