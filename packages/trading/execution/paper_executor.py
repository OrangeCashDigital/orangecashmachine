# -*- coding: utf-8 -*-
"""
trading/execution/paper_executor.py
=====================================

PaperExecutor — executor de paper trading.

Responsabilidad: simular ejecución de órdenes sin dinero real.
Siempre acepta la orden y loguea el fill al precio de señal.
"""
from __future__ import annotations

from loguru import logger
from trading.execution.order import Order


class PaperExecutor:
    """
    Executor paper — acepta toda orden y loguea el fill.

    Implementa el protocolo OrderExecutor.
    No tiene estado — una instancia puede usarse para múltiples órdenes.
    """

    def execute(self, order: Order) -> bool:
        logger.info(
            "[PAPER] EXECUTE {} {} {} @ {:.4f} | size={:.1%}",
            order.order_id,
            order.side.value.upper(),
            order.symbol,
            order.signal.price,
            order.size_pct,
        )
        return True   # siempre acepta

    def __repr__(self) -> str:
        return "PaperExecutor()"
