# -*- coding: utf-8 -*-
"""
trading/adapters/execution/paper_transport.py
=============================================

PaperTransport: implementación simulada (sin I/O) del puerto OrderTransport.
"""

from __future__ import annotations

from trading.ports.execution.transport import ExchangeOrderStatus, OrderState


class PaperTransport:
    """Transporte simulado — sigue el flujo orden→fill→estado sin I/O.

    Usado en el modo paper del LiveExecutor para validar el ciclo completo
    (orden→fill→reconciliación) con el mismo pipeline que live, pero sin
    dinero real. Siempre confirma FILLED al precio de señal.
    """

    def submit(
        self,
        symbol: str,
        side: str,
        qty: float,
        *,
        client_order_id: str,
    ) -> OrderState:
        return OrderState(
            order_id=client_order_id,
            status=ExchangeOrderStatus.FILLED,
            filled_qty=qty,
        )

    def fetch_state(self, exchange_order_id: str) -> OrderState:
        # Papel no mantiene estado persistente; cualquier id es del ciclo actual.
        return OrderState(order_id=exchange_order_id, status=ExchangeOrderStatus.FILLED)

    def cancel(self, symbol: str, exchange_order_id: str) -> OrderState:
        # Sin I/O: el papel confirma el cancel inmediatamente (ADR-0029).
        return OrderState(order_id=exchange_order_id, status=ExchangeOrderStatus.CANCELLED)

    def fetch_open_orders(self) -> list[OrderState]:
        # Papel no mantiene estado persistente entre ordenes (submit()
        # siempre resuelve a FILLED en el mismo ciclo) — nunca hay ordenes
        # abiertas que reconciliar.
        return []

    def close(self) -> None:
        return None

    def __repr__(self) -> str:
        return "PaperTransport()"
