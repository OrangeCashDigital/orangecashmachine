# -*- coding: utf-8 -*-
"""
tests/trading/test_oms_reconciliation.py
========================================

Tests para la reconciliación del OMS y condiciones de carrera en cancelaciones.
"""

from unittest.mock import MagicMock

import pytest
from trading.application.oms import OMS
from trading.domain.models.order import OrderStatus
from trading.ports.outbound.transport import ExchangeOrderStatus, OrderResult, OrderState, OrderTransport


class MockSignal:
    def __init__(self, symbol: str, quantity: float):
        self.symbol = symbol
        self.quantity = quantity
        self.direction = "buy"
        self.price = 50000.0

class ControllableTransport(OrderTransport):
    """Transporte stateful — nunca auto-fill, el test controla transiciones."""
    def __init__(self):
        self._states: dict[str, OrderState] = {}
        self._execute_state = OrderState(
            order_id="exchange-123", status=ExchangeOrderStatus.SUBMITTED
        )

    def force_execute_state(self, state: OrderState) -> None:
        self._execute_state = state

    def execute(self, request) -> OrderResult:
        return OrderResult(accepted=True, state=self._execute_state)

    def cancel(self, order_id: str) -> OrderState:
        return self._states.get(order_id, OrderState(order_id=order_id, status=ExchangeOrderStatus.SUBMITTED))

    def force_state(self, order_id: str, state: OrderState) -> None:
        self._states[order_id] = state

@pytest.fixture
def mocks():
    transport = ControllableTransport()
    risk = MagicMock()
    risk.validate.return_value = MagicMock(rejected=False, size_pct=0.1)
    
    portfolio = MagicMock()
    portfolio.snapshot.return_value = MagicMock(positions=[])
    
    return {
        "risk": risk,
        "executor": transport,
        "portfolio": portfolio
    }

def test_resolve_cancel_fill_prevails_over_cancel(mocks):
    # Setup
    oms = OMS(risk_manager=mocks["risk"], executor=mocks["executor"], portfolio=mocks["portfolio"])
    signal = MockSignal("BTC/USDT", 1.0)
    
    # 1. Submit and cancel
    order = oms.submit(signal)
    assert order is not None
    assert order.status == OrderStatus.SUBMITTED
    
    order_id = order.order_id
    oms.request_cancel(order_id)
    assert oms.get_order(order_id).status == OrderStatus.CANCELLING
    
    # 2. Reconcile with FILLED state
    filled_state = OrderState(
        order_id="exchange-123",
        status=ExchangeOrderStatus.FILLED,
        fill_price=50000.0,
        filled_qty=1.0,
        fees=0.0
    )
    
    # Simulamos que el transporte ya tiene el fill
    mocks["executor"].force_state("exchange-123", filled_state)
    
    status = oms.resolve_cancel(order_id, filled_state)
    
    # 3. Assert: FILLED gana sobre CANCELLING
    assert status == OrderStatus.FILLED
    assert oms.get_order(order_id).status == OrderStatus.FILLED

def test_resolve_cancel_confirmed_cancel(mocks):
    # Setup
    oms = OMS(risk_manager=mocks["risk"], executor=mocks["executor"], portfolio=mocks["portfolio"])
    signal = MockSignal("BTC/USDT", 1.0)
    
    order = oms.submit(signal)
    assert order is not None
    order_id = order.order_id
    oms.request_cancel(order_id)
    
    # Reconcile with CANCELLED state
    cancelled_state = OrderState(
        order_id="exchange-123",
        status=ExchangeOrderStatus.CANCELLED
    )
    mocks["executor"].force_state("exchange-123", cancelled_state)
    
    status = oms.resolve_cancel(order_id, cancelled_state)
    
    assert status == OrderStatus.CANCELLED
    assert oms.get_order(order_id).status == OrderStatus.CANCELLED

def test_resolve_cancel_invalid_state_stays_cancelling(mocks):
    # Setup
    oms = OMS(risk_manager=mocks["risk"], executor=mocks["executor"], portfolio=mocks["portfolio"])
    signal = MockSignal("BTC/USDT", 1.0)
    
    order = oms.submit(signal)
    assert order is not None
    order_id = order.order_id
    oms.request_cancel(order_id)
    
    # Reconcile with SUBMITTED (unknown state in terms of cancel)
    unknown_state = OrderState(
        order_id="exchange-123",
        status=ExchangeOrderStatus.SUBMITTED
    )
    
    status = oms.resolve_cancel(order_id, unknown_state)
    
    assert status == OrderStatus.CANCELLING


def test_submit_partial_fill_keeps_order_open(mocks):
    mocks["executor"].force_execute_state(
        OrderState(
            order_id="exchange-123",
            status=ExchangeOrderStatus.PARTIALLY_FILLED,
            fill_price=50000.0,
            filled_qty=0.4,
            fees=0.0,
        )
    )
    oms = OMS(risk_manager=mocks["risk"], executor=mocks["executor"], portfolio=mocks["portfolio"])

    order = oms.submit(MockSignal("BTC/USDT", 1.0))

    assert order is not None
    assert order.status == OrderStatus.SUBMITTED
    assert not order.is_terminal
    assert order.filled_qty == 0.4
    assert order.fill_price == 50000.0
    assert order.fill_timestamp is not None
    mocks["risk"].record_position.assert_not_called()


def test_resolve_cancel_partial_fill_stays_cancelling(mocks):
    oms = OMS(risk_manager=mocks["risk"], executor=mocks["executor"], portfolio=mocks["portfolio"])

    order = oms.submit(MockSignal("BTC/USDT", 1.0))
    assert order is not None
    order_id = order.order_id
    assert oms.request_cancel(order_id) is True

    partial_state = OrderState(
        order_id="exchange-123",
        status=ExchangeOrderStatus.PARTIALLY_FILLED,
        fill_price=50000.0,
        filled_qty=0.4,
        fees=0.0,
    )
    status = oms.resolve_cancel(order_id, partial_state)

    assert status == OrderStatus.CANCELLING
    stored = oms.get_order(order_id)
    assert stored is not None
    assert stored.status == OrderStatus.CANCELLING
    assert stored.filled_qty == 0.4
    mocks["risk"].record_position.assert_not_called()


def test_resolve_cancel_cancelled_after_partial_books_executed_qty(mocks):
    on_fill = MagicMock()
    oms = OMS(
        risk_manager=mocks["risk"],
        executor=mocks["executor"],
        portfolio=mocks["portfolio"],
        on_fill=on_fill,
    )

    order = oms.submit(MockSignal("BTC/USDT", 1.0))
    assert order is not None
    order_id = order.order_id
    assert oms.request_cancel(order_id) is True

    oms.resolve_cancel(
        order_id,
        OrderState(
            order_id="exchange-123",
            status=ExchangeOrderStatus.PARTIALLY_FILLED,
            fill_price=50000.0,
            filled_qty=0.4,
            fees=0.0,
        ),
    )
    mocks["risk"].record_position.assert_not_called()

    cancelled_state = OrderState(
        order_id="exchange-123",
        status=ExchangeOrderStatus.CANCELLED,
    )
    status = oms.resolve_cancel(order_id, cancelled_state)

    assert status == OrderStatus.CANCELLED
    stored = oms.get_order(order_id)
    assert stored is not None
    assert stored.status == OrderStatus.CANCELLED
    assert stored.filled_qty == 0.4
    mocks["risk"].record_position.assert_called_once_with("BTC/USDT", 0.4, 50000.0)
    on_fill.assert_called_once_with(stored)
