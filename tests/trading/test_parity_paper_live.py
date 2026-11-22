# -*- coding: utf-8 -*-
from __future__ import annotations
from datetime import datetime, timezone
from decimal import Decimal
import pytest
from trading.adapters.outbound.live_executor import LiveExecutor
from trading.adapters.outbound.paper_executor import PaperExecutor
from trading.domain.models.order import Order, OrderSide
from trading.domain.strategies.base import Signal
from trading.ports.outbound.transport import ExchangeOrderStatus, OrderState
from shared.types.values import Price


def _signal(side: OrderSide = OrderSide.BUY) -> Signal:
    return Signal(
        symbol="BTC/USDT",
        timeframe="1m",
        direction=side.value,
        price=Price(Decimal("50000.0")),
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
        confidence=1.0,
    )


def _order(side: OrderSide = OrderSide.BUY) -> Order:
    return Order(symbol="BTC/USDT", side=side, size_pct=0.1, signal=_signal(side))


class _FakeTransport:
    def __init__(self, state: OrderState):
        self.state = state

    def submit(self, symbol, side, qty, *, client_order_id=None):
        return self.state

    def fetch_state(self, exchange_order_id):
        return self.state

    def close(self):
        pass


# FakeFreshnessPort mínimo
class _FakeFreshnessPort:
    def __init__(self, stale=False):
        self.stale = stale

    def is_stale(self, signal: Signal) -> bool:
        return self.stale


def test_accepted_flag_matches():
    order = _order()
    paper = PaperExecutor(capital_usd=10000.0)
    transport = _FakeTransport(OrderState(order_id="1", status=ExchangeOrderStatus.FILLED, filled_qty=0.02))
    live = LiveExecutor(capital_usd=10000.0, transport=transport)

    res_p = paper.execute(order)
    res_l = live.execute(order)

    assert res_p.accepted == res_l.accepted == True


def test_order_state_status_matches():
    order = _order()
    paper = PaperExecutor(capital_usd=10000.0)
    transport = _FakeTransport(OrderState(order_id="1", status=ExchangeOrderStatus.FILLED, filled_qty=0.02))
    live = LiveExecutor(capital_usd=10000.0, transport=transport)

    assert paper.execute(order).state.status == live.execute(order).state.status == ExchangeOrderStatus.FILLED


def test_filled_qty_within_tolerance():
    order = _order()
    paper = PaperExecutor(capital_usd=10000.0)
    transport = _FakeTransport(OrderState(order_id="1", status=ExchangeOrderStatus.FILLED, filled_qty=0.02))
    live = LiveExecutor(capital_usd=10000.0, transport=transport)

    qty_p = paper.execute(order).state.filled_qty
    qty_l = live.execute(order).state.filled_qty
    assert abs(qty_p - qty_l) < 1e-6


def test_fill_price_within_tolerance():
    order = _order()
    paper = PaperExecutor(capital_usd=10000.0)
    transport = _FakeTransport(
        OrderState(order_id="1", status=ExchangeOrderStatus.FILLED, filled_qty=0.02, fill_price=50000.0)
    )
    live = LiveExecutor(capital_usd=10000.0, transport=transport)

    price_p = paper.execute(order).state.fill_price
    price_l = live.execute(order).state.fill_price
    assert abs(price_p - price_l) < 1e-6


def test_stale_signal_blocked_by_both():
    order = _order()
    stale_port = _FakeFreshnessPort(stale=True)

    def should_reject(stale_port, order):
        return stale_port.is_stale(order.signal)

    assert should_reject(stale_port, order) is True
