"""Tests de regresión para Finding #1 (Greptile review PR #110).

Escenario del bug: un BUY rechazado (revert_open=True) borraba la posición
de un BUY previo del mismo símbolo, porque _reject llamaba a
record_position(symbol, None) incondicionalmente.

Fix: _reject(revert_open=True) ya no toca el mirror de Risk, porque un BUY
rechazado nunca llegó a _fill, entonces portfolio.open_position nunca se
llamó. No hay estado patrimonial que revertir.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from portfolio.infra.memory_store import InMemoryPositionStore
from portfolio.services.portfolio_service import PortfolioService
from trading.analytics.trade_tracker import TradeTracker
from trading.domain.models.order import Order, OrderSide, OrderStatus
from trading.domain.risk.models import RiskConfig
from trading.domain.strategies.base import Signal
from trading.execution.fill_sync import build_fill_sync
from trading.execution.oms import OMS
from trading.ports.outbound.transport import ExchangeOrderStatus as ExchangeStatus
from trading.ports.outbound.transport import ExchangeOrderStatus as TStatus
from trading.ports.outbound.transport import OrderResult, OrderState
from trading.risk.manager import RiskManager

_NOW = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def _signal(direction: str, price: float, symbol: str = "BTC/USDT") -> Signal:
    return Signal(
        symbol=symbol,
        timeframe="1h",
        direction=direction,
        price=price,
        timestamp=_NOW,
        confidence=1.0,
    )


class _AcceptingExecutor:
    def execute(self, order: Order) -> OrderResult:
        return OrderResult(
            accepted=True,
            state=OrderState(
                order_id=order.order_id,
                status=TStatus.FILLED,
                fill_price=order.signal.price,
                filled_qty=1.0,
                fees=0.0,
            ),
        )


class _RejectingExecutor:
    def execute(self, order: Order) -> OrderResult:
        return OrderResult(accepted=False, reason="test_reject")


def _make_chain(executor) -> tuple[RiskManager, OMS, PortfolioService]:
    risk = RiskManager(config=RiskConfig())
    portfolio = PortfolioService(
        capital_usd=10_000.0,
        store=InMemoryPositionStore(),
        exchange="bybit",
    )
    tracker = TradeTracker(exchange="bybit", portfolio=portfolio)
    oms = OMS(
        risk_manager=risk,
        executor=executor,
        portfolio=portfolio,
        on_fill=build_fill_sync(tracker, portfolio),
    )
    return risk, oms, portfolio


def test_reject_buy_does_not_erase_prior_position() -> None:
    """BUY A fill → BUY B rejected → posición A sigue en el mirror de Risk."""
    risk, oms, portfolio = _make_chain(_AcceptingExecutor())
    oms.submit(_signal("buy", price=50_000.0))
    assert "BTC/USDT" in risk._positions

    oms._executor = _RejectingExecutor()
    oms.submit(_signal("buy", price=50_000.0))

    assert "BTC/USDT" in risk._positions
    assert risk._positions["BTC/USDT"][0] == pytest.approx(1.0)
    assert len(portfolio.snapshot().positions) == 1


def test_reject_buy_with_no_prior_position_is_noop() -> None:
    """BUY rejected sin prior → _positions vacío, _pnl_unknown_closes = 0."""
    risk, oms, _ = _make_chain(_RejectingExecutor())
    oms.submit(_signal("buy", price=50_000.0))

    assert "BTC/USDT" not in risk._positions
    assert risk._pnl_unknown_closes == 0


def test_reject_other_symbol_does_not_affect_positions() -> None:
    """BUY X rechazado no afecta posición de Y."""
    risk, oms, _ = _make_chain(_AcceptingExecutor())
    oms.submit(_signal("buy", price=50_000.0, symbol="BTC/USDT"))
    assert "BTC/USDT" in risk._positions

    oms._executor = _RejectingExecutor()
    oms.submit(_signal("buy", price=3_000.0, symbol="ETH/USDT"))

    assert "BTC/USDT" in risk._positions
    assert "ETH/USDT" not in risk._positions


def test_reject_does_not_increment_pnl_unknown_closes() -> None:
    """BUY rejected NO es cierre; _pnl_unknown_closes no se infla."""
    risk, oms, _ = _make_chain(_RejectingExecutor())
    oms.submit(_signal("buy", price=50_000.0))

    assert risk._pnl_unknown_closes == 0


def test_multientry_position_survives_rejected_third_buy() -> None:
    """BUY A fill + BUY B fill (merge) + BUY C rejected → qty=2 preservado."""
    risk, oms, portfolio = _make_chain(_AcceptingExecutor())
    oms.submit(_signal("buy", price=50_000.0))
    oms.submit(_signal("buy", price=51_000.0))

    positions = portfolio.snapshot().positions
    assert len(positions) == 1
    assert positions[0].quantity == pytest.approx(2.0)

    oms._executor = _RejectingExecutor()
    oms.submit(_signal("buy", price=52_000.0))

    positions = portfolio.snapshot().positions
    assert len(positions) == 1
    assert positions[0].quantity == pytest.approx(2.0)
    assert "BTC/USDT" in risk._positions
    assert risk._positions["BTC/USDT"][0] == pytest.approx(2.0)


def test_cancel_buy_after_submit_does_not_erase_prior_position() -> None:
    """CANCELLED BUY no borra posición de un BUY previo."""
    risk, oms, _ = _make_chain(_AcceptingExecutor())
    oms.submit(_signal("buy", price=50_000.0))
    assert "BTC/USDT" in risk._positions

    order = Order(
        symbol="BTC/USDT",
        side=OrderSide.BUY,
        size_pct=0.05,
        signal=_signal("buy", price=50_000.0),
    )
    order.transition(OrderStatus.SUBMITTED)
    oms._orders[order.order_id] = order

    oms.request_cancel(order.order_id)
    result = oms.resolve_cancel(
        order.order_id,
        OrderState(order_id=order.order_id, status=ExchangeStatus.CANCELLED),
    )
    assert result == OrderStatus.CANCELLED

    assert "BTC/USDT" in risk._positions
