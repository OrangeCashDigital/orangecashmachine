# -*- coding: utf-8 -*-
"""
tests/trading/test_oms_cancel_lifecycle.py
============================================

B-MD-008 paso 4 (ADR-0029): estado transitorio CANCELLING + resolución CANCEL/FILL.

Cubre:
- Transiciones de la máquina de estados (SUBMITTED → CANCELLING → CANCELLED |
  FILLED | REJECTED; terminales no relajados).
- request_cancel() idempotente.
- Resolución determinista: el fill SIEMPRE prevalece; fail-closed sin
  confirmación (permanece CANCELLING); CANCELLED revierte HELD; REJECTED.
- Sin infraestructura: sin I/O, sin sleeps.

El flujo síncrono de submit() siempre resuelve a FILLED o REJECTED; las
órdenes vivas (SUBMITTED/CANCELLING) las gestiona el loop de reconciliación
(manage_open_orders, B-MD-008 paso 5). Por eso los tests inyectan la orden
directamente en el estado deseado vía los mapas internos del OMS.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from trading.execution.oms import OMS
from trading.execution.order import Order, OrderSide, OrderStatus
from trading.execution.transport import OrderResult, OrderState
from trading.execution.transport import OrderStatus as ExchangeStatus
from trading.risk.manager import RiskManager
from trading.risk.models import RiskConfig
from trading.strategies.base import Signal


def _risk_cfg() -> RiskConfig:
    return RiskConfig()


def _sg(side: OrderSide) -> Signal:
    return Signal(
        symbol="BTC/USDT",
        timeframe="1m",
        direction=side.value,
        price=50_000.0,
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
        confidence=1.0,
    )


def _oms() -> tuple[RiskManager, OMS]:
    risk = RiskManager(config=_risk_cfg())

    class _PendingExecutor:
        """Executor que deja la orden pendiente (sin I/O)."""

        def execute(self, order: Order) -> OrderResult:
            return OrderResult(accepted=False, reason="pending")

    oms = OMS(risk_manager=risk, executor=_PendingExecutor())
    return risk, oms


def _inject(oms: OMS, side: OrderSide = OrderSide.BUY) -> Order:
    """Crea una orden viva en SUBMITTED dentro del OMS (sin pasar por submit).

    Reproduce el estado que deja submit() tras la transición a SUBMITTED:
    registrada en _orders/_open y (si BUY) con record_open de la posición HELD.
    """
    order = Order(symbol="BTC/USDT", side=side, size_pct=0.05, signal=_sg(side))
    order.transition(OrderStatus.SUBMITTED)
    oms._orders[order.order_id] = order
    oms._open[order.order_id] = order
    if side == OrderSide.BUY:
        oms._risk.record_open()
    return order


# ----------------------------------------------------------------------
# Máquina de estados (order.py)
# ----------------------------------------------------------------------


def test_submitted_can_go_to_cancelling() -> None:
    _, oms = _oms()
    order = _inject(oms)
    order.transition(OrderStatus.CANCELLING)  # no lanza


def test_cancelling_is_open_and_not_terminal() -> None:
    _, oms = _oms()
    order = _inject(oms)
    order.transition(OrderStatus.CANCELLING)
    assert order.is_open, "CANCELLING es transitorio y visible (ADR-0029)"
    assert not order.is_terminal


def test_cancelling_transitions_are_valid() -> None:
    _, oms = _oms()
    for target in (
        OrderStatus.CANCELLED,
        OrderStatus.FILLED,
        OrderStatus.REJECTED,
    ):
        order = _inject(oms)
        order.transition(OrderStatus.CANCELLING)
        order.transition(target)  # no lanza — transiciones legítimas


def test_terminal_states_are_not_relaxed() -> None:
    _, oms = _oms()
    for terminal in (OrderStatus.FILLED, OrderStatus.REJECTED, OrderStatus.CANCELLED):
        order = _inject(oms)
        order.transition(terminal)
        for impossible in (OrderStatus.SUBMITTED, OrderStatus.CANCELLING, OrderStatus.PENDING):
            with pytest.raises(ValueError):
                order.transition(impossible)


# ----------------------------------------------------------------------
# request_cancel — idempotencia y no-ops
# ----------------------------------------------------------------------


def test_request_cancel_transitions_to_cancelling() -> None:
    _, oms = _oms()
    order = _inject(oms)
    assert oms.request_cancel(order.order_id) is True
    assert order.status == OrderStatus.CANCELLING


def test_request_cancel_idempotent_on_cancelling() -> None:
    _, oms = _oms()
    order = _inject(oms)
    assert oms.request_cancel(order.order_id) is True
    assert oms.request_cancel(order.order_id) is True  # segunda petición → no-op True
    assert order.status == OrderStatus.CANCELLING


def test_request_cancel_returns_false_for_unknown_order() -> None:
    _, oms = _oms()
    assert oms.request_cancel("no-existe") is False


def test_request_cancel_returns_false_for_terminal_order() -> None:
    _, oms = _oms()
    order = _inject(oms)
    order.transition(OrderStatus.FILLED, fill_price=50_000.0, filled_qty=1.0)
    assert oms.request_cancel(order.order_id) is False


# ----------------------------------------------------------------------
# Resolución CANCEL/FILL (resolve_cancel)
# ----------------------------------------------------------------------


def test_resolve_cancel_filled_prevails() -> None:
    """Fill real durante CANCELLING → FILLED terminal (el fill SIEMPRE prevalece)."""
    _, oms = _oms()
    order = _inject(oms)
    assert oms.request_cancel(order.order_id) is True

    state = OrderState(
        order_id=order.order_id,
        status=ExchangeStatus.FILLED,
        fill_price=order.signal.price,
        filled_qty=1.0,
        fees=0.0,
    )
    result = oms.resolve_cancel(order.order_id, state)

    assert result == OrderStatus.FILLED
    assert order.status == OrderStatus.FILLED
    assert order.is_terminal


def test_resolve_cancel_cancelled_confirmed_reverts_held_buy() -> None:
    """Cancel confirmado → CANCELLED; la posición HELD del BUY se revierte."""
    risk, oms = _oms()
    order = _inject(oms)
    assert risk.open_positions == 1, "BUY vivo mantiene la posición HELD"
    assert oms.request_cancel(order.order_id) is True

    result = oms.resolve_cancel(
        order.order_id,
        OrderState(order_id=order.order_id, status=ExchangeStatus.CANCELLED),
    )

    assert result == OrderStatus.CANCELLED
    assert order.status == OrderStatus.CANCELLED
    assert order.is_terminal
    assert risk.open_positions == 0, "CANCELLED confirmado debe revertir HELD del BUY"


def test_resolve_cancel_rejected() -> None:
    """Cancel rechazado / no concluyente → REJECTED (fail-closed)."""
    _, oms = _oms()
    order = _inject(oms)
    assert oms.request_cancel(order.order_id) is True

    result = oms.resolve_cancel(
        order.order_id,
        OrderState(order_id=order.order_id, status=ExchangeStatus.REJECTED, error="too_late"),
    )

    assert result == OrderStatus.REJECTED
    assert order.status == OrderStatus.REJECTED
    assert order.is_terminal


def test_resolve_cancel_fail_closed_stays_cancelling_on_error() -> None:
    """Timeout/error sin confirmación → permanece CANCELLING (fail-closed)."""
    _, oms = _oms()
    order = _inject(oms)
    assert oms.request_cancel(order.order_id) is True

    result = oms.resolve_cancel(
        order.order_id,
        OrderState(order_id=order.order_id, status=ExchangeStatus.ERROR, error="timeout"),
    )

    assert result == OrderStatus.CANCELLING
    assert order.status == OrderStatus.CANCELLING
    assert not order.is_terminal


def test_resolve_cancel_fail_closed_on_none_state() -> None:
    """Sin estado del exchange → permanece CANCELLING (nunca CANCELLED ciego)."""
    _, oms = _oms()
    order = _inject(oms)
    assert oms.request_cancel(order.order_id) is True

    result = oms.resolve_cancel(order.order_id, None)

    assert result == OrderStatus.CANCELLING
    assert order.status == OrderStatus.CANCELLING


def test_resolve_cancel_fail_closed_on_ambiguous_state() -> None:
    """Estado del exchange sin decisión (open/submitted) → permanece CANCELLING."""
    _, oms = _oms()
    order = _inject(oms)
    assert oms.request_cancel(order.order_id) is True

    result = oms.resolve_cancel(
        order.order_id,
        OrderState(order_id=order.order_id, status=ExchangeStatus.SUBMITTED),
    )

    assert result == OrderStatus.CANCELLING
    assert order.status == OrderStatus.CANCELLING


def test_resolve_cancel_noop_for_unknown_or_resolved() -> None:
    """resolve_cancel sobre orden desconocida o ya resuelta → no-op."""
    _, oms = _oms()
    # Desconocida
    assert oms.resolve_cancel("no-existe", OrderState(status=ExchangeStatus.CANCELLED)) == OrderStatus.CANCELLED

    # Ya resuelta (FILLED terminal) → devuelve su estado, no lo cambia
    order = _inject(oms)
    order.transition(OrderStatus.FILLED, fill_price=50_000.0, filled_qty=1.0)
    assert oms.resolve_cancel(order.order_id, OrderState(status=ExchangeStatus.CANCELLED)) == OrderStatus.FILLED
    assert order.status == OrderStatus.FILLED


# ----------------------------------------------------------------------
# exchange_order_id (prerrequisito manage_open_orders, ADR-0029 paso 5)
# ----------------------------------------------------------------------


def test_submit_captures_exchange_order_id_on_fill() -> None:
    """submit() propaga result.state.order_id a Order.exchange_order_id."""
    risk = RiskManager(config=_risk_cfg())

    class _FillingExecutor:
        def execute(self, order: Order) -> OrderResult:
            state = OrderState(
                order_id="exch-12345",
                status=ExchangeStatus.FILLED,
                fill_price=50_000.0,
                filled_qty=0.01,
                fees=0.5,
            )
            return OrderResult(accepted=True, state=state)

    oms = OMS(risk_manager=risk, executor=_FillingExecutor())
    order = oms.submit(_sg(OrderSide.BUY))

    assert order is not None
    assert order.exchange_order_id == "exch-12345"
    assert order.status == OrderStatus.FILLED


def test_submit_captures_exchange_order_id_even_when_rejected() -> None:
    """Captura el ID aunque accepted=False (reconciliación no confirmada) —
    el executor pudo haber creado la orden en el exchange antes de fallar
    la reconciliación; sin el ID, manage_open_orders no podría verificarla."""
    risk = RiskManager(config=_risk_cfg())

    class _UnconfirmedExecutor:
        def execute(self, order: Order) -> OrderResult:
            state = OrderState(order_id="exch-67890", status=ExchangeStatus.SUBMITTED)
            return OrderResult(accepted=False, reason="reconciliation_no_confirmed", state=state)

    oms = OMS(risk_manager=risk, executor=_UnconfirmedExecutor())
    order = oms.submit(_sg(OrderSide.BUY))

    assert order is not None
    assert order.exchange_order_id == "exch-67890"
    assert order.status == OrderStatus.REJECTED


def test_submit_exchange_order_id_none_when_no_state() -> None:
    """Sin OrderState (p.ej. excepción antes del transport), el campo queda
    None — no se inventa un ID."""
    risk = RiskManager(config=_risk_cfg())

    class _NoStateExecutor:
        def execute(self, order: Order) -> OrderResult:
            return OrderResult(accepted=False, reason="transport_error")

    oms = OMS(risk_manager=risk, executor=_NoStateExecutor())
    order = oms.submit(_sg(OrderSide.BUY))

    assert order is not None
    assert order.exchange_order_id is None
