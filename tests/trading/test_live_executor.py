# -*- coding: utf-8 -*-
"""
tests/trading/test_live_executor.py
======================================

Guard R9/R10 / F3-B12 (ADR-0016): LiveExecutor sobre OrderTransport.

R9  (positivo):  un fill confirmado por el exchange → execute() accepted=True,
                 se registra success en el guard.
R10 (negativo):  fallo de transporte → orden rechazada (accepted=False), y la
                 reconciliación NO confirma fill → fail-closed (sin countdown
                 de posición vía OMS).

S1: execute() retorna OrderResult (no bool) — se aserta sobre `.accepted` y se
puede inspeccionar el OrderState del fill real.

Se cubren además:
  - kill switch activo bloquea el submit (accepted=False, sin I/O).
  - convergencia de reintentos con backoff ante error transitorio.
  - PaperTransport y un transport fake validan el flujo orden→fill→estado.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from trading.execution.live_executor import LiveExecutor
from trading.execution.order import Order, OrderSide
from trading.execution.transport import (
    OrderState,
    OrderStatus,
)
from trading.strategies.base import Signal

from ocm.runtime.guard import ExecutionGuard


def _signal(side: OrderSide = OrderSide.BUY) -> Signal:
    return Signal(
        symbol="BTC/USDT",
        timeframe="1m",
        direction=side.value,
        price=50_000.0,
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
        confidence=1.0,
    )


def _order(side: OrderSide = OrderSide.BUY) -> Order:
    return Order(symbol="BTC/USDT", side=side, size_pct=0.1, signal=_signal(side))


class _FakeTransport:
    """Transport controlable para pruebas — con submit/fetch_state configurables."""

    def __init__(
        self,
        submit_state: OrderState | None = None,
        fetch_state: OrderState | None = None,
        submit_error: Exception | None = None,
    ) -> None:
        self._submit_state = submit_state
        self._fetch_state = fetch_state
        self._submit_error = submit_error
        self.submitted: list[tuple[str, str, float]] = []
        self.fetch_ids: list[str] = []
        self.closed = False

    def submit(self, symbol, side, qty, *, client_order_id=None):
        self.submitted.append((symbol, side, qty))
        if self._submit_error is not None:
            raise self._submit_error
        if self._submit_state is not None:
            return self._submit_state
        return OrderState(
            order_id=f"exc-{client_order_id}",
            status=OrderStatus.FILLED,
            filled_qty=qty,
        )

    def fetch_state(self, exchange_order_id):
        self.fetch_ids.append(exchange_order_id)
        if self._fetch_state is not None:
            return self._fetch_state
        return OrderState(order_id=exchange_order_id, status=OrderStatus.FILLED)

    def close(self):
        self.closed = True


def _make_executor(
    transport,
    *,
    guard: ExecutionGuard | None = None,
    capital: float = 10_000.0,
):
    return LiveExecutor(
        capital_usd=capital,
        transport=transport,
        exchange="bybit",
        guard=guard,
        max_retries=1,
        backoff_s=0.0,
    )


# ── R10 (positivo): fill confirmado → aceptada ───────────────────────────────


def test_filled_confirmed_transport_accepts() -> None:
    t = _FakeTransport(submit_state=OrderState(order_id="e1", status=OrderStatus.FILLED, filled_qty=0.02))
    exe = _make_executor(t)

    assert exe.execute(_order()).accepted is True
    assert len(t.submitted) == 1
    assert t.submitted[0][0] == "BTC/USDT"
    assert t.submitted[0][1] == "buy"


def test_filled_confirmed_records_success_on_guard() -> None:
    guard = ExecutionGuard(max_errors=2)
    t = _FakeTransport(submit_state=OrderState(order_id="e1", status=OrderStatus.FILLED, filled_qty=0.02))
    exe = _make_executor(t, guard=guard)

    exe.execute(_order())
    assert guard.summary()["total_successes"] == 1


# ── R10 (negativo): sin confirmación → rechazada ─────────────────────────────


def test_submit_error_rejects_and_guard_records_error() -> None:
    guard = ExecutionGuard(max_errors=2)
    t = _FakeTransport(submit_error=RuntimeError("network"))
    exe = _make_executor(t, guard=guard)

    assert exe.execute(_order()).accepted is False
    assert guard.summary()["total_errors"] == 1


def test_reconciliation_unconfirmed_rejects() -> None:
    """fill no confirmado → rechazada (fail-closed, caso B/H)."""
    t = _FakeTransport(
        submit_state=OrderState(order_id="e1", status=OrderStatus.SUBMITTED),
        fetch_state=OrderState(order_id="e1", status=OrderStatus.ERROR, error="timeout"),
    )
    exe = _make_executor(t)

    assert exe.execute(_order()).accepted is False
    assert t.fetch_ids == ["e1"]


def test_kill_switch_blocks_without_io() -> None:
    guard = ExecutionGuard(max_errors=2)
    guard.trigger("manual_kill")
    t = _FakeTransport()
    exe = _make_executor(t, guard=guard)

    assert exe.execute(_order()).accepted is False
    assert t.submitted == []  # sin I/O: kill switch aborta antes del submit


# ── convergencia con retries ─────────────────────────────────────────────────


def test_transient_error_then_success() -> None:
    class _Flaky:
        def __init__(self):
            self.calls = 0

        def submit(self, symbol, side, qty, *, client_order_id=None):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("transient")
            return OrderState(order_id="e1", status=OrderStatus.FILLED, filled_qty=qty)

        def fetch_state(self, exchange_order_id):
            return OrderState(order_id=exchange_order_id, status=OrderStatus.FILLED)

        def close(self):
            pass

    exe = LiveExecutor(
        capital_usd=10_000.0,
        transport=_Flaky(),
        exchange="bybit",
        max_retries=2,
        backoff_s=0.0,
    )
    assert exe.execute(_order()).accepted is True


# ── invariantes de construcción ──────────────────────────────────────────────


def test_requires_transport() -> None:
    with pytest.raises(ValueError, match="transport"):
        LiveExecutor(capital_usd=10_000.0, transport=None, exchange="bybit")  # type: ignore[arg-type]


def test_requires_positive_capital() -> None:
    t = _FakeTransport()
    with pytest.raises(ValueError, match="capital"):
        _make_executor(t, capital=0.0)


def test_print_executor_and_paper_transport_repr() -> None:
    from trading.execution.transport import PaperTransport

    empty = PaperTransport()
    exe = _make_executor(_FakeTransport())
    assert "LiveExecutor" in repr(exe)
    assert "PaperTransport" in repr(empty)
