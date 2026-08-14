# -*- coding: utf-8 -*-
"""
tests/trading/test_f4_position_accounting.py
=============================================

F4a/F4b (ADR-0025) — cantidad económica y weighted average cost.

Cubre los TEST 1-8 de la tarea sobre la cadena completa
Exchange fill → execution result (OMS) → settlement (fill_sync) →
portfolio position (PortfolioService):

  TEST 1 — BUY: Position.quantity == cantidad ejecutada real; avg == fill real.
  TEST 2 — multi-entry: 1@100 + 2@110 → qty=3, avg=106.667, basis=320 (WAC).
  TEST 3 — partial fill: requested 1.0, executed 0.37 → position.quantity = 0.37.
  TEST 4 — partial close: SELL 0.4 → remaining 0.6, avg preservado.
  TEST 5 — multi-entry + partial close → remaining basis coherente + P&L
           realizado sobre closed_qty × (exit − avg).
  TEST 6 — signal.price ≠ fill real → la posición usa el fill real (INV-10).
  TEST 7 — UNKNOWN fee (None) no se convierte en 0; basis sin fees (ADR-0026).
  TEST 8 — requested ≠ executed → la posición nunca supera lo ejecutado (INV-01).

El P&L realizado se asienta en el camino único de OMS._fill (ADR-0025 no-goal);
aquí se verifican los DATOS que habilita (qty, avg, basis), no un P&L engine.

Principios: Aislamiento (sin Redis, sin Iceberg, sin red) · Fail-Fast ·
Nomenclatura test_<condición>.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import pytest
from portfolio.infra.memory_store import InMemoryPositionStore
from portfolio.services.portfolio_service import PortfolioService
from trading.analytics.trade_tracker import TradeTracker
from trading.execution.fill_sync import build_fill_sync
from trading.execution.oms import OMS
from trading.execution.order import Order
from trading.execution.transport import OrderResult, OrderState
from trading.execution.transport import OrderStatus as TStatus
from trading.risk.manager import RiskManager
from trading.risk.models import RiskConfig
from trading.strategies.base import Signal

_NOW = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Fakes / helpers
# ---------------------------------------------------------------------------


class _ScriptedExecutor:
    """Executor que llena secuencialmente con (price, qty, fees) por lado.

    fees=None simula que el exchange NO reporta el coste (UNKNOWN, ADR-0026).
    """

    def __init__(self, fills_by_side: dict[str, list[tuple[float, float, Optional[float]]]]) -> None:
        self._iters = {side: iter(fills) for side, fills in fills_by_side.items()}

    def execute(self, order: Order) -> OrderResult:
        fill_price, filled_qty, fees = next(self._iters[order.side.value])
        return OrderResult(
            accepted=True,
            state=OrderState(
                order_id=f"exc-{order.order_id}",
                status=TStatus.FILLED,
                fill_price=fill_price,
                filled_qty=filled_qty,
                fees=fees,
            ),
        )


def _signal(side: str, price: float) -> Signal:
    return Signal(
        symbol="BTC/USDT",
        timeframe="1h",
        direction=side,
        price=price,
        timestamp=_NOW,
        confidence=1.0,
    )


def _chain(executor) -> tuple[OMS, PortfolioService, RiskManager]:
    """OMS + fill_sync + PortfolioService + TradeTracker reales, en memoria."""
    store = InMemoryPositionStore()
    portfolio = PortfolioService(capital_usd=10_000.0, store=store, exchange="bybit")
    tracker = TradeTracker(exchange="bybit")
    risk = RiskManager(config=RiskConfig())
    oms = OMS(
        risk_manager=risk,
        executor=executor,
        on_fill=build_fill_sync(tracker, portfolio),
    )
    return oms, portfolio, risk


# ── TEST 1 — BUY: quantity = ejecutado real, avg = fill real ────────────────


def test_position_quantity_equals_executed_and_avg_equals_fill() -> None:
    oms, portfolio, _ = _chain(_ScriptedExecutor({"buy": [(100.0, 1.0, 0.0)]}))
    order = oms.submit(_signal("buy", price=100.0))

    assert order is not None and order.status.value == "filled"
    positions = portfolio.snapshot().positions
    assert len(positions) == 1
    assert positions[0].quantity == pytest.approx(1.0)  # INV-01
    assert positions[0].avg_entry == pytest.approx(100.0)


# ── TEST 2 — multi-entry → WAC (1@100 + 2@110 = qty 3, avg 106.667) ─────────


def test_multi_entry_weighted_average_cost() -> None:
    oms, portfolio, _ = _chain(_ScriptedExecutor({"buy": [(100.0, 1.0, 0.0), (110.0, 2.0, 0.0)]}))

    oms.submit(_signal("buy", price=100.0))
    oms.submit(_signal("buy", price=110.0))

    positions = portfolio.snapshot().positions
    assert len(positions) == 1, "multi-entry se fusiona en una única posición (WAC)"
    pos = positions[0]
    assert pos.quantity == pytest.approx(3.0)
    assert pos.avg_entry == pytest.approx(320.0 / 3.0)  # 106.667, INV-04
    assert pos.cost_basis == pytest.approx(320.0)


# ── TEST 3 — partial fill: la posición usa lo ejecutado, no lo pedido ────────


def test_partial_fill_position_quantity_is_executed_not_requested() -> None:
    oms, portfolio, _ = _chain(_ScriptedExecutor({"buy": [(100.0, 0.37, 0.0)]}))

    order = oms.submit(_signal("buy", price=100.0))

    assert order is not None and order.filled_qty == pytest.approx(0.37)
    positions = portfolio.snapshot().positions
    assert positions[0].quantity == pytest.approx(0.37)  # INV-03
    assert positions[0].quantity <= (order.filled_qty or 0.0)  # nunca sobre lo ejecutado


# ── TEST 4 — partial close: remaining 0.6, avg preservado ────────────────────


def test_partial_close_reduces_remaining_and_preserves_avg() -> None:
    oms, portfolio, _ = _chain(_ScriptedExecutor({"buy": [(100.0, 1.0, 0.0)], "sell": [(110.0, 0.4, 0.0)]}))

    oms.submit(_signal("buy", price=100.0))
    oms.submit(_signal("sell", price=110.0))

    positions = portfolio.snapshot().positions
    assert len(positions) == 1, "cierre parcial mantiene la posición abierta"
    assert positions[0].quantity == pytest.approx(0.6)
    assert positions[0].avg_entry == pytest.approx(100.0)


# ── TEST 5 — multi-entry + partial close → basis restante coherente ─────────


def test_multi_entry_partial_close_remaining_basis_coherent() -> None:
    oms, portfolio, risk = _chain(
        _ScriptedExecutor(
            {
                "buy": [(100.0, 1.0, 0.0), (110.0, 2.0, 0.0)],
                "sell": [(120.0, 1.0, 0.0)],
            }
        )
    )

    oms.submit(_signal("buy", price=100.0))
    oms.submit(_signal("buy", price=110.0))
    oms.submit(_signal("sell", price=120.0))

    avg = 320.0 / 3.0  # 106.667
    positions = portfolio.snapshot().positions
    assert len(positions) == 1
    assert positions[0].quantity == pytest.approx(2.0)
    assert positions[0].avg_entry == pytest.approx(avg)
    assert positions[0].cost_basis == pytest.approx(2.0 * avg)  # 213.333

    # P&L realizado USD: closed_qty × (exit − avg) = 1.0 × (120.0 − 106.667) = 13.33 USD.
    assert risk.state()["total_pnl_usd"] == pytest.approx(13.33, abs=0.01)


def test_partial_close_then_full_close_leaves_position_closed() -> None:
    oms, portfolio, _ = _chain(
        _ScriptedExecutor(
            {
                "buy": [(100.0, 1.0, 0.0)],
                "sell": [(110.0, 0.4, 0.0), (112.0, 0.6, 0.0)],
            }
        )
    )

    oms.submit(_signal("buy", price=100.0))
    oms.submit(_signal("sell", price=110.0))
    assert portfolio.snapshot().open_count == 1  # parcial → sigue abierta

    oms.submit(_signal("sell", price=112.0))
    assert portfolio.snapshot().is_flat, "posición cerrada a qty=0 (INV-01)"


# ── TEST 6 — el fill real gana a signal.price (INV-10 / Q-C) ─────────────────


def test_position_uses_real_fill_not_signal_price() -> None:
    oms, portfolio, _ = _chain(_ScriptedExecutor({"buy": [(49_800.0, 1.0, 0.0)]}))

    order = oms.submit(_signal("buy", price=50_000.0))

    assert order is not None and order.fill_price == pytest.approx(49_800.0)
    positions = portfolio.snapshot().positions
    assert positions[0].avg_entry == pytest.approx(49_800.0)
    assert positions[0].avg_entry != 50_000.0  # nunca el precio de señal


# ── TEST 7 — UNKNOWN fee no se convierte en 0; basis sin fees (ADR-0026) ─────


def test_unknown_fee_stays_none_and_basis_excludes_fees() -> None:
    oms, portfolio, _ = _chain(_ScriptedExecutor({"buy": [(100.0, 1.0, None)]}))

    order = oms.submit(_signal("buy", price=100.0))

    assert order is not None
    assert order.fees is None, "fee UNKNOWN (exchange no reporta) → None, no 0 (INV-06)"
    positions = portfolio.snapshot().positions
    assert positions[0].cost_basis == pytest.approx(1.0 * 100.0)  # sin fees (ADR-0025 §9)


# ── TEST 8 — requested ≠ executed: posición nunca supera lo ejecutado ────────


def test_position_never_exceeds_executed_quantity() -> None:
    oms, portfolio, _ = _chain(_ScriptedExecutor({"buy": [(100.0, 0.37, 0.0)]}))

    oms.submit(_signal("buy", price=100.0))

    positions = portfolio.snapshot().positions
    assert positions[0].quantity == pytest.approx(0.37)
    assert positions[0].quantity <= 0.37, "INV-01: la posición refleja lo ejecutado, no lo pedido"
