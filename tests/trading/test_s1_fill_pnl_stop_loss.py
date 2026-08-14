# -*- coding: utf-8 -*-
"""
tests/trading/test_s1_fill_pnl_stop_loss.py
=============================================

S1 — Integridad de P&L, fills reales y stop-loss (IMPLEMENTACIÓN S1).

Cubre los TEST 1-7 de la tarea:
  TEST 1  — el fill usa el precio/cantidad/costes reales del exchange,
            no signal.price.
  TEST 2  — slippage: exit por debajo del entry → P&L realizado negativo
            propagado a RiskManager (record_close).
  TEST 3  — costes: TradeRecord.pnl_pct bruto y net_pnl_pct tras fees.
  TEST 4  — round-trip perdedor: P&L real negativo en risk y analytics.
  TEST 5  — stop-loss ENABLED: posición que cruza el nivel → SELL por el
            flujo normal del OMS.
  TEST 6  — stop-loss DISABLED: sin stop → no se emite SELL.
  TEST 7  — partial fill: limitación DOCUMENTADA (no se inventa mecanismo);
            el OMS propaga filled_qty parcial sin redimensionar posición.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest
from portfolio.models.position import PortfolioState, PositionSnapshot
from trading.analytics.trade_tracker import TradeTracker
from trading.engine import TradingEngine
from trading.execution.oms import OMS
from trading.execution.order import Order, OrderSide, OrderStatus
from trading.execution.transport import OrderResult, OrderState
from trading.execution.transport import OrderStatus as TStatus
from trading.risk.manager import RiskManager
from trading.risk.models import RiskConfig, StopLossConfig
from trading.strategies.base import BaseStrategy, Signal

_NOW = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _ScriptedExecutor:
    """Executor que llena con precios/cantidad/fees por lado — simula exchange."""

    def __init__(self, fills: dict) -> None:
        self._fills = fills

    def execute(self, order: Order) -> OrderResult:
        fill_price, filled_qty, fees = self._fills[order.side.value]
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


class _SignalPriceExecutor:
    """Executor que llena al precio de señal (paper)."""

    def execute(self, order: Order) -> OrderResult:
        return OrderResult(
            accepted=True,
            state=OrderState(
                order_id=order.order_id,
                status=TStatus.FILLED,
                fill_price=order.signal.price,
                fees=0.0,
            ),
        )


class _NoSignalStrategy(BaseStrategy):
    """Estrategia que nunca genera señales — aísla el stop-loss."""

    name = "noop"
    symbol = "BTC/USDT"
    timeframe = "1h"

    def generate_signals(self, df):
        return []


class _CloseDataSource:
    """FeatureSource con un único close fijo — sin Iceberg."""

    def __init__(self, close: float) -> None:
        self._close = close

    def load_features(self, exchange, symbol, timeframe, market_type="spot", **kwargs):
        return pd.DataFrame(
            {
                "timestamp": [_NOW],
                "open": [self._close],
                "high": [self._close * 1.001],
                "low": [self._close * 0.999],
                "close": [self._close],
                "volume": [100.0],
            }
        )


class _PositionPortfolio:
    """Fake de PortfolioService con posiciones abiertas fijas."""

    def __init__(self, positions: list[PositionSnapshot]) -> None:
        self._positions = positions

    def snapshot(self) -> PortfolioState:
        return PortfolioState(positions=tuple(self._positions), capital_usd=10_000.0)

    def open_position(self, **kwargs) -> None:
        pass

    def close_position(self, order_id):
        return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _signal(side: str, price: float = 50_000.0) -> Signal:
    return Signal(
        symbol="BTC/USDT",
        timeframe="1h",
        direction=side,
        price=price,
        timestamp=_NOW,
        confidence=1.0,
    )


def _filled_order(
    side: OrderSide,
    fill_price: float,
    *,
    filled_qty: float | None = None,
    fees: float = 0.0,
    oid: str | None = None,
) -> Order:
    order = Order(symbol="BTC/USDT", side=side, size_pct=0.05, signal=_signal(side.value, fill_price), order_id=oid)
    order.transition(OrderStatus.SUBMITTED)
    order.transition(
        OrderStatus.FILLED,
        fill_price=fill_price,
        fill_timestamp=_NOW,
        filled_qty=filled_qty,
        fees=fees,
    )
    return order


def _new_oms(executor) -> tuple[RiskManager, OMS]:
    risk = RiskManager(config=RiskConfig())
    oms = OMS(risk_manager=risk, executor=executor)
    return risk, oms


# ── TEST 1 — fill real (no signal.price) ─────────────────────────────────────


def test_fill_uses_real_exchange_price_qty_and_fees() -> None:
    """TEST 1: el fill propaga precio/cantidad/fees reales del exchange."""
    risk, oms = _new_oms(_ScriptedExecutor({"buy": (49_800.0, 0.02, 2.5)}))
    order = oms.submit(_signal("buy", price=50_000.0))

    assert order is not None
    assert order.status == OrderStatus.FILLED
    assert order.fill_price == pytest.approx(49_800.0)
    assert order.fill_price != order.signal.price, "no debe usar signal.price como fill"
    assert order.filled_qty == pytest.approx(0.02)
    assert order.fees == pytest.approx(2.5)


# ── TEST 2 — slippage → P&L negativo en risk ─────────────────────────────────


def test_slippage_exit_below_entry_yields_negative_realized_pnl() -> None:
    """TEST 2: exit por debajo del entry → P&L negativo propagado a RiskManager."""
    risk, oms = _new_oms(_ScriptedExecutor({"buy": (50_000.0, 0.02, 0.0), "sell": (49_800.0, 0.02, 0.0)}))

    oms.submit(_signal("buy", price=50_000.0))
    oms.submit(_signal("sell", price=49_900.0))

    # P&L USD: 0.02 × (49800 - 50000) = -4.0 (no 0.4%).
    assert risk.state()["total_pnl_usd"] == pytest.approx(-4.0)
    assert risk.state()["total_pnl_usd"] < 0.0


# ── TEST 3 — costes: bruto vs neto ───────────────────────────────────────────


def test_trade_record_net_pnl_subtracts_costs() -> None:
    """TEST 3: pnl_pct bruto y fee_amount_usd tras costs (costes del dominio)."""
    tracker = TradeTracker(exchange="bybit")
    entry = _filled_order(OrderSide.BUY, 50_000.0, filled_qty=0.02, fees=5.0, oid="b1")
    exit_ = _filled_order(OrderSide.SELL, 51_000.0, filled_qty=0.02, fees=5.0, oid="s1")

    tracker.on_fill(entry)
    tracker.on_fill(exit_)

    trade = tracker.last_trade()
    assert trade is not None
    # pnl_pct bruto derivado: (51000-50000)/50000 = 2%
    assert trade.pnl_pct == pytest.approx(0.02)
    # fee_amount_usd en lugar de fees (atributo eliminado en F3).
    assert trade.fee_amount_usd == pytest.approx(10.0)  # 5 + 5
    # fees_pct derivado: fee_amount_usd / (closed_qty × avg_entry) = 10/1000 = 0.01
    assert trade.fees_pct == pytest.approx(0.01)
    # Under F3, net P&L USD: gross 20.0 - fees 10.0 = 10.0 USD.
    assert trade.pnl_usd == pytest.approx(10.0)


def test_trade_record_paper_has_zero_costs_net_equals_gross() -> None:
    """TEST 3: sin fees (paper) → net_pnl_pct == pnl_pct (costes = 0)."""
    tracker = TradeTracker(exchange="bybit")
    entry = _filled_order(OrderSide.BUY, 50_000.0, filled_qty=0.02, fees=0.0, oid="b1")
    exit_ = _filled_order(OrderSide.SELL, 52_000.0, filled_qty=0.02, fees=0.0, oid="s1")

    tracker.on_fill(entry)
    tracker.on_fill(exit_)

    trade = tracker.last_trade()
    assert trade is not None
    assert trade.net_pnl_pct == pytest.approx(trade.pnl_pct)
    assert trade.pnl_pct == pytest.approx(0.04)


# ── TEST 4 — round-trip perdedor ─────────────────────────────────────────────


def test_losing_round_trip_reflects_real_pnl() -> None:
    """TEST 4: BUY@100 SELL@90 → P&L ≈ -10% en risk y analytics."""
    risk, oms = _new_oms(_ScriptedExecutor({"buy": (100.0, 1.0, 0.0), "sell": (90.0, 1.0, 0.0)}))
    tracker = TradeTracker(exchange="bybit")
    oms._on_fill = tracker.on_fill

    oms.submit(_signal("buy", price=100.0))
    oms.submit(_signal("sell", price=95.0))

    # P&L USD: 1.0 × (90 - 100) = -10.0 (no -10% percentage).
    assert risk.state()["total_pnl_usd"] == pytest.approx(-10.0)

    trade = tracker.last_trade()
    assert trade is not None
    assert trade.pnl_pct == pytest.approx((90.0 - 100.0) / 100.0)
    assert trade.is_winner is False


# ── TEST 5 — stop-loss ENABLED ───────────────────────────────────────────────


def _build_stop_loss_engine(stop_loss: StopLossConfig, close: float):
    portfolio = _PositionPortfolio(
        [
            PositionSnapshot(
                symbol="BTC/USDT",
                exchange="bybit",
                side="long",
                quantity=1.0,
                avg_entry=50_000.0,
                size_pct=0.05,
                entry_at=_NOW,
                order_id="pos-1",
            )
        ]
    )
    risk = RiskManager(config=RiskConfig(stop_loss=stop_loss))
    oms = OMS(risk_manager=risk, executor=_SignalPriceExecutor())
    engine = TradingEngine(
        strategy=_NoSignalStrategy(),
        oms=oms,
        data_source=_CloseDataSource(close),
        exchange="bybit",
        portfolio=portfolio,
        stop_loss=stop_loss,
    )
    return engine


def test_stop_loss_enabled_emits_sell_when_level_crossed() -> None:
    """TEST 5: close ≤ entry*(1-pct) → SELL emitido por el flujo normal del OMS."""
    stop = StopLossConfig(enabled=True, default_pct=0.02)  # stop en 49_000
    engine = _build_stop_loss_engine(stop, close=48_500.0)

    result = engine.run_once()

    assert result.stop_loss_closes == 1
    assert len(result.orders) == 1
    assert result.orders[0].side.value == "sell"
    assert result.orders[0].status == OrderStatus.FILLED


def test_stop_loss_enabled_no_breach_no_sell() -> None:
    """TEST 5: close sobre el nivel de stop → sin SELL."""
    stop = StopLossConfig(enabled=True, default_pct=0.02)  # stop en 49_000
    engine = _build_stop_loss_engine(stop, close=49_500.0)

    result = engine.run_once()

    assert result.stop_loss_closes == 0
    assert result.orders == []


# ── TEST 6 — stop-loss DISABLED ──────────────────────────────────────────────


def test_stop_loss_disabled_ignores_breach() -> None:
    """TEST 6: enabled=False → el cruce de nivel NO genera SELL."""
    stop = StopLossConfig(enabled=False, default_pct=0.02)
    engine = _build_stop_loss_engine(stop, close=48_500.0)  # brecha clara

    result = engine.run_once()

    assert result.stop_loss_closes == 0
    assert result.orders == []


# ── TEST 7 — partial fill: limitación documentada ────────────────────────────


def test_partial_fill_propagates_filled_qty_without_resize() -> None:
    """TEST 7: fill parcial se propaga (filled_qty) sin inventar mecanismo.

    Limitación documentada (S1): el OMS registra la cantidad ejecutada real
    pero NO redimensiona la posición/order — el sistema asume fill completo
    para sizing y cierre. No se inventa un mecanismo de fracción aquí.
    """
    risk, oms = _new_oms(_ScriptedExecutor({"buy": (50_000.0, 0.01, 1.0)}))  # 0.01 < 0.02 pedido
    order = oms.submit(_signal("buy", price=50_000.0))

    assert order is not None
    assert order.status == OrderStatus.FILLED
    assert order.filled_qty == pytest.approx(0.01)  # cantidad parcial real propagada
    assert risk.state()["open_positions"] == 1  # posición cuenta completa (limitación)
