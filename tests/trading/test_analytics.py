# -*- coding: utf-8 -*-
"""
tests/trading/test_analytics.py
=================================

Tests unitarios de TradeRecord, TradeTracker y PerformanceEngine.
Sin I/O — lógica pura.
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta

import pytest

from trading.analytics.trade_record import TradeRecord
from trading.analytics.trade_tracker import TradeTracker
from trading.analytics.performance import PerformanceEngine
from trading.execution.order import Order, OrderSide, OrderStatus
from trading.strategies.base import Signal


# ── Helpers ───────────────────────────────────────────────────────────────────

_NOW = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

def _make_trade(
    pnl_pct:  float,
    symbol:   str   = "BTC/USDT",
    trade_id: str   = "t001",
    dur_s:    float = 3600.0,
) -> TradeRecord:
    entry = 50_000.0
    exit_ = entry * (1 + pnl_pct)
    return TradeRecord.close(
        trade_id    = trade_id,
        symbol      = symbol,
        exchange    = "bybit",
        timeframe   = "1h",
        entry_price = entry,
        exit_price  = exit_,
        size_pct    = 0.05,
        entry_at    = _NOW,
        exit_at     = _NOW + timedelta(seconds=dur_s),
    )


def _make_filled_order(
    side:   OrderSide,
    symbol: str   = "BTC/USDT",
    price:  float = 50_000.0,
    oid:    str   = "o001",
) -> Order:
    signal = Signal(
        symbol    = symbol,
        timeframe = "1h",
        signal    = side.value,
        price     = price,
        timestamp = _NOW,
        confidence = 1.0,
    )
    order = Order(
        symbol   = symbol,
        side     = side,
        size_pct = 0.05,
        signal   = signal,
        order_id = oid,
    )
    order.transition(OrderStatus.SUBMITTED)
    order.transition(OrderStatus.FILLED, fill_price=price, fill_timestamp=_NOW)
    return order


# ── TradeRecord ────────────────────────────────────────────────────────────────

class TestTradeRecord:
    def test_close_factory_calculates_pnl(self):
        trade = _make_trade(pnl_pct=0.05)
        assert abs(trade.pnl_pct - 0.05) < 1e-9

    def test_close_factory_calculates_duration(self):
        trade = _make_trade(pnl_pct=0.01, dur_s=7200.0)
        assert trade.duration_s == pytest.approx(7200.0)

    def test_is_winner_positive_pnl(self):
        assert _make_trade(0.01).is_winner is True

    def test_is_winner_negative_pnl(self):
        assert _make_trade(-0.01).is_winner is False

    def test_is_winner_zero_pnl(self):
        assert _make_trade(0.0).is_winner is False

    def test_pnl_usd_returns_none(self):
        # TradeRecord no conoce capital — siempre None
        assert _make_trade(0.05).pnl_usd is None

    def test_str_contains_win_loss(self):
        assert "WIN"  in str(_make_trade(0.05))
        assert "LOSS" in str(_make_trade(-0.05))

    def test_frozen_immutable(self):
        """dataclass(frozen=True) lanza AttributeError (FrozenInstanceError es subclase)."""
        trade = _make_trade(0.05)
        with pytest.raises(AttributeError):
            trade.pnl_pct = 0.99  # type: ignore[misc]


# ── TradeTracker ───────────────────────────────────────────────────────────────

class TestTradeTracker:
    def test_buy_fill_opens_position(self):
        tracker = TradeTracker(exchange="bybit")
        buy = _make_filled_order(OrderSide.BUY, oid="b1")
        tracker.on_fill(buy)
        assert "BTC/USDT" in tracker.open_positions

    def test_sell_closes_position(self):
        tracker = TradeTracker(exchange="bybit")
        buy  = _make_filled_order(OrderSide.BUY,  oid="b1", price=50_000.0)
        sell = _make_filled_order(OrderSide.SELL, oid="s1", price=52_000.0)
        tracker.on_fill(buy)
        tracker.on_fill(sell)
        assert "BTC/USDT" not in tracker.open_positions
        assert tracker.trade_count == 1

    def test_pnl_correct_on_close(self):
        tracker = TradeTracker(exchange="bybit")
        buy  = _make_filled_order(OrderSide.BUY,  oid="b1", price=50_000.0)
        sell = _make_filled_order(OrderSide.SELL, oid="s1", price=55_000.0)
        tracker.on_fill(buy)
        tracker.on_fill(sell)
        trade = tracker.last_trade()
        assert trade is not None
        assert trade.pnl_pct == pytest.approx(0.10, rel=1e-6)

    def test_sell_without_buy_ignored(self):
        tracker = TradeTracker(exchange="bybit")
        sell = _make_filled_order(OrderSide.SELL, oid="s1")
        tracker.on_fill(sell)  # no lanza
        assert tracker.trade_count == 0

    def test_on_fill_never_raises(self):
        """
        SafeOps: on_fill no debe lanzar ante errores internos.

        Usamos un objeto que causa excepción durante _process_fill
        sin depender de spec= (que restringe atributos del mock).
        """
        tracker = TradeTracker(exchange="bybit")

        # Objeto que lanza en cualquier acceso a atributo
        class BrokenOrder:
            @property
            def side(self):
                raise RuntimeError("broken object")

        tracker.on_fill(BrokenOrder())  # no debe lanzar

    def test_summary_observable(self):
        tracker = TradeTracker()
        s = tracker.summary()
        assert "closed_trades"  in s
        assert "open_positions" in s


# ── PerformanceEngine ──────────────────────────────────────────────────────────

class TestPerformanceEngine:
    def _trades(self, pnls: list[float]) -> list[TradeRecord]:
        return [_make_trade(p, trade_id=f"t{i}") for i, p in enumerate(pnls)]

    def test_summarize_empty(self):
        s = PerformanceEngine.summarize([])
        assert s.total_trades   == 0
        assert s.win_rate       is None
        assert s.sharpe_ratio   is None
        assert s.max_drawdown   == 0.0

    def test_win_rate_all_winners(self):
        trades = self._trades([0.01, 0.02, 0.03])
        assert PerformanceEngine.win_rate(trades) == pytest.approx(1.0)

    def test_win_rate_mixed(self):
        trades = self._trades([0.05, -0.02, 0.03, -0.01])
        assert PerformanceEngine.win_rate(trades) == pytest.approx(0.5)

    def test_sharpe_single_trade_returns_none(self):
        assert PerformanceEngine.sharpe_ratio(self._trades([0.05])) is None

    def test_sharpe_identical_returns_none(self):
        # std = 0 → sharpe indefinido
        trades = self._trades([0.01, 0.01, 0.01])
        assert PerformanceEngine.sharpe_ratio(trades) is None

    def test_sharpe_positive_for_positive_returns(self):
        trades = self._trades([0.02, 0.03, 0.015, 0.025])
        sr = PerformanceEngine.sharpe_ratio(trades)
        assert sr is not None and sr > 0

    def test_max_drawdown_no_loss(self):
        trades = self._trades([0.01, 0.02, 0.01])
        assert PerformanceEngine.max_drawdown(trades) == pytest.approx(0.0)

    def test_max_drawdown_with_loss(self):
        # equity: 1 → 1.1 → 0.99 → 1.089 → drawdown en step 2
        trades = self._trades([0.10, -0.10, 0.10])
        mdd = PerformanceEngine.max_drawdown(trades)
        assert mdd > 0.0
        assert mdd < 0.15  # no catastrófico

    def test_max_drawdown_always_positive(self):
        trades = self._trades([-0.05, -0.03, -0.02])
        assert PerformanceEngine.max_drawdown(trades) > 0.0

    def test_total_pnl_pct(self):
        trades = self._trades([0.05, -0.02, 0.03])
        assert PerformanceEngine.total_pnl_pct(trades) == pytest.approx(0.06)

    def test_pnl_usd(self):
        trades = self._trades([0.10])
        assert PerformanceEngine.pnl_usd(trades, 10_000.0) == pytest.approx(1_000.0)

    def test_equity_curve_starts_at_one(self):
        trades = self._trades([0.05, -0.02])
        curve = PerformanceEngine.equity_curve(trades)
        assert curve[0] == 1.0
        assert len(curve) == 3  # punto inicial + un punto por trade (2 trades → 3 puntos)

    def test_equity_curve_empty(self):
        assert PerformanceEngine.equity_curve([]) == [1.0]

    def test_profit_factor_no_losses(self):
        s = PerformanceEngine.summarize(self._trades([0.05, 0.03]))
        assert s.profit_factor is None  # sin pérdidas → indefinido

    def test_summarize_full(self):
        trades = self._trades([0.05, -0.02, 0.03, -0.01, 0.04])
        s = PerformanceEngine.summarize(trades, capital_usd=10_000.0)
        assert s.total_trades   == 5
        assert s.winning_trades == 3
        assert s.losing_trades  == 2
        assert s.win_rate       == pytest.approx(0.6)
        assert s.pnl_usd        is not None
        assert s.max_drawdown   >= 0.0
        assert "Performance("   in str(s)
