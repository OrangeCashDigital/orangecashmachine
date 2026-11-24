# -*- coding: utf-8 -*-
"""tests/trading/test_engine.py — cobertura de trading/engine.py (issue #220).

Hermético: sin red, sin Kafka, sin sleeps. Fakes en línea.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import polars as pl
from trading.domain.models.order import OrderStatus
from trading.engine import EngineResult, TradingEngine

from shared.types.signal import Signal
from shared.types.values import Price

_NOW = datetime(2024, 1, 1, tzinfo=timezone.utc)


def _df(closes: list[float]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "timestamp": [_NOW] * len(closes),
            "open": closes,
            "high": [c * 1.01 for c in closes],
            "low": [c * 0.99 for c in closes],
            "close": closes,
            "volume": [1.0] * len(closes),
        }
    )


def _signal(direction="buy", symbol="BTC/USDT") -> Signal:
    return Signal(
        symbol=symbol,
        timeframe="1h",
        direction=direction,
        price=Price(Decimal("100")),
        timestamp=_NOW,
        confidence=0.9,
    )


def _order(status=OrderStatus.FILLED):
    class _O:
        def __init__(self, s):
            self.status = s

    return _O(status)


class _FakeStrategy:
    name = "fake"
    symbol = "BTC/USDT"
    timeframe = "1h"

    def __init__(self, signals=None, raises=None):
        self._s = signals or []
        self._r = raises

    def generate_signals(self, df):
        if self._r:
            raise self._r
        return list(self._s)


class _FakeDataSource:
    def __init__(self, df=None, raises=None):
        self._df = df
        self._r = raises

    def load_features(self, **kw):
        if self._r:
            raise self._r
        return self._df


class _FakeOMS:
    def __init__(self, returns=None):
        self._returns = returns or []
        self.submits = []
        self._idx = 0

    def submit(self, sig):
        self.submits.append(sig)
        if self._idx < len(self._returns):
            r = self._returns[self._idx]
            self._idx += 1
            return r
        return _order()

    def summary(self):
        return {"open": 0, "filled": 1}


class _FakeGuard:
    def __init__(self, should_stop=False, reason=""):
        self._stop = should_stop
        self.stop_reason = reason
        self.successes = 0
        self.errors = []

    def should_stop(self):
        return self._stop

    def record_success(self):
        self.successes += 1

    def record_error(self, m):
        self.errors.append(m)


def _engine(strategy=None, oms=None, data=None, guard=None):
    return TradingEngine(
        strategy=strategy or _FakeStrategy(),
        oms=oms or _FakeOMS(),
        data_source=data or _FakeDataSource(_df([100.0])),
        guard=guard,
    )


# ── Tests ───────────────────────────────────────────────────────


def test_run_once_happy_path_emits_signal_to_oms():
    oms = _FakeOMS(returns=[_order(OrderStatus.FILLED)])
    engine = _engine(strategy=_FakeStrategy([_signal()]), oms=oms, data=_FakeDataSource(_df([100.0, 101.0])))
    result = engine.run_once()
    assert result.signals_generated == 1
    assert result.orders_submitted == 1
    assert result.orders_filled == 1
    assert result.orders_rejected == 0
    assert len(oms.submits) == 1


def test_run_once_zero_signals_does_not_touch_oms():
    oms = _FakeOMS()
    engine = _engine(strategy=_FakeStrategy([]), oms=oms)
    result = engine.run_once()
    assert result.signals_generated == 0
    assert oms.submits == []
    assert result.status == "no_signal"


def test_run_once_halted_guard_skips_execution():
    oms = _FakeOMS()
    guard = _FakeGuard(should_stop=True, reason="kill_switch")
    engine = _engine(strategy=_FakeStrategy([_signal()]), oms=oms, guard=guard)
    result = engine.run_once()
    assert result.skipped is True
    assert "guard:kill_switch" in result.skip_reason
    assert oms.submits == []


def test_run_once_empty_dataframe_skips_with_no_data():
    engine = _engine(data=_FakeDataSource(pl.DataFrame()))
    result = engine.run_once()
    assert result.skipped is True
    assert result.skip_reason == "no_data"


def test_run_once_none_dataframe_skips():
    engine = _engine(data=_FakeDataSource(None))
    result = engine.run_once()
    assert result.skipped is True
    assert result.skip_reason == "no_data"


def test_run_once_featuresource_error_returns_no_data():
    engine = _engine(data=_FakeDataSource(raises=ConnectionError("down")))
    result = engine.run_once()
    assert result.skipped is True
    assert result.skip_reason == "no_data"


def test_strategy_error_skips_and_records_guard_error():
    guard = _FakeGuard()
    engine = _engine(strategy=_FakeStrategy(raises=ValueError("boom")), guard=guard)
    result = engine.run_once()
    assert result.skipped is True
    assert "strategy_error" in result.skip_reason
    assert len(guard.errors) == 1


def test_rejected_order_counts_and_does_not_append():
    oms = _FakeOMS(returns=[None])  # submit devuelve None = rechazo
    engine = _engine(strategy=_FakeStrategy([_signal()]), oms=oms)
    result = engine.run_once()
    assert result.orders_submitted == 0
    assert result.orders_rejected == 1
    assert result.orders == []


def test_unfilled_order_counts_as_rejected():
    oms = _FakeOMS(returns=[_order(OrderStatus.PENDING)])
    engine = _engine(strategy=_FakeStrategy([_signal()]), oms=oms)
    result = engine.run_once()
    assert result.orders_submitted == 1
    assert result.orders_filled == 0
    assert result.orders_rejected == 1


def test_guard_record_success_on_filled():
    guard = _FakeGuard()
    oms = _FakeOMS(returns=[_order(OrderStatus.FILLED)])
    engine = _engine(strategy=_FakeStrategy([_signal()]), oms=oms, guard=guard)
    engine.run_once()
    assert guard.successes == 1


def test_oms_summary_delegates():
    assert _engine().oms_summary == {"open": 0, "filled": 1}


def test_engine_result_status_transitions():
    r = EngineResult(symbol="BTC/USDT", timeframe="1h")
    assert r.status == "no_signal"
    r.skipped = True
    assert r.status == "skipped"
    r2 = EngineResult(symbol="BTC/USDT", timeframe="1h", orders_filled=1)
    assert r2.status == "filled"


def test_repr_includes_strategy_symbol_exchange():
    r = repr(_engine())
    assert "fake" in r and "BTC/USDT" in r and "bybit" in r
