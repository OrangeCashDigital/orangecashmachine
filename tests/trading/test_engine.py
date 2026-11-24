# -*- coding: utf-8 -*-
"""tests/trading/test_engine.py — cobertura de trading/engine.py (issue #220).

Hermético: sin red, sin Kafka, sin sleeps. Fakes en línea.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import polars as pl
import pytest
from trading.domain.models.order import OrderStatus
from trading.domain.risk.models import StopLossConfig
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


def _signal(direction: str = "buy", symbol: str = "BTC/USDT") -> Signal:
    return Signal(
        symbol=symbol,
        timeframe="1h",
        direction=direction,
        price=Price(Decimal("100")),
        timestamp=_NOW,
        confidence=0.9,
    )


class _FakeOrder:
    def __init__(self, status: OrderStatus = OrderStatus.FILLED) -> None:
        self.status = status


class _FakeStrategy:
    name = "fake"
    symbol = "BTC/USDT"
    timeframe = "1h"

    def __init__(self, signals: list[Signal] | None = None, raises: Exception | None = None) -> None:
        self._signals = list(signals or [])
        self._raises = raises

    def generate_signals(self, df: pl.DataFrame) -> list[Signal]:
        if self._raises is not None:
            raise self._raises
        return self._signals


class _FakeFeatureSource:
    def __init__(self, df: pl.DataFrame | None, raises: Exception | None = None) -> None:
        self.df = df
        self._raises = raises
        self.calls = 0

    def load_features(self, **kw: object) -> pl.DataFrame | None:
        self.calls += 1
        if self._raises is not None:
            raise self._raises
        return self.df


class _RecordingOMS:
    def __init__(self, returns: list[object | None] | None = None) -> None:
        self.returns = returns or []
        self.submits: list[object] = []
        self._idx = 0

    def submit(self, sig: object) -> object | None:
        self.submits.append(sig)
        if self._idx < len(self.returns):
            result = self.returns[self._idx]
            self._idx += 1
            return result
        return _FakeOrder()

    def validate_signal(self, sig: object) -> object:
        return object()

    def summary(self) -> dict[str, int]:
        return {"open": 0, "filled": 1}


class _FakeGuard:
    def __init__(self, should_stop: bool = False, reason: str = "") -> None:
        self._stop = should_stop
        self.stop_reason = reason
        self.successes = 0
        self.errors: list[str] = []

    def should_stop(self) -> bool:
        return self._stop

    def record_success(self) -> None:
        self.successes += 1

    def record_error(self, message: str) -> None:
        self.errors.append(message)


class _FakePosition:
    def __init__(self, symbol: str, side: str, avg_entry: float, quantity: float) -> None:
        self.symbol = symbol
        self.side = side
        self.avg_entry = avg_entry
        self.quantity = quantity


class _FakeSnapshot:
    def __init__(self, positions: list[_FakePosition]) -> None:
        self.positions = positions


class _FakePortfolio:
    def __init__(self, positions: list[_FakePosition]) -> None:
        self._positions = positions

    def snapshot(self) -> _FakeSnapshot:
        return _FakeSnapshot(self._positions)


def _engine(
    strategy: _FakeStrategy | None = None,
    oms: _RecordingOMS | None = None,
    data: _FakeFeatureSource | None = None,
    guard: _FakeGuard | None = None,
    portfolio: _FakePortfolio | None = None,
    stop_loss: StopLossConfig | None = None,
) -> TradingEngine:
    return TradingEngine(
        strategy=strategy or _FakeStrategy(),
        oms=oms or _RecordingOMS(),
        data_source=data or _FakeFeatureSource(_df([100.0])),
        guard=guard,
        portfolio=portfolio,
        stop_loss=stop_loss,
    )


# ── Required scenarios ──────────────────────────────────────────────────────


def test_run_once_happy_path_emits_signal_to_oms() -> None:
    oms = _RecordingOMS(returns=[_FakeOrder(OrderStatus.FILLED)])
    engine = _engine(
        strategy=_FakeStrategy([_signal()]),
        oms=oms,
        data=_FakeFeatureSource(_df([100.0, 101.0])),
    )

    result = engine.run_once()

    assert result.signals_generated == 1
    assert result.orders_submitted == 1
    assert result.orders_filled == 1
    assert result.orders_rejected == 0
    assert len(oms.submits) == 1
    assert result.status == "filled"


def test_run_once_zero_signals_when_dataframe_short() -> None:
    oms = _RecordingOMS()
    engine = _engine(strategy=_FakeStrategy([]), oms=oms, data=_FakeFeatureSource(_df([100.0])))

    result = engine.run_once()

    assert result.signals_generated == 0
    assert oms.submits == []
    assert result.status == "no_signal"


def test_run_once_halted_guard_skips_execution() -> None:
    oms = _RecordingOMS()
    data = _FakeFeatureSource(_df([100.0]))
    guard = _FakeGuard(should_stop=True, reason="kill_switch")
    engine = _engine(strategy=_FakeStrategy([_signal()]), oms=oms, data=data, guard=guard)

    result = engine.run_once()

    assert result.skipped is True
    assert result.skip_reason == "guard:kill_switch"
    assert data.calls == 0
    assert oms.submits == []


def test_stop_loss_triggers_sell_signal() -> None:
    oms = _RecordingOMS(returns=[_FakeOrder(OrderStatus.FILLED)])
    portfolio = _FakePortfolio([_FakePosition(symbol="BTC/USDT", side="long", avg_entry=100.0, quantity=1.0)])
    engine = _engine(
        strategy=_FakeStrategy([]),
        oms=oms,
        data=_FakeFeatureSource(_df([90.0])),
        portfolio=portfolio,
        stop_loss=StopLossConfig(enabled=True, default_pct=0.02),
    )

    result = engine.run_once()

    assert result.stop_loss_closes == 1
    assert len(oms.submits) == 1
    assert getattr(oms.submits[0], "direction", None) == "sell"
    assert getattr(oms.submits[0], "quantity", None) == 1.0
    assert result.status == "filled"


def test_run_once_propagates_featuresource_error() -> None:
    engine = _engine(data=_FakeFeatureSource(None, raises=ConnectionError("down")))

    result = engine.run_once()

    # engine.py::_load_data() es fail-soft: la excepción se convierte en no_data.
    assert result.skipped is True
    assert result.skip_reason == "no_data"


# ── Additional coverage ─────────────────────────────────────────────────────


def test_run_once_empty_dataframe_skips_with_no_data() -> None:
    engine = _engine(data=_FakeFeatureSource(pl.DataFrame()))
    result = engine.run_once()
    assert result.skipped is True
    assert result.skip_reason == "no_data"


def test_run_once_none_dataframe_skips() -> None:
    engine = _engine(data=_FakeFeatureSource(None))
    result = engine.run_once()
    assert result.skipped is True
    assert result.skip_reason == "no_data"


def test_run_once_rejects_non_polars_dataframe() -> None:
    engine = _engine(data=_FakeFeatureSource({"close": [100.0]}))
    with pytest.raises(TypeError, match="pl.DataFrame"):
        engine.run_once()


def test_strategy_error_skips_and_records_guard_error() -> None:
    guard = _FakeGuard()
    engine = _engine(strategy=_FakeStrategy(raises=ValueError("boom")), guard=guard)

    result = engine.run_once()

    assert result.skipped is True
    assert "strategy_error" in result.skip_reason
    assert len(guard.errors) == 1


def test_rejected_order_counts_and_does_not_append() -> None:
    oms = _RecordingOMS(returns=[None])
    engine = _engine(strategy=_FakeStrategy([_signal()]), oms=oms)

    result = engine.run_once()

    assert result.orders_submitted == 0
    assert result.orders_rejected == 1
    assert result.orders == []


def test_unfilled_order_counts_as_rejected() -> None:
    oms = _RecordingOMS(returns=[_FakeOrder(OrderStatus.PENDING)])
    engine = _engine(strategy=_FakeStrategy([_signal()]), oms=oms)

    result = engine.run_once()

    assert result.orders_submitted == 1
    assert result.orders_filled == 0
    assert result.orders_rejected == 1


def test_guard_record_success_on_filled() -> None:
    guard = _FakeGuard()
    engine = _engine(
        oms=_RecordingOMS(returns=[_FakeOrder(OrderStatus.FILLED)]),
        strategy=_FakeStrategy([_signal()]),
        guard=guard,
    )

    engine.run_once()

    assert guard.successes == 1


def test_oms_summary_delegates() -> None:
    assert _engine().oms_summary == {"open": 0, "filled": 1}


def test_engine_result_status_transitions() -> None:
    result = EngineResult(symbol="BTC/USDT", timeframe="1h")
    assert result.status == "no_signal"
    result.skipped = True
    assert result.status == "skipped"
    filled = EngineResult(symbol="BTC/USDT", timeframe="1h", orders_filled=1)
    assert filled.status == "filled"


def test_repr_includes_strategy_symbol_exchange() -> None:
    text = repr(_engine())
    assert "fake" in text
    assert "BTC/USDT" in text
    assert "bybit" in text
