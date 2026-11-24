# -*- coding: utf-8 -*-
"""tests/trading/test_risk_manager.py — cobertura de trading/risk/manager.py (issue #220)."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest
from trading.domain.risk.models import (
    DrawdownConfig,
    OrderLimits,
    PositionConfig,
    RiskConfig,
    SignalFilterConfig,
)
from trading.risk.manager import RiskManager

from shared.types.signal import Signal
from shared.types.values import Price

_NOW = datetime(2024, 1, 1, tzinfo=timezone.utc)


def _signal(direction: str = "buy", confidence: float = 0.9) -> Signal:
    return Signal(
        symbol="BTC/USDT",
        timeframe="1h",
        direction=direction,
        price=Price(Decimal("100")),
        timestamp=_NOW,
        confidence=confidence,
    )


# ── Required scenarios ──────────────────────────────────────────────────────


def test_sizing_within_limits_returns_approved() -> None:
    cfg = RiskConfig(
        position=PositionConfig(max_position_pct=0.05),
        order=OrderLimits(min_order_usd=10.0, max_order_usd=1000.0),
    )
    risk = RiskManager(config=cfg, capital_usd=10_000.0)

    decision = risk.validate(_signal())

    assert decision.approved is True
    assert decision.reason == "ok"
    assert decision.size_pct == pytest.approx(0.05)


def test_sizing_exceeds_max_order_usd_rejects() -> None:
    """El nombre del escenario se conserva; la producción clampéa en vez de rechazar."""
    cfg = RiskConfig(
        position=PositionConfig(max_position_pct=0.05),
        order=OrderLimits(min_order_usd=10.0, max_order_usd=400.0),
    )
    risk = RiskManager(config=cfg, capital_usd=10_000.0)

    decision = risk.validate(_signal())

    assert decision.approved is True
    assert decision.size_pct == pytest.approx(400.0 / 10_000.0)
    assert decision.size_pct < cfg.position.max_position_pct


def test_drawdown_triggers_halt() -> None:
    cfg = RiskConfig(
        drawdown=DrawdownConfig(
            max_daily_drawdown_pct=0.05,
            max_total_drawdown_pct=0.15,
            halt_on_breach=True,
        )
    )
    risk = RiskManager(config=cfg, capital_usd=10_000.0)

    risk.record_close(pnl_usd=-600.0)

    assert risk.is_halted is True
    decision = risk.validate(_signal())
    assert decision.approved is False
    assert "system_halted" in decision.reason


def test_min_confidence_filter_rejects_low_signal() -> None:
    cfg = RiskConfig(signal_filter=SignalFilterConfig(min_confidence=0.8))
    risk = RiskManager(config=cfg, capital_usd=10_000.0)

    decision = risk.validate(_signal(confidence=0.5))

    assert decision.rejected is True
    assert decision.reason.startswith("confidence_too_low")


def test_kill_switch_halts_all_signals() -> None:
    risk = RiskManager(config=RiskConfig(), capital_usd=10_000.0)

    risk.halt("manual")

    assert risk.is_halted is True
    for direction in ("buy", "sell"):
        decision = risk.validate(_signal(direction=direction))
        assert decision.approved is False
        assert "system_halted:manual" in decision.reason


# ── Additional coverage ─────────────────────────────────────────────────────


def test_record_position_reflects_real_exposure() -> None:
    risk = RiskManager(config=RiskConfig(), capital_usd=10_000.0)

    risk.record_position("BTC/USDT", 0.37, 49_800.0)

    assert risk.quantity("BTC/USDT") == pytest.approx(0.37)
    assert risk.open_positions == 1
    assert risk.exposure_usd == pytest.approx(0.37 * 49_800.0)
    assert risk.positions_unknown_price == 0


def test_unknown_entry_price_is_not_zero_exposure() -> None:
    risk = RiskManager(config=RiskConfig(), capital_usd=10_000.0)

    risk.record_position("BTC/USDT", 0.5, None)

    assert risk.quantity("BTC/USDT") == pytest.approx(0.5)
    assert risk.positions_unknown_price == 1
    assert risk.exposure_usd == 0.0
    assert risk.state()["positions_unknown_price"] == 1


def test_reset_daily_and_total_restores_state() -> None:
    risk = RiskManager(config=RiskConfig(), capital_usd=10_000.0)

    risk.record_close(pnl_usd=100.0)
    assert risk.state()["daily_pnl_usd"] == pytest.approx(100.0)

    risk.reset_daily()
    assert risk.state()["daily_pnl_usd"] == pytest.approx(0.0)
    assert risk.state()["total_pnl_usd"] == pytest.approx(100.0)

    risk.reset_total()
    assert risk.state()["total_pnl_usd"] == pytest.approx(0.0)
    assert risk.is_halted is False


def test_validate_internal_error_returns_rejection() -> None:
    class _BrokenSignal:
        direction = "buy"
        confidence = 0.9

        @property
        def is_actionable(self) -> bool:
            raise RuntimeError("boom")

    risk = RiskManager(config=RiskConfig(), capital_usd=10_000.0)

    decision = risk.validate(_BrokenSignal())

    assert decision.approved is False
    assert "internal_error:boom" in decision.reason
