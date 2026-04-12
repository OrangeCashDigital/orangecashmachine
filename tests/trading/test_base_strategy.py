# -*- coding: utf-8 -*-
"""
tests/trading/test_base_strategy.py
=====================================

Tests unitarios de Signal y BaseStrategy.
Sin I/O — lógica pura.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from trading.strategies.base import Signal, SignalType


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_signal(signal: SignalType = "buy", confidence: float = 1.0) -> Signal:
    return Signal(
        symbol     = "BTC/USDT",
        timeframe  = "1h",
        signal     = signal,
        price      = 50_000.0,
        timestamp  = datetime(2024, 1, 1, tzinfo=timezone.utc),
        confidence = confidence,
    )


# ── Signal — construcción e invariantes ───────────────────────────────────────

def test_signal_buy_is_actionable():
    assert _make_signal("buy").is_actionable is True


def test_signal_sell_is_actionable():
    assert _make_signal("sell").is_actionable is True


def test_signal_hold_is_not_actionable():
    assert _make_signal("hold").is_actionable is False


def test_signal_confidence_out_of_range_raises():
    with pytest.raises(ValueError, match="confidence"):
        _make_signal(confidence=1.1)


def test_signal_confidence_negative_raises():
    with pytest.raises(ValueError, match="confidence"):
        _make_signal(confidence=-0.1)


def test_signal_confidence_boundary_values_accepted():
    assert _make_signal(confidence=0.0).confidence == 0.0
    assert _make_signal(confidence=1.0).confidence == 1.0


def test_signal_metadata_defaults_to_empty_dict():
    s = _make_signal()
    assert s.metadata == {}


def test_signal_metadata_is_isolated_between_instances():
    """Dos Signal no deben compartir el mismo dict de metadata."""
    s1 = _make_signal()
    s2 = _make_signal()
    s1.metadata["key"] = "value"
    assert "key" not in s2.metadata
