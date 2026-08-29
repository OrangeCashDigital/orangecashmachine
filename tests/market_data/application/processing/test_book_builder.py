# -*- coding: utf-8 -*-
"""
tests/market_data/application/processing/test_book_builder.py
==============================================================
Tests del use case puro BookBuilder (schema v2).

Cubre: snapshot → base, delta atómico multinivel, gap por update_id (D-7b),
delta antes de snapshot, cantidad negativa → invalidar, viewport (D-7d),
staleness y decisión Decimal (D-7c). Sin dependencias de Kafka/infraestructura.
"""

from __future__ import annotations

import pytest
from market_data.application.processing.book_builder import (
    BookBuilder,
    OutcomeKind,
)

EX = "bybit"
SYM = "BTC/USDT"


def _snap(builder, update_id=1_000, ts=1_700_000_000_000, bids=None, asks=None):
    return builder.on_snapshot(
        EX,
        SYM,
        ts,
        bids or [("30250.0", "0.5"), ("30200.0", "1.2")],
        asks or [("30300.0", "0.7")],
        update_id=update_id,
    )


def _delta(builder, update_id, bids=None, asks=None, ts=1_700_000_000_100):
    return builder.on_delta(
        EX,
        SYM,
        ts,
        bids or [],
        asks or [],
        update_id=update_id,
    )


class TestSnapshot:
    def test_snapshot_sets_base_and_emits(self):
        b = BookBuilder()
        out = _snap(b, update_id=42)
        assert out.kind is OutcomeKind.SNAPSHOT_APPLIED
        assert out.publishes
        assert out.update_id == 42
        assert out.bids[0] == ("30250.0", "0.5")

    def test_snapshot_resets_previous_state(self):
        b = BookBuilder()
        _snap(b, update_id=10, bids=[("100.0", "1.0")], asks=[("101.0", "2.0")])
        out = _snap(b, update_id=11, bids=[("200.0", "3.0")], asks=[("201.0", "4.0")])
        assert out.bids == [("200.0", "3.0")]

    def test_snapshot_overwrites_delta_state(self):
        b = BookBuilder()
        _snap(b, update_id=1)
        _delta(b, update_id=2, bids=[("30200.0", "9.9")])
        out = _snap(b, update_id=3, asks=[("400.0", "1.0")])
        assert out.asks == [("400.0", "1.0")]


class TestDelta:
    def test_delta_before_snapshot_discarded(self):
        b = BookBuilder()
        out = _delta(b, update_id=2)
        assert out.kind is OutcomeKind.DELTA_BEFORE_SNAPSHOT
        assert not out.publishes

    def test_contiguous_delta_applied(self):
        b = BookBuilder()
        _snap(b, update_id=1)
        out = _delta(b, update_id=2, bids=[("30200.0", "1.5")])
        assert out.kind is OutcomeKind.DELTA_APPLIED
        # "30250.0" sigue, "30200.0" pasó de 1.2 → 1.5
        assert ("30250.0", "0.5") in out.bids
        assert ("30200.0", "1.5") in out.bids

    def test_delete_level_via_zero(self):
        b = BookBuilder()
        _snap(b, update_id=1)
        out = _delta(b, update_id=2, bids=[("30250.0", "0.0")])
        assert ("30250.0", "0.0") not in out.bids
        assert ("30200.0", "1.2") in out.bids

    def test_multilevel_atomic_apply(self):
        b = BookBuilder()
        _snap(b, update_id=1)
        out = _delta(
            b,
            update_id=2,
            bids=[("30250.0", "0.0"), ("30200.0", "1.5"), ("30100.0", "2.0")],
            asks=[("30300.0", "0.9")],
        )
        assert out.bids == [("30200.0", "1.5"), ("30100.0", "2.0")]
        assert out.asks == [("30300.0", "0.9")]

    def test_gap_detected_and_invalidates(self):
        b = BookBuilder()
        _snap(b, update_id=100)
        out = _delta(b, update_id=105)  # esperaba 101
        assert out.kind is OutcomeKind.GAP_DETECTED
        assert not out.publishes
        assert "dE=4" in out.detail
        # estado invalidado → el siguiente delta se descarta hasta snapshot
        out2 = _delta(b, update_id=106)
        assert out2.kind is OutcomeKind.DELTA_BEFORE_SNAPSHOT

    def test_negative_quantity_invalidates(self):
        b = BookBuilder()
        _snap(b, update_id=1)
        out = _delta(b, update_id=2, bids=[("30000.0", "-1.0")])
        assert out.kind is OutcomeKind.STRUCTURAL_INVALID
        assert not out.publishes


class TestDecimalPrecision:
    def test_price_precision_preserved(self):
        b = BookBuilder()
        _snap(b, update_id=1, bids=[("0.00001234", "1.0000000001")], asks=[("0.00001235", "2.0")])
        out = _delta(b, update_id=2, bids=[("0.00001234", "0.0000000002")])
        assert ("0.00001234", "0.0000000002") in out.bids


class TestViewport:
    def test_viewport_trims_to_n(self):
        b = BookBuilder(viewport=1)
        _snap(b, update_id=1, bids=[("300.0", "1.0"), ("299.0", "2.0")], asks=[("301.0", "3.0"), ("302.0", "4.0")])
        assert b.book_state(EX, SYM) == ([("300.0", "1.0")], [("301.0", "3.0")])

    def test_zero_viewport_unlimited(self):
        b = BookBuilder(viewport=0)
        _snap(b, update_id=1, bids=[("300.0", "1.0"), ("299.0", "2.0")])
        bids, _ = b.book_state(EX, SYM)
        assert len(bids) == 2


class TestStale:
    def test_stale_detected(self):
        b = BookBuilder(stale_ms=100)
        _snap(b, update_id=1, ts=1_000)
        stale = b.check_stale(now_ms=1_200)
        assert len(stale) == 1
        assert stale[0].kind is OutcomeKind.STALE

    def test_not_stale_within_window(self):
        b = BookBuilder(stale_ms=100)
        _snap(b, update_id=1, ts=1_000)
        assert b.check_stale(now_ms=1_050) == []

    def test_no_snapshot_not_stale(self):
        b = BookBuilder(stale_ms=100)
        assert b.check_stale(now_ms=1_200) == []


def test_invalid_constructor_args():
    with pytest.raises(ValueError):
        BookBuilder(viewport=-1)
    with pytest.raises(ValueError):
        BookBuilder(stale_ms=-1)


__all__ = []
