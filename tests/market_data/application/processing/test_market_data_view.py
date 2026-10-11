# -*- coding: utf-8 -*-
"""
tests/market_data/application/processing/test_market_data_view.py
==================================================================
Tests de la vista pública de lectura D4 — ADR-0028 §3.6
(MarketDataViewPort implementado por BookBuilder).

Cubre: conformidad estructural con el Protocol, mid/bid/ask/spread/depth,
is_ready = has_snapshot AND not_stale (C1), fail-closed sin snapshot y
precisión Decimal (D-7c). Sin dependencias de Kafka/infraestructura.
"""

from __future__ import annotations

from market_data.application.processing.book_builder import BookBuilder
from market_data.ports.outbound.market_data_view import MarketDataViewPort

EX = "bybit"
SYM = "BTC/USDT"

BIDS = [("30250.0", "0.5"), ("30200.0", "1.2")]
ASKS = [("30300.0", "0.7")]


def _ready_builder():
    b = BookBuilder()
    b.on_snapshot(EX, SYM, 1_000, BIDS, ASKS, update_id=1_000)
    return b


class TestConformance:
    def test_builder_implements_port(self):
        assert isinstance(BookBuilder(), MarketDataViewPort)

    def test_stale_threshold_ms_exposed(self):
        assert BookBuilder().stale_threshold_ms == 2_000
        assert BookBuilder(stale_ms=5_000).stale_threshold_ms == 5_000


class TestBestBidAsk:
    def test_best_bid_is_top_of_book(self):
        b = _ready_builder()
        assert b.best_bid(EX, SYM) == "30250.0"
        assert b.best_ask(EX, SYM) == "30300.0"

    def test_none_without_snapshot(self):
        b = BookBuilder()
        assert b.best_bid(EX, SYM) is None
        assert b.best_ask(EX, SYM) is None

    def test_none_for_unknown_symbol(self):
        b = _ready_builder()
        assert b.best_bid(EX, "ETH/USDT") is None


class TestMidAndSpread:
    def test_mid_price_midpoint(self):
        b = _ready_builder()  # (30250 + 30300) / 2
        assert b.mid_price(EX, SYM) == "30275.0"

    def test_spread_is_ask_minus_bid(self):
        b = _ready_builder()  # 30300 - 30250
        assert b.spread(EX, SYM) == "50.0"

    def test_mid_and_spread_none_if_one_side_missing(self):
        b = BookBuilder()
        b.on_snapshot(EX, SYM, 1_000, BIDS, [], update_id=1)
        assert b.mid_price(EX, SYM) is None
        assert b.spread(EX, SYM) is None


class TestDepth:
    def test_depth_counts_levels(self):
        b = _ready_builder()
        assert b.depth(EX, SYM) == 3  # 2 bids + 1 ask

    def test_depth_none_without_book(self):
        b = BookBuilder()
        assert b.depth(EX, SYM) is None

    def test_depth_updates_with_delta(self):
        b = _ready_builder()
        b.on_delta(EX, SYM, 1_100, [("30100.0", "2.0")], [], update_id=1_001)
        assert b.depth(EX, SYM) == 4


class TestIsReady:
    def test_ready_with_fresh_snapshot(self):
        b = _ready_builder()
        assert b.is_ready(EX, SYM, now_ms=1_500) is True

    def test_not_ready_without_snapshot(self):
        b = BookBuilder()
        assert b.is_ready(EX, SYM, now_ms=1_500) is False

    def test_not_ready_when_stale(self):
        b = BookBuilder(stale_ms=100)
        b.on_snapshot(EX, SYM, 1_000, BIDS, ASKS, update_id=1)
        assert b.is_ready(EX, SYM, now_ms=1_050) is True
        assert b.is_ready(EX, SYM, now_ms=1_200) is False

    def test_gap_invalidates_readiness(self):
        b = _ready_builder()
        b.on_delta(EX, SYM, 1_100, [], [], update_id=1_005)  # gap → invalidado
        assert b.is_ready(EX, SYM, now_ms=1_100) is False


class TestDecimalPrecision:
    def test_view_preserves_decimal_precision(self):
        b = BookBuilder()
        b.on_snapshot(
            EX,
            SYM,
            1_000,
            [("0.00001234", "1.0")],
            [("0.00001235", "1.0")],
            update_id=1,
        )
        assert b.best_bid(EX, SYM) == "0.00001234"
        assert b.best_ask(EX, SYM) == "0.00001235"
        assert b.spread(EX, SYM) == "0.00000001"
        assert b.mid_price(EX, SYM) == "0.000012345"


__all__ = []
