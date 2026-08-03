# -*- coding: utf-8 -*-
"""
tests/kafka/test_schemas_trades.py
====================================

Tests de wire schemas de microestructura — TradePayload, TradeSeriesPayload.

Sin dependencias externas. Verifica: round-trip, fail-fast por versión y por
source desconocido, fail-soft en side, helpers Kappa e immutabilidad (frozen).

Regresión Fase 1: literales SSOT (source) y aliases de versión canónicos.
"""

from __future__ import annotations

import pytest

from shared.kafka.schemas.trades import (
    TRADE_SCHEMA_VERSION,
    TRADE_SERIES_SCHEMA_VERSION,
    TradePayload,
    TradeSchemaVersionError,
    TradeSeriesPayload,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _trade(event_id: str = "trade-001", source: str = "live", side: str = "buy") -> TradePayload:
    return TradePayload(
        event_id=event_id,
        occurred_at="2026-01-01T00:00:00+00:00",
        exchange="bybit",
        market_type="linear",
        symbol="BTC/USDT",
        trade_id="tx-000001",
        timestamp_ms=1_700_000_000_000,
        price="30250.5",
        amount="0.013",
        side=side,
        source=source,
        run_id="run-001",
        meta={"feed": "ws"},
    )


def _series(event_id: str = "series-001", source: str = "backfill") -> TradeSeriesPayload:
    return TradeSeriesPayload(
        event_id=event_id,
        occurred_at="2026-01-01T00:00:00+00:00",
        exchange="bybit",
        market_type="linear",
        symbol="BTC/USDT",
        window_start_ms=1_700_000_000_000,
        window_end_ms=1_700_000_036_000,
        trade_count=42,
        vwap="30245.123",
        total_volume="1.234",
        total_cost="37322.0",
        buy_volume="0.800",
        sell_volume="0.434",
        buy_sell_imbalance=0.30,
        open_price="30200.0",
        close_price="30250.5",
        high_price="30300.0",
        low_price="30150.0",
        source=source,
        run_id="run-001",
        meta=None,
    )


# ---------------------------------------------------------------------------
# TradePayload
# ---------------------------------------------------------------------------


class TestTradePayload:
    def test_version_alias_matches_classvar(self):
        assert TRADE_SCHEMA_VERSION == TradePayload.SCHEMA_VERSION

    def test_error_alias_is_canonical(self):
        assert TradeSchemaVersionError.__mro__[1] is ValueError

    def test_immutable(self):
        payload = _trade()
        with pytest.raises((AttributeError, TypeError)):
            payload.price = "99999.0"  # type: ignore

    def test_to_dict_contains_wire_fields(self):
        d = _trade().to_dict()
        assert set(d.keys()) == {
            "event_id",
            "event_version",
            "occurred_at",
            "exchange",
            "market_type",
            "symbol",
            "trade_id",
            "timestamp_ms",
            "price",
            "amount",
            "side",
            "source",
            "run_id",
            "meta",
        }

    def test_round_trip(self):
        original = _trade("trade-rt")
        recovered = TradePayload.from_dict(original.to_dict())
        assert recovered == original

    def test_round_trip_preserves_decimal_strings(self):
        original = _trade()
        recovered = TradePayload.from_dict(original.to_dict())
        assert recovered.price == "30250.5"
        assert recovered.amount == "0.013"

    def test_missing_version_defaults_to_v1(self):
        d = _trade().to_dict()
        del d["event_version"]
        assert TradePayload.from_dict(d).SCHEMA_VERSION == 1

    def test_incompatible_version_raises(self):
        d = _trade().to_dict()
        d["event_version"] = TradePayload.SCHEMA_VERSION + 99
        with pytest.raises(TradeSchemaVersionError):
            TradePayload.from_dict(d)

    def test_unknown_source_raises(self):
        d = _trade().to_dict()
        d["source"] = "cached"
        with pytest.raises(TradeSchemaVersionError, match="source"):
            TradePayload.from_dict(d)

    def test_invalid_side_fails_soft_to_unknown(self):
        d = _trade().to_dict()
        d["side"] = "sideways"
        assert TradePayload.from_dict(d).side == "unknown"

    def test_source_live_helpers(self):
        payload = _trade(source="live")
        assert payload.is_live
        assert not payload.is_backfill
        assert not payload.is_replay

    def test_source_backfill_helpers(self):
        payload = _trade(source="backfill")
        assert payload.is_backfill

    def test_source_replay_helpers(self):
        payload = _trade(source="replay")
        assert payload.is_replay


# ---------------------------------------------------------------------------
# TradeSeriesPayload
# ---------------------------------------------------------------------------


class TestTradeSeriesPayload:
    def test_version_alias_matches_classvar(self):
        assert TRADE_SERIES_SCHEMA_VERSION == TradeSeriesPayload.SCHEMA_VERSION

    def test_round_trip(self):
        original = _series("series-rt")
        recovered = TradeSeriesPayload.from_dict(original.to_dict())
        assert recovered == original

    def test_round_trip_preserves_microstructure(self):
        original = _series()
        recovered = TradeSeriesPayload.from_dict(original.to_dict())
        assert recovered.vwap == "30245.123"
        assert recovered.buy_sell_imbalance == 0.30
        assert recovered.trade_count == 42

    def test_incompatible_version_raises(self):
        d = _series().to_dict()
        d["event_version"] = TradeSeriesPayload.SCHEMA_VERSION + 99
        with pytest.raises(TradeSchemaVersionError):
            TradeSeriesPayload.from_dict(d)

    def test_unknown_source_raises(self):
        d = _series().to_dict()
        d["source"] = "cached"
        with pytest.raises(TradeSchemaVersionError, match="source"):
            TradeSeriesPayload.from_dict(d)

    def test_source_default_is_live(self):
        d = _series().to_dict()
        del d["source"]
        assert TradeSeriesPayload.from_dict(d).is_live


__all__ = []
