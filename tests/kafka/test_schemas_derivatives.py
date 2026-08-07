# -*- coding: utf-8 -*-
"""
tests/kafka/test_schemas_derivatives.py
=========================================

Tests de wire schemas de derivados — FundingRatePayload, OpenInterestPayload,
LiquidationPayload.

Sin dependencias externas. Verifica: round-trip, fail-fast por versión y por
side desconocido (liquidations), preservación de campos None e immutabilidad
(frozen).

Regresión Fase 1: aliases de versión canónicos.

PROVENANCE (Contract Provenance, gancho F3 → ADR-0017)
------------------------------------------------------
  FundingRatePayload   — UPSTREAM_LIBRARY(CCXT) para timestamp/funding_rate.
                          ASSUMED para interval_h, predicted_rate, next_funding_ms
                          (CCXT fetch_funding_rate solo entrega ts+rate; los
                          demás son proyección de dominio sin fuente).
  OpenInterestPayload   — UPSTREAM_LIBRARY(CCXT) para timestamp/open_interest_contracts.
                          ASSUMED para open_interest_value, mark_price (derivados
                          sin emisor real).
  LiquidationPayload    — ASSUMED (orphan): no hay productor/stream (WsLiquidationsStream)
                          ni conversion raw → payload; on_liquidation es código muerto.
"""

from __future__ import annotations

import pytest

from shared.kafka.schemas.funding import (
    FUNDING_RATE_SCHEMA_VERSION,
    FundingRatePayload,
    FundingSchemaVersionError,
)
from shared.kafka.schemas.liquidations import (
    LIQUIDATION_SCHEMA_VERSION,
    LiquidationPayload,
    LiquidationSchemaVersionError,
)
from shared.kafka.schemas.oi import (
    OPEN_INTEREST_SCHEMA_VERSION,
    OISchemaVersionError,
    OpenInterestPayload,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _funding(event_id: str = "fund-001") -> FundingRatePayload:
    return FundingRatePayload(
        event_id=event_id,
        occurred_at="2026-01-01T00:00:00+00:00",
        exchange="bybit",
        symbol="BTC/USDT",
        market_type="linear",
        timestamp_ms=1_700_000_000_000,
        funding_rate="0.0001",
        next_funding_ms=1_700_002_880_000,
        interval_h=8,
        predicted_rate="0.0002",
    )


def _oi(event_id: str = "oi-001") -> OpenInterestPayload:
    return OpenInterestPayload(
        event_id=event_id,
        occurred_at="2026-01-01T00:00:00+00:00",
        exchange="bybit",
        symbol="BTC/USDT",
        market_type="linear",
        timestamp_ms=1_700_000_000_000,
        open_interest_contracts="1250.5",
        open_interest_value="37800000.0",
        mark_price="30250.5",
    )


def _liquidation(event_id: str = "liq-001", side: str = "buy") -> LiquidationPayload:
    return LiquidationPayload(
        event_id=event_id,
        occurred_at="2026-01-01T00:00:00+00:00",
        exchange="bybit",
        symbol="BTC/USDT",
        market_type="linear",
        timestamp_ms=1_700_000_000_000,
        price="29850.0",
        quantity="3.2",
        quantity_usd="95520.0",
        side=side,
        order_type="market",
    )


# ---------------------------------------------------------------------------
# FundingRatePayload
# ---------------------------------------------------------------------------


class TestFundingRatePayload:
    def test_version_alias_matches_classvar(self):
        assert FUNDING_RATE_SCHEMA_VERSION == FundingRatePayload.SCHEMA_VERSION

    def test_error_alias_is_canonical(self):
        assert FundingSchemaVersionError.__mro__[1] is ValueError

    def test_immutable(self):
        payload = _funding()
        with pytest.raises((AttributeError, TypeError)):
            payload.funding_rate = "0.99"  # type: ignore

    def test_to_dict_contains_wire_fields(self):
        d = _funding().to_dict()
        assert set(d.keys()) == {
            "event_id",
            "event_version",
            "occurred_at",
            "exchange",
            "symbol",
            "market_type",
            "timestamp_ms",
            "funding_rate",
            "next_funding_ms",
            "interval_h",
            "predicted_rate",
        }

    def test_round_trip(self):
        original = _funding("fund-rt")
        recovered = FundingRatePayload.from_dict(original.to_dict())
        assert recovered == original

    def test_round_trip_preserves_rate_string(self):
        original = _funding()
        recovered = FundingRatePayload.from_dict(original.to_dict())
        assert recovered.funding_rate == "0.0001"
        assert recovered.next_funding_ms == 1_700_002_880_000
        assert recovered.interval_h == 8

    def test_none_fields_preserved(self):
        payload = FundingRatePayload(event_id="fund-none", symbol="BTC/USDT")
        recovered = FundingRatePayload.from_dict(payload.to_dict())
        assert recovered.next_funding_ms is None
        assert recovered.predicted_rate is None
        assert recovered.interval_h is None

    def test_incompatible_version_raises(self):
        d = _funding().to_dict()
        d["event_version"] = FundingRatePayload.SCHEMA_VERSION + 99
        with pytest.raises(FundingSchemaVersionError):
            FundingRatePayload.from_dict(d)


# ---------------------------------------------------------------------------
# OpenInterestPayload
# ---------------------------------------------------------------------------


class TestOpenInterestPayload:
    def test_version_alias_matches_classvar(self):
        assert OPEN_INTEREST_SCHEMA_VERSION == OpenInterestPayload.SCHEMA_VERSION

    def test_error_alias_is_canonical(self):
        assert OISchemaVersionError.__mro__[1] is ValueError

    def test_round_trip(self):
        original = _oi("oi-rt")
        recovered = OpenInterestPayload.from_dict(original.to_dict())
        assert recovered == original

    def test_round_trip_preserves_values(self):
        original = _oi()
        recovered = OpenInterestPayload.from_dict(original.to_dict())
        assert recovered.open_interest_contracts == "1250.5"
        assert recovered.open_interest_value == "37800000.0"
        assert recovered.mark_price == "30250.5"

    def test_none_fields_preserved(self):
        payload = OpenInterestPayload(event_id="oi-none", symbol="BTC/USDT")
        recovered = OpenInterestPayload.from_dict(payload.to_dict())
        assert recovered.open_interest_value is None
        assert recovered.mark_price is None

    def test_incompatible_version_raises(self):
        d = _oi().to_dict()
        d["event_version"] = OpenInterestPayload.SCHEMA_VERSION + 99
        with pytest.raises(OISchemaVersionError):
            OpenInterestPayload.from_dict(d)


# ---------------------------------------------------------------------------
# LiquidationPayload
# ---------------------------------------------------------------------------


class TestLiquidationPayload:
    def test_version_alias_matches_classvar(self):
        assert LIQUIDATION_SCHEMA_VERSION == LiquidationPayload.SCHEMA_VERSION

    def test_error_alias_is_canonical(self):
        assert LiquidationSchemaVersionError.__mro__[1] is ValueError

    def test_round_trip(self):
        original = _liquidation("liq-rt")
        recovered = LiquidationPayload.from_dict(original.to_dict())
        assert recovered == original

    def test_round_trip_preserves_values(self):
        original = _liquidation()
        recovered = LiquidationPayload.from_dict(original.to_dict())
        assert recovered.price == "29850.0"
        assert recovered.quantity == "3.2"
        assert recovered.quantity_usd == "95520.0"
        assert recovered.side == "buy"

    def test_incompatible_version_raises(self):
        d = _liquidation().to_dict()
        d["event_version"] = LiquidationPayload.SCHEMA_VERSION + 99
        with pytest.raises(LiquidationSchemaVersionError):
            LiquidationPayload.from_dict(d)

    def test_invalid_side_raises(self):
        d = _liquidation().to_dict()
        d["side"] = "sideways"
        with pytest.raises(LiquidationSchemaVersionError, match="side"):
            LiquidationPayload.from_dict(d)

    def test_sell_side_is_short_liquidation(self):
        d = _liquidation(side="sell").to_dict()
        assert LiquidationPayload.from_dict(d).side == "sell"

    def test_default_order_type_is_market(self):
        d = _liquidation().to_dict()
        del d["order_type"]
        assert LiquidationPayload.from_dict(d).order_type == "market"


__all__ = []
