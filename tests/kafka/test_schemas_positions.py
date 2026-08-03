# -*- coding: utf-8 -*-
"""
tests/kafka/test_schemas_positions.py
======================================

Tests de wire schemas de posiciones — PositionOpenedPayload,
PositionClosedPayload.

Sin dependencias externas. Verifica: round-trip, fail-fast por versión
incompatible, propiedad derivada is_winner e immutabilidad (frozen).

Regresión Fase 1: los aliases de error y versión son el SSOT canónico.
"""

from __future__ import annotations

import pytest

from shared.kafka.schemas.positions import (
    POSITION_CLOSED_SCHEMA_VERSION,
    POSITION_OPENED_SCHEMA_VERSION,
    PositionClosedPayload,
    PositionOpenedPayload,
    PositionSchemaVersionError,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _opened(event_id: str = "pos-open-001") -> PositionOpenedPayload:
    return PositionOpenedPayload(
        event_id=event_id,
        occurred_at="2026-01-01T00:00:00+00:00",
        order_id="a1b2c3d4",
        exchange="bybit",
        symbol="BTC/USDT",
        side="long",
        entry_price=30_250.5,
        size_pct=0.1,
        opened_at="2026-01-01T00:00:05+00:00",
        run_id="run-001",
    )


def _closed(event_id: str = "pos-close-001", pnl_pct: float = 0.08) -> PositionClosedPayload:
    return PositionClosedPayload(
        event_id=event_id,
        occurred_at="2026-01-01T00:00:00+00:00",
        order_id="a1b2c3d4",
        exchange="bybit",
        symbol="BTC/USDT",
        side="long",
        entry_price=30_250.5,
        exit_price=32_670.5,
        size_pct=0.1,
        pnl_pct=pnl_pct,
        opened_at="2026-01-01T00:00:05+00:00",
        closed_at="2026-01-02T00:00:05+00:00",
        run_id="run-001",
    )


# ---------------------------------------------------------------------------
# PositionOpenedPayload
# ---------------------------------------------------------------------------


class TestPositionOpenedPayload:
    def test_version_alias_matches_classvar(self):
        assert POSITION_OPENED_SCHEMA_VERSION == PositionOpenedPayload.SCHEMA_VERSION

    def test_error_alias_is_canonical(self):
        assert PositionSchemaVersionError.__mro__[1] is ValueError

    def test_immutable(self):
        payload = _opened()
        with pytest.raises((AttributeError, TypeError)):
            payload.entry_price = 99_999.0  # type: ignore

    def test_to_dict_contains_wire_fields(self):
        d = _opened().to_dict()
        assert set(d.keys()) == {
            "event_id",
            "event_version",
            "occurred_at",
            "order_id",
            "exchange",
            "symbol",
            "side",
            "entry_price",
            "size_pct",
            "opened_at",
            "run_id",
        }

    def test_round_trip(self):
        original = _opened("pos-open-rt")
        recovered = PositionOpenedPayload.from_dict(original.to_dict())
        assert recovered == original

    def test_missing_version_defaults_to_v1(self):
        d = _opened().to_dict()
        del d["event_version"]
        assert PositionOpenedPayload.from_dict(d).SCHEMA_VERSION == 1

    def test_incompatible_version_raises(self):
        d = _opened().to_dict()
        d["event_version"] = PositionOpenedPayload.SCHEMA_VERSION + 99
        with pytest.raises(PositionSchemaVersionError):
            PositionOpenedPayload.from_dict(d)

    def test_side_long_default(self):
        d = _opened().to_dict()
        del d["side"]
        assert PositionOpenedPayload.from_dict(d).side == "long"


# ---------------------------------------------------------------------------
# PositionClosedPayload
# ---------------------------------------------------------------------------


class TestPositionClosedPayload:
    def test_version_alias_matches_classvar(self):
        assert POSITION_CLOSED_SCHEMA_VERSION == PositionClosedPayload.SCHEMA_VERSION

    def test_round_trip(self):
        original = _closed("pos-close-rt")
        recovered = PositionClosedPayload.from_dict(original.to_dict())
        assert recovered == original

    def test_round_trip_preserves_pnl(self):
        original = _closed()
        recovered = PositionClosedPayload.from_dict(original.to_dict())
        assert recovered.exit_price == 32_670.5
        assert recovered.pnl_pct == 0.08
        assert recovered.closed_at == "2026-01-02T00:00:05+00:00"

    def test_incompatible_version_raises(self):
        d = _closed().to_dict()
        d["event_version"] = PositionClosedPayload.SCHEMA_VERSION + 99
        with pytest.raises(PositionSchemaVersionError):
            PositionClosedPayload.from_dict(d)

    def test_is_winner_positive_pnl(self):
        assert _closed(pnl_pct=0.01).is_winner

    def test_is_winner_negative_pnl(self):
        assert not _closed(pnl_pct=-0.02).is_winner

    def test_is_winner_zero_pnl(self):
        assert not _closed(pnl_pct=0.0).is_winner


__all__ = []
