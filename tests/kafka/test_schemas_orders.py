# -*- coding: utf-8 -*-
"""
tests/kafka/test_schemas_orders.py
====================================

Tests de wire schemas de órdenes — OrderFilledPayload, OrderRejectedPayload.

Sin dependencias externas. Verifica: round-trip, fail-fast por versión
incompatible, immutabilidad (frozen).

Regresión Fase 1: los aliases de error y versión son el SSOT canónico.

PROVENANCE (Contract PICO, gancho F3 → ADR-0017)
------------------------------------------------
- OrderFilledPayload / OrderRejectedPayload → DOMAIN: eventos propios del
  dominio OCM (OMS→portfolio); NO existen en ningún wire de exchange. Estables
  por diseño; no requieren PROTOCOL/DOCUMENTATION.
"""

from __future__ import annotations

import pytest

from shared.kafka.schemas.orders import (
    ORDER_FILLED_SCHEMA_VERSION,
    ORDER_REJECTED_SCHEMA_VERSION,
    OrderFilledPayload,
    OrderRejectedPayload,
    OrderSchemaVersionError,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _filled(event_id: str = "ord-fill-001") -> OrderFilledPayload:
    return OrderFilledPayload(
        event_id=event_id,
        occurred_at="2026-01-01T00:00:00+00:00",
        order_id="a1b2c3d4",
        exchange="bybit",
        symbol="BTC/USDT",
        side="buy",
        fill_price=30_250.5,
        size_pct=0.1,
        filled_at="2026-01-01T00:00:05+00:00",
        signal_event_id="sig-app-001",
        run_id="run-001",
    )


def _rejected(event_id: str = "ord-rej-001") -> OrderRejectedPayload:
    return OrderRejectedPayload(
        event_id=event_id,
        occurred_at="2026-01-01T00:00:00+00:00",
        order_id="b2c3d4e5",
        exchange="bybit",
        symbol="BTC/USDT",
        side="sell",
        reason="insufficient_margin",
        signal_event_id="sig-app-002",
        run_id="run-001",
    )


# ---------------------------------------------------------------------------
# OrderFilledPayload
# ---------------------------------------------------------------------------


class TestOrderFilledPayload:
    def test_version_alias_matches_classvar(self):
        assert ORDER_FILLED_SCHEMA_VERSION == OrderFilledPayload.SCHEMA_VERSION

    def test_error_alias_is_canonical(self):
        assert OrderSchemaVersionError.__mro__[1] is ValueError

    def test_immutable(self):
        payload = _filled()
        with pytest.raises((AttributeError, TypeError)):
            payload.fill_price = 99_999.0  # type: ignore

    def test_to_dict_contains_wire_fields(self):
        d = _filled().to_dict()
        assert set(d.keys()) == {
            "event_id",
            "event_version",
            "occurred_at",
            "order_id",
            "exchange",
            "symbol",
            "side",
            "fill_price",
            "size_pct",
            "filled_at",
            "signal_event_id",
            "run_id",
        }

    def test_round_trip(self):
        original = _filled("ord-fill-rt")
        recovered = OrderFilledPayload.from_dict(original.to_dict())
        assert recovered == original

    def test_missing_version_defaults_to_v1(self):
        d = _filled().to_dict()
        del d["event_version"]
        assert OrderFilledPayload.from_dict(d).SCHEMA_VERSION == 1

    def test_incompatible_version_raises(self):
        d = _filled().to_dict()
        d["event_version"] = OrderFilledPayload.SCHEMA_VERSION + 99
        with pytest.raises(OrderSchemaVersionError):
            OrderFilledPayload.from_dict(d)

    def test_side_preserved(self):
        d = _filled().to_dict()
        assert OrderFilledPayload.from_dict(d).side == "buy"


# ---------------------------------------------------------------------------
# OrderRejectedPayload
# ---------------------------------------------------------------------------


class TestOrderRejectedPayload:
    def test_version_alias_matches_classvar(self):
        assert ORDER_REJECTED_SCHEMA_VERSION == OrderRejectedPayload.SCHEMA_VERSION

    def test_round_trip(self):
        original = _rejected("ord-rej-rt")
        recovered = OrderRejectedPayload.from_dict(original.to_dict())
        assert recovered == original

    def test_round_trip_preserves_reason(self):
        original = _rejected()
        recovered = OrderRejectedPayload.from_dict(original.to_dict())
        assert recovered.reason == "insufficient_margin"
        assert recovered.signal_event_id == "sig-app-002"

    def test_incompatible_version_raises(self):
        d = _rejected().to_dict()
        d["event_version"] = OrderRejectedPayload.SCHEMA_VERSION + 99
        with pytest.raises(OrderSchemaVersionError):
            OrderRejectedPayload.from_dict(d)


__all__ = []
