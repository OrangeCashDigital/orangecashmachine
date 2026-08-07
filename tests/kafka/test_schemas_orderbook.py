# -*- coding: utf-8 -*-
"""
tests/kafka/test_schemas_orderbook.py
======================================

Tests de wire schemas de order book L2 — OrderBookSnapshotPayload,
OrderBookDeltaPayload.

Sin dependencias externas. Verifica: round-trip, fail-fast por versión y por
side desconocido, payload_type discriminador e immutabilidad (frozen).

Regresión Fase 1: aliases de versión canónicos.

PROVENANCE (Contract Provenance — Protocol Discovery Framework (ADR-0017, F2.5))
------------------------------------------------
- OrderBookSnapshotPayload / OrderBookDeltaPayload → PROTOCOL con matiz:
    Tráfico Bybit WS OBSERVADO (confirmado en vivo via cryptofeed; ver
    cryptofeed_orderbook_stream.py y este test). La forma del campo llega vía
    cryptofeed (UPSTREAM_LIBRARY) que normaliza el wire, por eso se anota
    PROTOCOL(observado) + UPSTREAM_LIBRARY(normalización library). El campo
    checksum es opcional y puede ser None (exhange no siempre lo expone).
"""

from __future__ import annotations

import pytest

from shared.kafka.schemas.orderbook import (
    ORDERBOOK_DELTA_SCHEMA_VERSION,
    ORDERBOOK_SNAPSHOT_SCHEMA_VERSION,
    OrderBookDeltaPayload,
    OrderBookSchemaVersionError,
    OrderBookSnapshotPayload,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _snapshot(event_id: str = "ob-snap-001") -> OrderBookSnapshotPayload:
    return OrderBookSnapshotPayload(
        event_id=event_id,
        occurred_at="2026-01-01T00:00:00+00:00",
        exchange="bybit",
        symbol="BTC/USDT",
        timestamp_ms=1_700_000_000_000,
        bids=[("30250.0", "0.5"), ("30200.0", "1.2")],
        asks=[("30300.0", "0.7"), ("30350.0", "0.9")],
        depth=2,
        checksum=1_234_567,
    )


def _delta(event_id: str = "ob-delta-001", side: str = "bid") -> OrderBookDeltaPayload:
    return OrderBookDeltaPayload(
        event_id=event_id,
        occurred_at="2026-01-01T00:00:00+00:00",
        exchange="bybit",
        symbol="BTC/USDT",
        timestamp_ms=1_700_000_000_100,
        side=side,
        price="30250.0",
        size="0.0",
    )


# ---------------------------------------------------------------------------
# OrderBookSnapshotPayload
# ---------------------------------------------------------------------------


class TestOrderBookSnapshotPayload:
    def test_version_alias_matches_classvar(self):
        assert ORDERBOOK_SNAPSHOT_SCHEMA_VERSION == OrderBookSnapshotPayload.SCHEMA_VERSION

    def test_error_alias_is_canonical(self):
        assert OrderBookSchemaVersionError.__mro__[1] is ValueError

    def test_immutable(self):
        payload = _snapshot()
        with pytest.raises((AttributeError, TypeError)):
            payload.depth = 99  # type: ignore

    def test_to_dict_contains_wire_fields(self):
        d = _snapshot().to_dict()
        assert set(d.keys()) == {
            "event_id",
            "event_version",
            "occurred_at",
            "payload_type",
            "exchange",
            "symbol",
            "timestamp_ms",
            "bids",
            "asks",
            "depth",
            "checksum",
        }

    def test_payload_type_is_snapshot(self):
        assert _snapshot().to_dict()["payload_type"] == "snapshot"

    def test_round_trip(self):
        original = _snapshot("ob-snap-rt")
        recovered = OrderBookSnapshotPayload.from_dict(original.to_dict())
        assert recovered == original

    def test_round_trip_preserves_levels_as_tuples(self):
        original = _snapshot()
        recovered = OrderBookSnapshotPayload.from_dict(original.to_dict())
        assert recovered.bids == [("30250.0", "0.5"), ("30200.0", "1.2")]
        assert recovered.asks == [("30300.0", "0.7"), ("30350.0", "0.9")]
        assert all(isinstance(lvl, tuple) for lvl in recovered.bids)

    def test_accepts_levels_as_lists(self):
        """Wire JSON serializa tuplas como listas — from_dict normaliza."""
        d = _snapshot().to_dict()
        d["bids"] = [["30100.0", "0.25"], ["30050.0", "1.0"]]
        recovered = OrderBookSnapshotPayload.from_dict(d)
        assert recovered.bids == [("30100.0", "0.25"), ("30050.0", "1.0")]

    def test_incompatible_version_raises(self):
        d = _snapshot().to_dict()
        d["event_version"] = OrderBookSnapshotPayload.SCHEMA_VERSION + 99
        with pytest.raises(OrderBookSchemaVersionError):
            OrderBookSnapshotPayload.from_dict(d)

    def test_checksum_none_default(self):
        d = _snapshot().to_dict()
        del d["checksum"]
        assert OrderBookSnapshotPayload.from_dict(d).checksum is None


# ---------------------------------------------------------------------------
# OrderBookDeltaPayload
# ---------------------------------------------------------------------------


class TestOrderBookDeltaPayload:
    def test_version_alias_matches_classvar(self):
        assert ORDERBOOK_DELTA_SCHEMA_VERSION == OrderBookDeltaPayload.SCHEMA_VERSION

    def test_payload_type_is_delta(self):
        assert _delta().to_dict()["payload_type"] == "delta"

    def test_round_trip(self):
        original = _delta("ob-delta-rt")
        recovered = OrderBookDeltaPayload.from_dict(original.to_dict())
        assert recovered == original

    def test_incompatible_version_raises(self):
        d = _delta().to_dict()
        d["event_version"] = OrderBookDeltaPayload.SCHEMA_VERSION + 99
        with pytest.raises(OrderBookSchemaVersionError):
            OrderBookDeltaPayload.from_dict(d)

    def test_invalid_side_raises(self):
        d = _delta().to_dict()
        d["side"] = "middle"
        with pytest.raises(OrderBookSchemaVersionError, match="side"):
            OrderBookDeltaPayload.from_dict(d)

    def test_side_bid_and_ask(self):
        assert _delta(side="bid").to_dict()["side"] == "bid"
        assert _delta(side="ask").to_dict()["side"] == "ask"

    def test_zero_size_means_delete_level(self):
        d = _delta().to_dict()
        assert d["size"] == "0.0"


__all__ = []
