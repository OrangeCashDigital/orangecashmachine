# -*- coding: utf-8 -*-
"""
tests/kafka/test_serializer.py
================================

Tests de serializer.py — serialize / deserialize / make_routing_key.

Sin dependencias externas — puro Python.
Verifica: round-trip, Fail-Fast, routing key formato canónico.
"""
from __future__ import annotations

import json
import pytest

from shared.kafka.schemas.ohlcv import (
    EventPayload,
    KafkaOHLCVBar as OHLCVBar,
    OHLCV_SCHEMA_VERSION as PAYLOAD_SCHEMA_VERSION,
    OHLCVSchemaVersionError as SchemaVersionError,
)
from shared.kafka.serializer import (
    serialize,
    deserialize,
    make_routing_key,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_event(event_id: str = "evt-001") -> EventPayload:
    return EventPayload(
        event_id       = event_id,
        exchange       = "bybit",
        symbol         = "BTC/USDT",
        timeframe      = "1h",
        batch_start_ts = 1_700_000_000_000,
        bars           = [
            OHLCVBar(
                ts=1_700_000_000_000,
                open=30_000.0, high=30_500.0,
                low=29_800.0,  close=30_200.0,
                volume=12.5,
            )
        ],
        meta = {"source": "backfill"},
    )


# ---------------------------------------------------------------------------
# serialize()
# ---------------------------------------------------------------------------

class TestSerialize:

    def test_returns_bytes(self):
        assert isinstance(serialize(_make_event()), bytes)

    def test_is_valid_json(self):
        raw = serialize(_make_event())
        parsed = json.loads(raw.decode("utf-8"))
        assert isinstance(parsed, dict)

    def test_contains_required_fields(self):
        raw    = serialize(_make_event())
        parsed = json.loads(raw.decode("utf-8"))
        required = {
            "event_id", "exchange", "symbol", "timeframe",
            "batch_start_ts", "bars", "event_version",
        }
        assert required.issubset(parsed.keys())

    def test_event_version_is_current(self):
        raw    = serialize(_make_event())
        parsed = json.loads(raw.decode("utf-8"))
        assert parsed["event_version"] == PAYLOAD_SCHEMA_VERSION

    def test_bars_is_list(self):
        raw    = serialize(_make_event())
        parsed = json.loads(raw.decode("utf-8"))
        assert isinstance(parsed["bars"], list)
        assert len(parsed["bars"]) == 1

    def test_meta_preserved(self):
        raw    = serialize(_make_event())
        parsed = json.loads(raw.decode("utf-8"))
        assert parsed["meta"] == {"source": "backfill"}

    def test_none_event_raises(self):
        with pytest.raises(ValueError, match="no puede ser None"):
            serialize(None)  # type: ignore

    def test_utf8_encoding(self):
        """Símbolos no-ASCII en meta no rompen la serialización."""
        event = EventPayload(
            event_id="evt-utf8", exchange="bybit", symbol="BTC/USDT",
            timeframe="1h", batch_start_ts=1_700_000_000_000,
            bars=[OHLCVBar(1_700_000_000_000, 1.0, 1.0, 1.0, 1.0, 1.0)],
            meta={"nota": "año fiscál"},
        )
        raw    = serialize(event)
        parsed = json.loads(raw.decode("utf-8"))
        assert parsed["meta"]["nota"] == "año fiscál"


# ---------------------------------------------------------------------------
# deserialize()
# ---------------------------------------------------------------------------

class TestDeserialize:

    def test_returns_event_payload(self):
        raw = serialize(_make_event())
        assert isinstance(deserialize(raw, EventPayload), EventPayload)

    def test_round_trip_preserves_all_fields(self):
        original  = _make_event("evt-rt")
        recovered = deserialize(serialize(original))

        assert recovered.event_id       == original.event_id
        assert recovered.exchange       == original.exchange
        assert recovered.symbol         == original.symbol
        assert recovered.timeframe      == original.timeframe
        assert recovered.batch_start_ts == original.batch_start_ts
        assert recovered.event_version  == original.event_version
        assert recovered.meta           == original.meta

    def test_round_trip_preserves_bars(self):
        original  = _make_event()
        recovered = deserialize(serialize(original))

        assert len(recovered.bars) == len(original.bars)
        bar_o = original.bars[0]
        bar_r = recovered.bars[0]
        assert bar_r.ts     == bar_o.ts
        assert bar_r.open   == bar_o.open
        assert bar_r.high   == bar_o.high
        assert bar_r.low    == bar_o.low
        assert bar_r.close  == bar_o.close
        assert bar_r.volume == bar_o.volume

    def test_invalid_json_raises(self):
        with pytest.raises((ValueError, json.JSONDecodeError)):
            deserialize(b"not-json", EventPayload)

    def test_incompatible_schema_version_raises(self):
        raw    = serialize(_make_event())
        parsed = json.loads(raw.decode("utf-8"))
        parsed["event_version"] = PAYLOAD_SCHEMA_VERSION + 99
        bad    = json.dumps(parsed).encode("utf-8")
        with pytest.raises(SchemaVersionError):
            deserialize(bad, EventPayload)

    def test_multiple_bars_round_trip(self):
        bars = [
            OHLCVBar(ts=1_700_000_000_000 + i * 3_600_000,
                     open=float(30_000 + i), high=float(30_500 + i),
                     low=float(29_800 + i), close=float(30_200 + i),
                     volume=float(10 + i))
            for i in range(5)
        ]
        event     = EventPayload(
            event_id="evt-multi", exchange="kucoin", symbol="ETH/USDT",
            timeframe="1h", batch_start_ts=1_700_000_000_000, bars=bars,
        )
        recovered = deserialize(serialize(event))
        assert len(recovered.bars) == 5
        assert recovered.bars[4].ts == bars[4].ts


# ---------------------------------------------------------------------------
# make_routing_key()
# ---------------------------------------------------------------------------

class TestMakeRoutingKey:

    def test_returns_bytes(self):
        assert isinstance(make_routing_key("bybit", "BTC/USDT", "1h"), bytes)

    def test_canonical_format(self):
        key = make_routing_key("bybit", "BTC/USDT", "1h")
        assert key == b"bybit:BTC/USDT:1h"

    def test_kucoin_futures(self):
        key = make_routing_key("kucoinfutures", "XBT/USDT:USDT", "4h")
        assert key == b"kucoinfutures:XBT/USDT:USDT:4h"

    def test_different_timeframes_produce_different_keys(self):
        k1 = make_routing_key("bybit", "BTC/USDT", "1h")
        k4 = make_routing_key("bybit", "BTC/USDT", "4h")
        assert k1 != k4

    def test_different_symbols_produce_different_keys(self):
        kb = make_routing_key("bybit", "BTC/USDT", "1h")
        ke = make_routing_key("bybit", "ETH/USDT", "1h")
        assert kb != ke

    def test_different_exchanges_produce_different_keys(self):
        k1 = make_routing_key("bybit",  "BTC/USDT", "1h")
        k2 = make_routing_key("kucoin", "BTC/USDT", "1h")
        assert k1 != k2

    def test_utf8_decodable(self):
        key = make_routing_key("bybit", "BTC/USDT", "1h")
        assert key.decode("utf-8") == "bybit:BTC/USDT:1h"


__all__ = []
