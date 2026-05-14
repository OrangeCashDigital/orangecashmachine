# -*- coding: utf-8 -*-
"""
tests/kafka/test_payloads.py
==============================

Tests de kafka/payloads.py — OHLCVBar, EventPayload, versionado.

Sin dependencias externas. Verifica: immutability, round-trip,
Fail-Fast en versión incompatible, normalización de wire format.
"""
from __future__ import annotations

import json
import pytest

from market_data.infrastructure.kafka.payloads import (
    EventPayload, OHLCVBar,
    PAYLOAD_SCHEMA_VERSION, SchemaVersionError,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _bar(ts: int = 1_700_000_000_000) -> OHLCVBar:
    return OHLCVBar(
        ts=ts, open=30_000.0, high=30_500.0,
        low=29_800.0, close=30_200.0, volume=12.5,
    )

def _event(event_id: str = "evt-001") -> EventPayload:
    return EventPayload(
        event_id       = event_id,
        exchange       = "bybit",
        symbol         = "BTC/USDT",
        timeframe      = "1h",
        batch_start_ts = 1_700_000_000_000,
        bars           = [_bar()],
    )


# ---------------------------------------------------------------------------
# OHLCVBar
# ---------------------------------------------------------------------------

class TestOHLCVBar:

    def test_immutable(self):
        bar = _bar()
        with pytest.raises((AttributeError, TypeError)):
            bar.close = 99999.0  # type: ignore

    def test_to_dict_keys(self):
        d = _bar().to_dict()
        assert set(d.keys()) == {"ts", "open", "high", "low", "close", "volume"}

    def test_to_dict_types(self):
        d = _bar().to_dict()
        assert isinstance(d["ts"],     int)
        assert isinstance(d["open"],   float)
        assert isinstance(d["volume"], float)

    def test_from_dict_round_trip(self):
        bar       = _bar(ts=1_700_001_000_000)
        recovered = OHLCVBar.from_dict(bar.to_dict())
        assert recovered == bar

    def test_from_dict_coerces_string_values(self):
        """Wire format puede traer valores como strings — from_dict normaliza."""
        d = {
            "ts": "1700000000000", "open": "30000.0",
            "high": "30500.0", "low": "29800.0",
            "close": "30200.0", "volume": "12.5",
        }
        bar = OHLCVBar.from_dict(d)
        assert bar.ts     == 1_700_000_000_000
        assert bar.close  == 30_200.0
        assert bar.volume == 12.5


# ---------------------------------------------------------------------------
# EventPayload
# ---------------------------------------------------------------------------

class TestEventPayload:

    def test_immutable(self):
        event = _event()
        with pytest.raises((AttributeError, TypeError)):
            event.exchange = "kucoin"  # type: ignore

    def test_default_version(self):
        assert _event().event_version == PAYLOAD_SCHEMA_VERSION

    def test_default_meta_is_none(self):
        assert _event().meta is None

    def test_to_dict_contains_required_fields(self):
        d = _event().to_dict()
        required = {
            "event_id", "exchange", "symbol", "timeframe",
            "batch_start_ts", "bars", "event_version", "meta",
        }
        assert required == set(d.keys())

    def test_to_dict_bars_is_list_of_dicts(self):
        d = _event().to_dict()
        assert isinstance(d["bars"], list)
        assert isinstance(d["bars"][0], dict)

    def test_from_dict_round_trip(self):
        original  = _event("evt-rt")
        recovered = EventPayload.from_dict(original.to_dict())
        assert recovered == original

    def test_from_dict_missing_version_defaults_to_v1(self):
        d = _event().to_dict()
        del d["event_version"]
        e = EventPayload.from_dict(d)
        assert e.event_version == 1

    def test_from_dict_incompatible_version_raises(self):
        d = _event().to_dict()
        d["event_version"] = PAYLOAD_SCHEMA_VERSION + 99
        with pytest.raises(SchemaVersionError):
            EventPayload.from_dict(d)

    def test_from_dict_accepts_json_string_bars(self):
        """Wire format con bars como JSON string — normalización interna."""
        d = _event().to_dict()
        d["bars"] = json.dumps(d["bars"])  # simular wire Redis/Kafka legacy
        e = EventPayload.from_dict(d)
        assert len(e.bars) == 1
        assert e.bars[0].close == 30_200.0

    def test_from_dict_accepts_json_string_meta(self):
        """Wire format con meta como JSON string."""
        d = _event().to_dict()
        d["meta"] = json.dumps({"source": "ws"})
        e = EventPayload.from_dict(d)
        assert e.meta == {"source": "ws"}

    def test_from_dict_meta_null_string(self):
        """Wire format con meta='null' → None."""
        d = _event().to_dict()
        d["meta"] = "null"
        e = EventPayload.from_dict(d)
        assert e.meta is None

    def test_multiple_bars_preserved(self):
        bars = [
            OHLCVBar(ts=1_700_000_000_000 + i * 3_600_000,
                     open=float(i), high=float(i+1),
                     low=float(i-1), close=float(i),
                     volume=float(i * 10))
            for i in range(1, 6)
        ]
        event     = EventPayload(
            event_id="evt-multi", exchange="kucoin", symbol="ETH/USDT",
            timeframe="4h", batch_start_ts=1_700_000_000_000, bars=bars,
        )
        recovered = EventPayload.from_dict(event.to_dict())
        assert len(recovered.bars) == 5
        for orig, rec in zip(bars, recovered.bars):
            assert orig == rec

    def test_schema_version_error_message_informative(self):
        d = _event().to_dict()
        d["event_version"] = 999
        with pytest.raises(SchemaVersionError, match="999"):
            EventPayload.from_dict(d)


__all__ = []
