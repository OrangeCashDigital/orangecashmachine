from __future__ import annotations

"""
tests/streaming/test_publisher.py
===================================

Tests de StreamPublisher con mocks — sin Redis real.

Verifica:
  - wire format correcto (bars y meta como JSON string)
  - round-trip: to_wire → from_dict reconstruye el EventPayload original
  - SafeOps: publish() nunca lanza, retorna None si el infra falla
  - delegación: llama exactamente una vez a publisher.publish()
"""

import json
from typing import Optional


from market_data.streaming.payloads import (
    EventPayload, OHLCVBar,
)
from market_data.streaming.publisher import StreamPublisher


# --------------------------------------------------
# Fixtures
# --------------------------------------------------

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
                open=30_000.0,
                high=30_500.0,
                low=29_800.0,
                close=30_200.0,
                volume=12.5,
            )
        ],
        meta = None,
    )


class _MockInfraPublisher:
    """Stub de RedisStreamPublisher — captura el wire dict enviado."""

    def __init__(self, return_value: Optional[str] = "1700000000000-0") -> None:
        self._return_value = return_value
        self.calls: list   = []

    def publish(self, fields: dict) -> Optional[str]:
        self.calls.append(fields)
        return self._return_value


class _FailingInfraPublisher:
    """Stub que siempre lanza excepción."""

    def publish(self, fields: dict) -> Optional[str]:
        raise RuntimeError("Redis connection lost")


# --------------------------------------------------
# Tests
# --------------------------------------------------

class TestStreamPublisher:

    def test_publish_returns_entry_id_on_success(self):
        event    = _make_event()
        infra    = _MockInfraPublisher("1700000000000-0")
        pub      = StreamPublisher(publisher=infra)

        entry_id = pub.publish(event)

        assert entry_id == "1700000000000-0"

    def test_publish_calls_infra_exactly_once(self):
        event = _make_event()
        infra = _MockInfraPublisher()
        pub   = StreamPublisher(publisher=infra)

        pub.publish(event)

        assert len(infra.calls) == 1

    def test_wire_format_all_strings(self):
        """Todos los valores del wire dict deben ser strings."""
        event = _make_event()
        infra = _MockInfraPublisher()
        pub   = StreamPublisher(publisher=infra)

        pub.publish(event)

        wire = infra.calls[0]
        for k, v in wire.items():
            assert isinstance(v, str), f"campo '{k}' no es string: {type(v)}"

    def test_wire_format_bars_is_json_string(self):
        """bars debe ser un JSON string parseable como lista."""
        event = _make_event()
        infra = _MockInfraPublisher()
        pub   = StreamPublisher(publisher=infra)

        pub.publish(event)

        wire     = infra.calls[0]
        bars_raw = json.loads(wire["bars"])
        assert isinstance(bars_raw, list)
        assert len(bars_raw) == 1
        assert bars_raw[0]["ts"] == 1_700_000_000_000

    def test_wire_format_meta_null_is_json_null(self):
        """meta=None debe serializarse como JSON 'null', no como 'None'."""
        event = _make_event()
        infra = _MockInfraPublisher()
        pub   = StreamPublisher(publisher=infra)

        pub.publish(event)

        wire = infra.calls[0]
        assert wire["meta"] == "null"

    def test_round_trip_wire_to_event_payload(self):
        """
        El wire format producido por StreamPublisher debe reconstruir
        exactamente el EventPayload original via from_dict().
        """
        original = _make_event("evt-roundtrip")
        infra    = _MockInfraPublisher()
        pub      = StreamPublisher(publisher=infra)

        pub.publish(original)

        wire       = infra.calls[0]
        recovered  = EventPayload.from_dict(wire)

        assert recovered.event_id       == original.event_id
        assert recovered.exchange       == original.exchange
        assert recovered.symbol         == original.symbol
        assert recovered.timeframe      == original.timeframe
        assert recovered.batch_start_ts == original.batch_start_ts
        assert recovered.event_version  == original.event_version
        assert len(recovered.bars)      == len(original.bars)
        assert recovered.bars[0].ts     == original.bars[0].ts
        assert recovered.bars[0].close  == original.bars[0].close
        assert recovered.meta           == original.meta

    def test_publish_returns_none_when_infra_returns_none(self):
        """Si el infra retorna None (fallo silencioso), publisher también retorna None."""
        event = _make_event()
        infra = _MockInfraPublisher(return_value=None)
        pub   = StreamPublisher(publisher=infra)

        result = pub.publish(event)

        assert result is None

    def test_publish_safeops_never_raises(self):
        """Si el infra lanza excepción, publish() retorna None y no propaga."""
        event = _make_event()
        infra = _FailingInfraPublisher()
        pub   = StreamPublisher(publisher=infra)

        result = pub.publish(event)  # no debe lanzar

        assert result is None

    def test_publish_multiple_events(self):
        """Publicar N eventos produce N llamadas al infra."""
        infra = _MockInfraPublisher()
        pub   = StreamPublisher(publisher=infra)

        for i in range(5):
            pub.publish(_make_event(f"evt-{i:03d}"))

        assert len(infra.calls) == 5
        # Cada llamada tiene event_id distinto
        event_ids = [json.loads(c.get("event_id", "null")) if False
                     else c["event_id"] for c in infra.calls]
        assert len(set(event_ids)) == 5
