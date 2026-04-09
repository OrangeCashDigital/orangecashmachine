from __future__ import annotations

"""
tests/streaming/test_dedup.py
================================

Tests de SeenFilter — deduplicación en memoria.
"""

from market_data.streaming.dedup import SeenFilter


class TestSeenFilter:

    def test_new_event_not_duplicate(self):
        f = SeenFilter()
        assert not f.is_duplicate("evt-001")

    def test_seen_event_is_duplicate(self):
        f = SeenFilter()
        f.mark_seen("evt-001")
        assert f.is_duplicate("evt-001")

    def test_different_events_not_duplicate(self):
        f = SeenFilter()
        f.mark_seen("evt-001")
        assert not f.is_duplicate("evt-002")

    def test_len_tracks_size(self):
        f = SeenFilter()
        f.mark_seen("a")
        f.mark_seen("b")
        assert len(f) == 2

    def test_duplicate_mark_does_not_grow(self):
        f = SeenFilter()
        f.mark_seen("a")
        f.mark_seen("a")
        assert len(f) == 1

    def test_evicts_oldest_when_full(self):
        f = SeenFilter(max_size=3)
        f.mark_seen("a")
        f.mark_seen("b")
        f.mark_seen("c")
        # "a" es el más antiguo — al agregar "d" se evicta
        f.mark_seen("d")
        assert len(f) == 3
        assert not f.is_duplicate("a")  # evictado
        assert f.is_duplicate("b")
        assert f.is_duplicate("c")
        assert f.is_duplicate("d")

    def test_contains_operator(self):
        f = SeenFilter()
        f.mark_seen("x")
        assert "x" in f
        assert "y" not in f

    def test_max_size_respected_under_load(self):
        f = SeenFilter(max_size=100)
        for i in range(500):
            f.mark_seen(f"evt-{i:04d}")
        assert len(f) == 100


class TestStreamSourceDedup:
    """Tests de idempotencia integrados en StreamSource."""

    def _make_source(self, messages, router):
        from market_data.streaming.source import StreamSource

        class _MockConsumer:
            def __init__(self, msgs):
                self._messages = list(msgs)
                self._acked    = []
            def consume(self):
                if not self._messages:
                    return []
                batch, self._messages = self._messages[:5], self._messages[5:]
                return batch
            def ack(self, entry_id):
                self._acked.append(entry_id)
                return True

        consumer = _MockConsumer(messages)
        source   = StreamSource(consumer=consumer, router=router)
        return source, consumer

    def _make_fields(self, event_id: str) -> dict:
        from market_data.streaming.payloads import PAYLOAD_SCHEMA_VERSION
        import json
        return {
            "event_version":  str(PAYLOAD_SCHEMA_VERSION),
            "event_id":       event_id,
            "exchange":       "bybit",
            "symbol":         "BTC/USDT",
            "timeframe":      "1h",
            "batch_start_ts": "1700000000000",
            "bars":           json.dumps([{
                "ts": 1700000000000, "open": 30000.0, "high": 30500.0,
                "low": 29800.0, "close": 30200.0, "volume": 12.5,
            }]),
            "meta": "null",
        }

    def test_duplicate_event_id_skipped(self):
        """Mismo event_id en dos mensajes distintos → procesado solo una vez."""
        from market_data.streaming.router import EventRouter
        from market_data.streaming.payloads import EventPayload

        handle_count = {"n": 0}

        class _CountingHandler:
            def handle(self, event: EventPayload) -> bool:
                handle_count["n"] += 1
                return True

        fields   = self._make_fields("evt-dup")
        router   = EventRouter(handlers=[_CountingHandler()])
        source, consumer = self._make_source(
            [("e1-0", fields), ("e2-0", fields)],  # mismo event_id
            router,
        )

        processed, failed = source.run_once()

        assert processed == 1          # solo uno procesado
        assert failed    == 0
        assert handle_count["n"] == 1  # handler llamado una vez
        # ambos entry_ids acked (el duplicado también se ackea)
        assert "e1-0" in consumer._acked
        assert "e2-0" in consumer._acked

    def test_different_event_ids_both_processed(self):
        """event_ids distintos → ambos procesados."""
        from market_data.streaming.router import EventRouter
        from market_data.streaming.payloads import EventPayload

        class _OKHandler:
            def handle(self, event: EventPayload) -> bool:
                return True

        messages = [
            ("m1-0", self._make_fields("evt-001")),
            ("m2-0", self._make_fields("evt-002")),
        ]
        router = EventRouter(handlers=[_OKHandler()])
        source, consumer = self._make_source(messages, router)

        processed, failed = source.run_once()

        assert processed == 2
        assert failed    == 0

    def test_dedup_persists_across_batches(self):
        """Deduplicación persiste entre llamadas a run_once()."""
        from market_data.streaming.router import EventRouter
        from market_data.streaming.payloads import EventPayload

        handle_count = {"n": 0}

        class _CountingHandler:
            def handle(self, event: EventPayload) -> bool:
                handle_count["n"] += 1
                return True

        fields   = self._make_fields("evt-cross-batch")
        router   = EventRouter(handlers=[_CountingHandler()])
        source, consumer = self._make_source([], router)

        # Batch 1
        consumer._messages = [("b1-0", fields)]
        source.run_once()

        # Batch 2 — mismo event_id
        consumer._messages = [("b2-0", fields)]
        source.run_once()

        assert handle_count["n"] == 1  # procesado solo en batch 1
        assert "b1-0" in consumer._acked
        assert "b2-0" in consumer._acked  # acked igual, pero no procesado
