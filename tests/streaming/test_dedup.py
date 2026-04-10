from __future__ import annotations

"""
tests/streaming/test_dedup.py
================================

Tests de SeenFilter, PersistentSeenFilter y CompositeSeenFilter.
Sin Redis real — PersistentSeenFilter usa un store mock en memoria.
"""

from market_data.streaming.dedup import (
    SeenFilter,
    PersistentSeenFilter,
    CompositeSeenFilter,
    DedupFilter,
)


# --------------------------------------------------
# Store mock — simula RedisCursorStore.get_raw/set_raw
# --------------------------------------------------

class _MockStore:
    def __init__(self, healthy: bool = True) -> None:
        self._data:    dict = {}
        self._healthy = healthy

    def get_raw(self, key: str):
        if not self._healthy:
            raise ConnectionError("Redis down")
        return self._data.get(key)

    def set_raw(self, key: str, value: str, ttl_seconds: int) -> None:
        if not self._healthy:
            raise ConnectionError("Redis down")
        self._data[key] = value

    def is_healthy(self) -> bool:
        return self._healthy


class _FailingStore:
    """Store que siempre lanza — prueba SafeOps."""
    def get_raw(self, key: str):
        raise RuntimeError("Redis exploded")
    def set_raw(self, key: str, value: str, ttl_seconds: int) -> None:
        raise RuntimeError("Redis exploded")


# --------------------------------------------------
# Tests: SeenFilter (L1)
# --------------------------------------------------

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

    def test_implements_dedup_filter_protocol(self):
        assert isinstance(SeenFilter(), DedupFilter)


# --------------------------------------------------
# Tests: PersistentSeenFilter (L2)
# --------------------------------------------------

class TestPersistentSeenFilter:

    def test_new_event_not_duplicate(self):
        f = PersistentSeenFilter(store=_MockStore())
        assert not f.is_duplicate("evt-001")

    def test_seen_event_is_duplicate(self):
        store = _MockStore()
        f     = PersistentSeenFilter(store=store)
        f.mark_seen("evt-001")
        assert f.is_duplicate("evt-001")

    def test_persists_across_instances(self):
        """Dos instancias con el mismo store comparten estado."""
        store = _MockStore()
        f1    = PersistentSeenFilter(store=store)
        f2    = PersistentSeenFilter(store=store)

        f1.mark_seen("evt-shared")
        assert f2.is_duplicate("evt-shared")

    def test_safeops_is_duplicate_redis_down(self):
        """Redis caído → is_duplicate retorna False (fail-open)."""
        f = PersistentSeenFilter(store=_FailingStore())
        assert not f.is_duplicate("evt-x")  # no lanza, fail-open

    def test_safeops_mark_seen_redis_down(self):
        """Redis caído → mark_seen no lanza."""
        f = PersistentSeenFilter(store=_FailingStore())
        f.mark_seen("evt-x")  # no debe lanzar

    def test_contains_operator(self):
        store = _MockStore()
        f     = PersistentSeenFilter(store=store)
        f.mark_seen("z")
        assert "z" in f
        assert "w" not in f

    def test_key_prefix_isolation(self):
        """Distintos event_ids usan claves distintas en Redis."""
        store = _MockStore()
        f     = PersistentSeenFilter(store=store)
        f.mark_seen("evt-A")
        assert not f.is_duplicate("evt-B")


# --------------------------------------------------
# Tests: CompositeSeenFilter (L1 + L2)
# --------------------------------------------------

class TestCompositeSeenFilter:

    def test_new_event_not_duplicate(self):
        f = CompositeSeenFilter(store=_MockStore())
        assert not f.is_duplicate("evt-001")

    def test_seen_event_is_duplicate(self):
        f = CompositeSeenFilter(store=_MockStore())
        f.mark_seen("evt-001")
        assert f.is_duplicate("evt-001")

    def test_l1_hit_no_redis_call(self):
        """Si L1 tiene el evento, L2 no se consulta."""
        store = _MockStore()
        f     = CompositeSeenFilter(store=store)
        f.mark_seen("evt-l1")

        # Romper Redis — si L2 se consultara, fallaría
        store._healthy = False
        assert f.is_duplicate("evt-l1")  # L1 hit, no toca Redis

    def test_l2_hit_warms_l1(self):
        """Evento en L2 pero no en L1 → L1 se calienta."""
        store = _MockStore()
        f1    = CompositeSeenFilter(store=store)
        f2    = CompositeSeenFilter(store=store)

        f1.mark_seen("evt-cross")       # escribe en L1+L2 de f1
        # f2 no tiene en L1, pero sí en L2
        assert f2.is_duplicate("evt-cross")   # L2 hit
        assert f2._l1.is_duplicate("evt-cross")  # L1 calentado

    def test_degrades_to_l1_when_store_none(self):
        """Sin store, opera solo con L1."""
        f = CompositeSeenFilter(store=None)
        f.mark_seen("evt-mem")
        assert f.is_duplicate("evt-mem")
        assert not f.is_duplicate("evt-other")

    def test_l2_redis_down_fail_open(self):
        """Redis caído → fail-open, no bloquea el pipeline."""
        f = CompositeSeenFilter(store=_FailingStore())
        assert not f.is_duplicate("evt-x")  # no lanza
        f.mark_seen("evt-x")               # no lanza
        # L1 sí tiene el evento
        assert f._l1.is_duplicate("evt-x")

    def test_persists_across_worker_restarts(self):
        """Simula restart: nueva instancia con el mismo store."""
        store    = _MockStore()
        worker_1 = CompositeSeenFilter(store=store)
        worker_1.mark_seen("evt-restart")

        # Worker reinicia — nueva instancia, L1 vacío
        worker_2 = CompositeSeenFilter(store=store)
        assert not worker_2._l1.is_duplicate("evt-restart")  # L1 vacío
        assert worker_2.is_duplicate("evt-restart")           # L2 lo encuentra

    def test_cross_worker_dedup(self):
        """Dos workers distintos comparten dedup via Redis."""
        store    = _MockStore()
        worker_a = CompositeSeenFilter(store=store)
        worker_b = CompositeSeenFilter(store=store)

        worker_a.mark_seen("evt-shared")
        assert worker_b.is_duplicate("evt-shared")

    def test_len_reflects_l1(self):
        f = CompositeSeenFilter(store=_MockStore())
        f.mark_seen("a")
        f.mark_seen("b")
        assert len(f) == 2


# --------------------------------------------------
# Tests: integración StreamSource con CompositeSeenFilter
# --------------------------------------------------

class TestStreamSourceWithPersistentDedup:

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
                "ts": 1_700_000_000_000, "open": 30_000.0, "high": 30_500.0,
                "low": 29_800.0, "close": 30_200.0, "volume": 12.5,
            }]),
            "meta": "null",
        }

    def test_cross_restart_dedup(self):
        """
        Worker 1 procesa evento → Worker 2 (mismo store) lo detecta
        como duplicado aunque su L1 esté vacío.
        """
        from market_data.streaming.router  import EventRouter
        from market_data.streaming.source  import StreamSource
        from market_data.streaming.payloads import EventPayload

        handle_count = {"n": 0}

        class _CountingHandler:
            def handle(self, event: EventPayload) -> bool:
                handle_count["n"] += 1
                return True

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

        store  = _MockStore()
        fields = self._make_fields("evt-cross-restart")
        router = EventRouter(handlers=[_CountingHandler()])

        # Worker 1 procesa el evento
        consumer_1 = _MockConsumer([("w1-0", fields)])
        source_1   = StreamSource(
            consumer       = consumer_1,
            router         = router,
            dedup_store    = store,
        )
        source_1.run_once()
        assert handle_count["n"] == 1

        # Worker 2 — nuevo proceso, L1 vacío, mismo store Redis
        consumer_2 = _MockConsumer([("w2-0", fields)])
        source_2   = StreamSource(
            consumer       = consumer_2,
            router         = router,
            dedup_store    = store,
        )
        source_2.run_once()

        # Handler no debe llamarse de nuevo
        assert handle_count["n"] == 1
        # Pero el entry_id sí debe ackarse
        assert "w2-0" in consumer_2._acked
