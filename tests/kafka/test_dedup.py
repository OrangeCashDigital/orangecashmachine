from __future__ import annotations

"""
tests/streaming/test_dedup.py
================================

Tests de SeenFilter, PersistentSeenFilter y CompositeSeenFilter.
Sin Redis real — PersistentSeenFilter usa un store mock en memoria.
"""

from market_data.infrastructure.kafka.dedup import (
    PersistentSeenFilter,
    SeenFilter,
)

# --------------------------------------------------
# Store mock — simula RedisCursorStore.get_raw/set_raw
# --------------------------------------------------


class _MockStore:
    def __init__(self, healthy: bool = True) -> None:
        self._data: dict = {}
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
        assert isinstance(SeenFilter(), SeenFilter)


# --------------------------------------------------
# Tests: PersistentSeenFilter (L2)
# --------------------------------------------------


class TestPersistentSeenFilter:
    def test_new_event_not_duplicate(self):
        f = PersistentSeenFilter(store=_MockStore())
        assert not f.is_duplicate("evt-001")

    def test_seen_event_is_duplicate(self):
        store = _MockStore()
        f = PersistentSeenFilter(store=store)
        f.mark_seen("evt-001")
        assert f.is_duplicate("evt-001")

    def test_persists_across_instances(self):
        """Dos instancias con el mismo store comparten estado."""
        store = _MockStore()
        f1 = PersistentSeenFilter(store=store)
        f2 = PersistentSeenFilter(store=store)

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
        f = PersistentSeenFilter(store=store)
        f.mark_seen("z")
        assert "z" in f
        assert "w" not in f

    def test_key_prefix_isolation(self):
        """Distintos event_ids usan claves distintas en Redis."""
        store = _MockStore()
        f = PersistentSeenFilter(store=store)
        f.mark_seen("evt-A")
        assert not f.is_duplicate("evt-B")


# --------------------------------------------------
# Tests: CompositeSeenFilter (L1 + L2)
