# -*- coding: utf-8 -*-
"""AUD-03 regression: pipeline_factory no debe acumular handlers."""

from __future__ import annotations

from market_data.domain.events.ingestion import OHLCVBatchReceived
from market_data.infrastructure.event_bus.in_memory import InMemoryEventBus


def test_subscribe_same_handler_is_idempotent():
    bus = InMemoryEventBus()

    class H:
        def handle(self, e):
            pass

    h = H()
    bus.subscribe(OHLCVBatchReceived, h.handle)
    bus.subscribe(OHLCVBatchReceived, h.handle)
    stats = bus.stats()
    assert stats.get("OHLCVBatchReceived", 0) == 1


def test_unsubscribe_removes_handler():
    bus = InMemoryEventBus()

    class H:
        def handle(self, e):
            pass

    h = H()
    bus.subscribe(OHLCVBatchReceived, h.handle)
    bus.unsubscribe(OHLCVBatchReceived, h.handle)
    assert bus.stats().get("OHLCVBatchReceived", 0) == 0
