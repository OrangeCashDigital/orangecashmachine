"""
tests/streaming/test_router.py
================================

Tests unitarios de EventRouter y PrefectTriggerHandler.

Sin dependencias externas — no necesita Redis, Prefect ni config.
"""

from __future__ import annotations

import pytest

from market_data.streaming.payloads import EventPayload, OHLCVBar
from market_data.streaming.consumer import PrefectTriggerHandler, EventHandler
from market_data.streaming.router import EventRouter


# --------------------------------------------------
# Fixtures
# --------------------------------------------------

def _make_event(event_id: str = "evt-001") -> EventPayload:
    return EventPayload(
        event_id=event_id,
        exchange="bybit",
        symbol="BTC/USDT",
        timeframe="1h",
        batch_start_ts=1_700_000_000_000,
        bars=[
            OHLCVBar(
                ts=1_700_000_000_000,
                open=30_000.0,
                high=30_500.0,
                low=29_800.0,
                close=30_200.0,
                volume=12.5,
            )
        ],
    )


# --------------------------------------------------
# PrefectTriggerHandler
# --------------------------------------------------

class TestPrefectTriggerHandler:
    def test_handle_returns_true_on_valid_event(self):
        handler = PrefectTriggerHandler()
        result = handler.handle(_make_event())
        assert result is True

    def test_handle_never_raises(self):
        """SafeOps: handle() nunca lanza aunque _dispatch falle."""
        handler = PrefectTriggerHandler()
        # Forzar fallo en dispatch
        handler._dispatch = lambda e: (_ for _ in ()).throw(RuntimeError("simulated"))
        result = handler.handle(_make_event())
        assert result is False

    def test_implements_event_handler_protocol(self):
        handler = PrefectTriggerHandler()
        assert isinstance(handler, EventHandler)


# --------------------------------------------------
# EventRouter
# --------------------------------------------------

class _OKHandler:
    """Handler stub que siempre acepta."""
    def handle(self, event: EventPayload) -> bool:
        return True


class _FailHandler:
    """Handler stub que siempre falla (retorna False)."""
    def handle(self, event: EventPayload) -> bool:
        return False


class _RaisingHandler:
    """Handler stub que lanza excepción."""
    def handle(self, event: EventPayload) -> bool:
        raise RuntimeError("handler error")


class TestEventRouter:
    def test_requires_at_least_one_handler(self):
        with pytest.raises(ValueError):
            EventRouter(handlers=[])

    def test_route_returns_true_when_handler_ok(self):
        router = EventRouter(handlers=[_OKHandler()])
        assert router.route(_make_event()) is True

    def test_route_returns_false_when_all_handlers_fail(self):
        router = EventRouter(handlers=[_FailHandler()])
        assert router.route(_make_event()) is False

    def test_route_continues_fanout_after_raising_handler(self):
        """Un handler que lanza no debe abortar el fan-out."""
        router = EventRouter(handlers=[_RaisingHandler(), _OKHandler()])
        assert router.route(_make_event()) is True

    def test_route_accepts_dict(self):
        router = EventRouter(handlers=[_OKHandler()])
        assert router.route(_make_event().to_dict()) is True

    def test_route_returns_false_on_invalid_dict(self):
        router = EventRouter(handlers=[_OKHandler()])
        assert router.route({"malformed": True}) is False

    def test_route_with_prefect_handler(self):
        router = EventRouter(handlers=[PrefectTriggerHandler()])
        assert router.route(_make_event()) is True

    def test_multiple_handlers_fanout(self):
        """Todos los handlers reciben el evento."""
        called = []

        class _TrackingHandler:
            def __init__(self, name):
                self._name = name
            def handle(self, event: EventPayload) -> bool:
                called.append(self._name)
                return True

        router = EventRouter(handlers=[
            _TrackingHandler("A"),
            _TrackingHandler("B"),
            _TrackingHandler("C"),
        ])
        router.route(_make_event())
        assert called == ["A", "B", "C"]
