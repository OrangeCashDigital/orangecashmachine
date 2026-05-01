"""
tests/streaming/test_context.py
================================

Tests de contrato para StreamingContext.

Sin dependencias externas — no necesita Redis, Prefect ni AppConfig.
"""

from __future__ import annotations

import pytest
from market_data.streaming.context import StreamingContext
from market_data.streaming.consumer import DispatchHandler
from market_data.streaming.payloads import (
    EventPayload, OHLCVBar,
    SchemaVersionError,
    CONTEXT_SCHEMA_VERSION,
)


# --------------------------------------------------
# Fixtures
# --------------------------------------------------

def _make_ctx(env: str = "development") -> StreamingContext:
    return StreamingContext(
        env         = env,
        run_id      = "abc123def456",
        pushgateway = "localhost:9091",
        deployment  = "market_data_ingestion/default",
        created_at  = "2026-04-08T00:00:00+00:00",
    )

def _make_event() -> EventPayload:
    return EventPayload(
        event_id        = "evt-002",
        exchange        = "bybit",
        symbol          = "ETH/USDT",
        timeframe       = "4h",
        batch_start_ts  = 1_700_000_000_000,
        bars            = [OHLCVBar(
            ts=1_700_000_000_000,
            open=2000.0, high=2100.0, low=1950.0, close=2050.0, volume=100.0,
        )],
    )


# --------------------------------------------------
# StreamingContext
# --------------------------------------------------

class TestStreamingContext:

    def test_to_dict_round_trip(self):
        ctx = _make_ctx()
        d   = ctx.to_dict()
        ctx2 = StreamingContext.from_dict(d)
        assert ctx == ctx2

    def test_to_dict_keys(self):
        d = _make_ctx().to_dict()
        assert set(d.keys()) == {"context_version", "env", "run_id", "pushgateway", "deployment", "created_at"}

    def test_to_dict_no_credentials(self):
        """El dict serializado no debe contener campos sensibles."""
        d = _make_ctx("production").to_dict()
        sensitive = {"api_key", "api_secret", "password", "secret", "app_config"}
        assert not sensitive.intersection(d.keys())

    def test_from_dict_type_coercion(self):
        """from_dict convierte a str aunque vengan como otros tipos."""
        d = {
            "env": 123,            # int → str
            "run_id": "abc",
            "pushgateway": "h:9091",
            "deployment": "dep/x",
            "created_at": "2026-01-01T00:00:00+00:00",
        }
        ctx = StreamingContext.from_dict(d)
        assert ctx.env == "123"

    def test_from_env_returns_streaming_context(self):
        ctx = StreamingContext.from_env()
        assert isinstance(ctx, StreamingContext)
        assert ctx.env in {"development", "test", "staging", "production"}
        assert len(ctx.run_id) == 12
        assert ":" in ctx.pushgateway

    def test_immutable(self):
        ctx = _make_ctx()
        with pytest.raises((AttributeError, TypeError)):
            ctx.env = "production"  # type: ignore

    def test_to_dict_includes_context_version(self):
        d = _make_ctx().to_dict()
        assert "context_version" in d
        assert d["context_version"] == CONTEXT_SCHEMA_VERSION

    def test_from_dict_round_trip_with_version(self):
        ctx  = _make_ctx()
        ctx2 = StreamingContext.from_dict(ctx.to_dict())
        assert ctx2.context_version == CONTEXT_SCHEMA_VERSION

    def test_from_dict_rejects_future_version(self):
        d = _make_ctx().to_dict()
        d["context_version"] = CONTEXT_SCHEMA_VERSION + 99
        with pytest.raises(SchemaVersionError):
            StreamingContext.from_dict(d)


# --------------------------------------------------
# DispatchHandler con StreamingContext
# --------------------------------------------------

class TestDispatchHandlerWithContext:

    def test_handle_with_context_returns_true(self):
        ctx     = _make_ctx()
        handler = DispatchHandler(context=ctx)
        assert handler.handle(_make_event()) is True

    def test_deployment_taken_from_context(self):
        ctx = StreamingContext(
            env="production", run_id="x" * 12, pushgateway="gw:9091",
            deployment="my_flow/prod", created_at="2026-01-01T00:00:00+00:00",
        )
        handler = DispatchHandler(
            run_name="ignored/default",
            context=ctx,
        )
        assert handler._run_name == "my_flow/prod"

    def test_no_context_uses_deployment_name(self):
        handler = DispatchHandler(run_name="explicit/deploy")
        assert handler._run_name == "explicit/deploy"

    def test_invalid_context_type_raises(self):
        with pytest.raises(TypeError):
            DispatchHandler(context="not-a-context")  # type: ignore

    def test_backward_compat_no_context(self):
        """Fase 1 tests: handler sin contexto sigue funcionando."""
        handler = DispatchHandler()
        assert handler.handle(_make_event()) is True

    def test_handle_never_raises_with_context(self):
        ctx     = _make_ctx()
        handler = DispatchHandler(context=ctx)
        handler._dispatch = lambda e: (_ for _ in ()).throw(RuntimeError("boom"))
        assert handler.handle(_make_event()) is False
