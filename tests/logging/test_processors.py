from __future__ import annotations

"""tests/logging/test_processors.py — Unit tests para processor chain."""

import json
import pytest
from ocm_platform.observability.processors import (
    build_processor_chain,
    process_event,
    _sanitize_secrets,
    _add_timestamp,
    _inject_service_context,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def chain():
    return build_processor_chain()


@pytest.fixture
def base_event() -> dict:
    return {"event": "test_event"}


# ── Timestamp ─────────────────────────────────────────────────────────────────

def test_add_timestamp_adds_key(base_event):
    result = _add_timestamp(None, "info", base_event)
    assert "timestamp" in result


def test_add_timestamp_does_not_overwrite(base_event):
    base_event["timestamp"] = "existing"
    result = _add_timestamp(None, "info", base_event)
    assert result["timestamp"] == "existing"


def test_timestamp_is_iso_format(base_event):
    result = _add_timestamp(None, "info", base_event)
    # ISO 8601 contiene T o espacio, y termina con +00:00 o Z
    ts = result["timestamp"]
    assert "T" in ts or " " in ts
    assert len(ts) > 10


# ── Service context ───────────────────────────────────────────────────────────

def test_inject_service_context_sets_defaults(base_event):
    result = _inject_service_context(None, "info", base_event)
    assert result["service"] == "orangecashmachine"
    assert result["version"] == "0.3.0"


def test_inject_service_context_does_not_overwrite(base_event):
    base_event["service"] = "custom"
    result = _inject_service_context(None, "info", base_event)
    assert result["service"] == "custom"


# ── Sanitize secrets ──────────────────────────────────────────────────────────

@pytest.mark.parametrize("key", [
    "api_key", "api_secret", "api_password", "password",
    "secret", "token", "passphrase", "authorization",
])
def test_sanitize_known_secret_keys(key):
    event = {"event": "test", key: "super_secret_value"}
    result = _sanitize_secrets(None, "info", event)
    assert result[key] == "**REDACTED**"


def test_sanitize_pattern_match():
    event = {"event": "test", "user_token": "abc123"}
    result = _sanitize_secrets(None, "info", event)
    assert result["user_token"] == "**REDACTED**"


def test_sanitize_does_not_redact_normal_fields():
    event = {"event": "test", "exchange": "bybit", "dataset": "ohlcv"}
    result = _sanitize_secrets(None, "info", event)
    assert result["exchange"] == "bybit"
    assert result["dataset"] == "ohlcv"


# ── Full chain ────────────────────────────────────────────────────────────────

def test_process_event_returns_json_string(chain, base_event):
    result = process_event(chain, "info", base_event)
    assert isinstance(result, str)
    parsed = json.loads(result)
    assert "event" in parsed


def test_process_event_includes_required_fields(chain):
    event = {"event": "pipeline_started", "exchange": "bybit"}
    result = process_event(chain, "info", event)
    parsed = json.loads(result)
    assert parsed["service"] == "orangecashmachine"
    assert "timestamp" in parsed
    assert "level" in parsed


def test_process_event_redacts_secrets(chain):
    event = {"event": "auth", "api_key": "secret123"}
    result = process_event(chain, "info", event)
    parsed = json.loads(result)
    assert parsed["api_key"] == "**REDACTED**"
