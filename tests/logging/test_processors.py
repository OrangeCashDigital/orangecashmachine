from __future__ import annotations

"""
tests/logging/test_processors.py
==================================

Unit tests para la processor chain de structlog.

Cobertura:
  • _add_timestamp — presencia, idempotencia, formato ISO 8601
  • _inject_service_context — contrato SSOT: service + version del paquete
  • _sanitize_secrets — claves conocidas, pattern match, no-redacción de campos normales
  • build_processor_chain / process_event — integración end-to-end
"""

import json
from importlib.metadata import version as _pkg_version, PackageNotFoundError

import pytest

from ocm_platform.observability.processors import (
    _SERVICE,
    _VERSION,
    _add_timestamp,
    _inject_service_context,
    _sanitize_secrets,
    build_processor_chain,
    process_event,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def chain():
    return build_processor_chain()


@pytest.fixture
def base_event() -> dict:
    return {"event": "test_event"}


# ── _add_timestamp ────────────────────────────────────────────────────────────

def test_add_timestamp_adds_key(base_event):
    result = _add_timestamp(None, "info", base_event)
    assert "timestamp" in result


def test_add_timestamp_is_idempotent(base_event):
    """setdefault: timestamp preexistente no debe ser sobreescrito."""
    base_event["timestamp"] = "existing"
    result = _add_timestamp(None, "info", base_event)
    assert result["timestamp"] == "existing"


def test_add_timestamp_iso_format(base_event):
    """Timestamp debe ser UTC ISO 8601 con separador T y offset +00:00."""
    ts = _add_timestamp(None, "info", base_event)["timestamp"]
    assert "T" in ts, f"timestamp no tiene separador T: {ts!r}"
    assert "+00:00" in ts or ts.endswith("Z"), f"timestamp no es UTC: {ts!r}"
    assert len(ts) >= 20


# ── _inject_service_context ───────────────────────────────────────────────────

def test_inject_service_context_sets_service(base_event):
    """El campo service debe ser el nombre canónico del paquete."""
    result = _inject_service_context(None, "info", base_event)
    assert result["service"] == "orangecashmachine"


def test_inject_service_context_version_matches_package(base_event):
    """
    SSOT: la versión inyectada debe coincidir con importlib.metadata.
    Si el paquete no está instalado, acepta 'unknown' (entorno de dev sin install).
    El test NUNCA hardcodea el número de versión — eso viola SSOT.
    """
    result = _inject_service_context(None, "info", base_event)
    injected = result["version"]

    try:
        expected = _pkg_version(_SERVICE)
    except PackageNotFoundError:
        expected = "unknown"

    assert injected == expected, (
        f"version en processors ({injected!r}) diverge de importlib.metadata ({expected!r}).\n"
        f"→ Verifica que _VERSION en processors.py use importlib.metadata con fallback 'unknown'."
    )


def test_inject_service_context_does_not_overwrite_service(base_event):
    """setdefault: service preexistente no debe ser sobreescrito."""
    base_event["service"] = "custom_service"
    result = _inject_service_context(None, "info", base_event)
    assert result["service"] == "custom_service"


def test_inject_service_context_does_not_overwrite_version(base_event):
    """setdefault: version preexistente no debe ser sobreescrita."""
    base_event["version"] = "pinned-version"
    result = _inject_service_context(None, "info", base_event)
    assert result["version"] == "pinned-version"


# ── _sanitize_secrets ─────────────────────────────────────────────────────────

@pytest.mark.parametrize("key", [
    "api_key", "api_secret", "api_password", "password",
    "secret", "token", "passphrase", "authorization",
])
def test_sanitize_known_secret_keys(key):
    event = {"event": "test", key: "super_secret_value"}
    result = _sanitize_secrets(None, "info", event)
    assert result[key] == "**REDACTED**"


def test_sanitize_pattern_match():
    """Claves que contienen 'token' por patrón regex deben ser redactadas."""
    event = {"event": "test", "user_token": "abc123"}
    result = _sanitize_secrets(None, "info", event)
    assert result["user_token"] == "**REDACTED**"


def test_sanitize_preserves_normal_fields():
    """Campos sin semántica de secreto no deben ser alterados."""
    event = {"event": "test", "exchange": "bybit", "dataset": "ohlcv", "latency_ms": 42}
    result = _sanitize_secrets(None, "info", event)
    assert result["exchange"] == "bybit"
    assert result["dataset"] == "ohlcv"
    assert result["latency_ms"] == 42


def test_sanitize_empty_string_value():
    """Un secreto con valor vacío igual debe ser redactado."""
    event = {"event": "test", "api_key": ""}
    result = _sanitize_secrets(None, "info", event)
    assert result["api_key"] == "**REDACTED**"


# ── build_processor_chain / process_event — integración ──────────────────────

def test_process_event_returns_valid_json(chain, base_event):
    result = process_event(chain, "info", base_event)
    assert isinstance(result, str)
    parsed = json.loads(result)
    assert "event" in parsed


def test_process_event_includes_required_fields(chain):
    """El JSON final debe tener service, version, timestamp y level."""
    event = {"event": "pipeline_started", "exchange": "bybit"}
    parsed = json.loads(process_event(chain, "info", event))
    assert parsed["service"] == "orangecashmachine"
    assert parsed["version"] == _VERSION
    assert "timestamp" in parsed
    assert "level" in parsed


def test_process_event_redacts_secrets_end_to_end(chain):
    """Secretos deben estar redactados en el JSON final."""
    event = {"event": "auth_attempt", "api_key": "secret123", "exchange": "bybit"}
    parsed = json.loads(process_event(chain, "info", event))
    assert parsed["api_key"] == "**REDACTED**"
    assert parsed["exchange"] == "bybit"  # campo normal intacto


def test_process_event_all_levels(chain):
    """La chain debe funcionar para todos los niveles estándar."""
    for level in ("debug", "info", "warning", "error", "critical"):
        result = process_event(chain, level, {"event": f"test_{level}"})
        parsed = json.loads(result)
        assert parsed["level"] == level
