# -*- coding: utf-8 -*-
"""
tests/kafka/test_schemas_signals.py
====================================

Tests de wire schemas de señales — SignalPayload, ApprovedSignalPayload,
RejectedSignalPayload.

Sin dependencias externas. Verifica: round-trip, fail-fast por versión
incompatible, invariantes de dirección y confianza, propiedades derivadas
e immutabilidad (frozen).

Regresión Fase 1: los aliases de error y versión son el SSOT canónico.

PROVENANCE (Contract Provenance — Protocol Discovery Framework (ADR-0017, F2.5))
------------------------------------------------
- SignalPayload / ApprovedSignalPayload / RejectedSignalPayload → DOMAIN:
  eventos del dominio estrategia/risk de OCM; NO existen en ningún wire de
  exchange. Estables por diseño; no requieren PROTOCOL/DOCUMENTATION.
"""

from __future__ import annotations

import pytest

from shared.kafka.schemas.signals import (
    APPROVED_SCHEMA_VERSION,
    REJECTED_SCHEMA_VERSION,
    SIGNAL_SCHEMA_VERSION,
    ApprovedSignalPayload,
    RejectedSignalPayload,
    SignalPayload,
    SignalSchemaVersionError,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _signal(event_id: str = "sig-001", direction: str = "buy", confidence: float = 0.8) -> SignalPayload:
    return SignalPayload(
        event_id=event_id,
        occurred_at="2026-01-01T00:00:00+00:00",
        exchange="bybit",
        symbol="BTC/USDT",
        timeframe="1h",
        direction=direction,
        price=30_000.0,
        confidence=confidence,
        strategy="ema_crossover",
        run_id="run-001",
        meta={"extra": 1},
    )


def _approved(event_id: str = "sig-app-001") -> ApprovedSignalPayload:
    return ApprovedSignalPayload(
        event_id=event_id,
        occurred_at="2026-01-01T00:00:00+00:00",
        exchange="bybit",
        symbol="BTC/USDT",
        timeframe="1h",
        direction="buy",
        price=30_000.0,
        confidence=0.9,
        strategy="ema_crossover",
        approved_size_pct=0.1,
        risk_score=0.2,
        original_event_id="sig-001",
        run_id="run-001",
        meta=None,
    )


def _rejected(event_id: str = "sig-rej-001") -> RejectedSignalPayload:
    return RejectedSignalPayload(
        event_id=event_id,
        occurred_at="2026-01-01T00:00:00+00:00",
        exchange="bybit",
        symbol="BTC/USDT",
        direction="sell",
        reason="confidence_below_threshold",
        original_event_id="sig-001",
        run_id="run-001",
    )


# ---------------------------------------------------------------------------
# SignalPayload
# ---------------------------------------------------------------------------


class TestSignalPayload:
    def test_version_alias_matches_classvar(self):
        assert SIGNAL_SCHEMA_VERSION == SignalPayload.SCHEMA_VERSION

    def test_error_alias_is_canonical(self):
        assert SignalSchemaVersionError.__mro__[1] is ValueError

    def test_immutable(self):
        signal = _signal()
        with pytest.raises((AttributeError, TypeError)):
            signal.symbol = "ETH/USDT"  # type: ignore

    def test_to_dict_contains_wire_fields(self):
        d = _signal().to_dict()
        assert set(d.keys()) == {
            "event_id",
            "event_version",
            "occurred_at",
            "exchange",
            "symbol",
            "timeframe",
            "direction",
            "price",
            "confidence",
            "strategy",
            "run_id",
            "meta",
        }

    def test_round_trip(self):
        original = _signal("sig-rt", confidence=0.9)
        recovered = SignalPayload.from_dict(original.to_dict())
        assert recovered == original

    def test_missing_version_defaults_to_v1(self):
        d = _signal().to_dict()
        del d["event_version"]
        assert SignalPayload.from_dict(d).SCHEMA_VERSION == 1

    def test_incompatible_version_raises(self):
        d = _signal().to_dict()
        d["event_version"] = SignalPayload.SCHEMA_VERSION + 99
        with pytest.raises(SignalSchemaVersionError):
            SignalPayload.from_dict(d)

    def test_version_error_message_informative(self):
        d = _signal().to_dict()
        d["event_version"] = 999
        with pytest.raises(SignalSchemaVersionError, match="999"):
            SignalPayload.from_dict(d)

    def test_invalid_direction_raises(self):
        d = _signal().to_dict()
        d["direction"] = "weird"
        with pytest.raises(ValueError, match="direction"):
            SignalPayload.from_dict(d)

    def test_confidence_out_of_range_raises(self):
        d = _signal().to_dict()
        d["confidence"] = 1.5
        with pytest.raises(ValueError, match="confidence"):
            SignalPayload.from_dict(d)

    def test_hold_direction_valid(self):
        d = _signal(direction="hold").to_dict()
        assert SignalPayload.from_dict(d).direction == "hold"

    def test_is_actionable(self):
        assert _signal(direction="buy").is_actionable
        assert _signal(direction="sell").is_actionable
        assert not _signal(direction="hold").is_actionable


# ---------------------------------------------------------------------------
# ApprovedSignalPayload
# ---------------------------------------------------------------------------


class TestApprovedSignalPayload:
    def test_version_alias_matches_classvar(self):
        assert APPROVED_SCHEMA_VERSION == ApprovedSignalPayload.SCHEMA_VERSION

    def test_round_trip(self):
        original = _approved("sig-app-rt")
        recovered = ApprovedSignalPayload.from_dict(original.to_dict())
        assert recovered == original

    def test_round_trip_preserves_risk_fields(self):
        original = _approved()
        recovered = ApprovedSignalPayload.from_dict(original.to_dict())
        assert recovered.approved_size_pct == 0.1
        assert recovered.risk_score == 0.2
        assert recovered.original_event_id == "sig-001"

    def test_incompatible_version_raises(self):
        d = _approved().to_dict()
        d["event_version"] = ApprovedSignalPayload.SCHEMA_VERSION + 99
        with pytest.raises(SignalSchemaVersionError):
            ApprovedSignalPayload.from_dict(d)

    def test_invalid_direction_raises(self):
        d = _approved().to_dict()
        d["direction"] = "sideways"
        with pytest.raises(ValueError, match="direction"):
            ApprovedSignalPayload.from_dict(d)

    def test_confidence_out_of_range_raises(self):
        d = _approved().to_dict()
        d["confidence"] = -0.1
        with pytest.raises(ValueError, match="confidence"):
            ApprovedSignalPayload.from_dict(d)


# ---------------------------------------------------------------------------
# RejectedSignalPayload
# ---------------------------------------------------------------------------


class TestRejectedSignalPayload:
    def test_version_alias_matches_classvar(self):
        assert REJECTED_SCHEMA_VERSION == RejectedSignalPayload.SCHEMA_VERSION

    def test_round_trip(self):
        original = _rejected("sig-rej-rt")
        recovered = RejectedSignalPayload.from_dict(original.to_dict())
        assert recovered == original

    def test_round_trip_preserves_reason(self):
        original = _rejected()
        recovered = RejectedSignalPayload.from_dict(original.to_dict())
        assert recovered.reason == "confidence_below_threshold"
        assert recovered.original_event_id == "sig-001"

    def test_incompatible_version_raises(self):
        d = _rejected().to_dict()
        d["event_version"] = RejectedSignalPayload.SCHEMA_VERSION + 99
        with pytest.raises(SignalSchemaVersionError):
            RejectedSignalPayload.from_dict(d)

    def test_invalid_direction_raises(self):
        d = _rejected().to_dict()
        d["direction"] = "flatten"
        with pytest.raises(ValueError, match="direction"):
            RejectedSignalPayload.from_dict(d)


__all__ = []
