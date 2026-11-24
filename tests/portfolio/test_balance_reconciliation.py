# -*- coding: utf-8 -*-
"""Criterio de aceptación #223: reconciliación gateada por freshness."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from portfolio.application.balance_reconciliation import (
    BalanceReconciliation,
    ReconciliationAlert,
)

from shared.contracts.boundaries import FreshnessSnapshot

_STREAM_ID = "ws:bybit:BTC/USDT"
_SYMBOL_TO_STREAM = {"BTC/USDT": _STREAM_ID}


def _snapshot(*, is_stale: bool = False, lag_ms: int = 100, stream_id: str = _STREAM_ID) -> FreshnessSnapshot:
    return FreshnessSnapshot(
        stream_id=stream_id,
        stream_type="ws_trades",
        last_seen_ms=1_000,
        lag_ms=lag_ms,
        is_stale=is_stale,
        staleness_reason="lag_sobre_umbral" if is_stale else None,
        threshold_ms=30_000,
        read_at_ms=1_000 + lag_ms,
        source="test",
    )


class _StubFreshnessPort:
    def __init__(
        self,
        *,
        is_stale: bool = False,
        snapshot: FreshnessSnapshot | None = None,
        error: Exception | None = None,
    ) -> None:
        self._snap = snapshot if snapshot is not None else _snapshot(is_stale=is_stale)
        self._error = error
        self.calls: list[str] = []

    def get_freshness(self, stream_id: str) -> FreshnessSnapshot:
        self.calls.append(stream_id)
        if self._error is not None:
            raise self._error
        return self._snap


class _StubBalancePort:
    def __init__(self, total: float = 0.0, error: Exception | None = None) -> None:
        self.total = total
        self.error = error
        self.calls = 0

    def get_balance(self, currency: str = "USDT") -> SimpleNamespace:
        self.calls += 1
        if self.error is not None:
            raise self.error
        return SimpleNamespace(total=self.total, currency=currency)


def _recon(fresh, bal, thr: float = 1.0) -> BalanceReconciliation:
    return BalanceReconciliation(
        freshness_port=fresh,
        balance_port=bal,
        stream_id_for_symbol=_SYMBOL_TO_STREAM,
        divergence_threshold_usd=thr,
    )


# ── Criterio de aceptación #223 ────────────────────────────────────


def test_stale_emits_alert_no_decision() -> None:
    """Con freshness stale, emite alerta y NO consulta balance (fail-closed)."""
    fresh = _StubFreshnessPort(is_stale=True)
    bal = _StubBalancePort(total=999.0)
    result = _recon(fresh, bal).reconcile("BTC/USDT", internal_balance_usd=0.0)

    assert result is not None
    assert isinstance(result, ReconciliationAlert)
    assert result.reason == "freshness_stale"
    assert result.symbol == "BTC/USDT"
    assert bal.calls == 0


def test_fresh_divergence_emits_alert() -> None:
    fresh = _StubFreshnessPort(is_stale=False)
    bal = _StubBalancePort(total=1000.0)
    result = _recon(fresh, bal).reconcile("BTC/USDT", internal_balance_usd=0.0)

    assert result is not None
    assert result.reason == "balance_divergence"
    assert bal.calls == 1


def test_fresh_no_divergence_returns_none() -> None:
    fresh = _StubFreshnessPort(is_stale=False)
    bal = _StubBalancePort(total=1000.0)
    result = _recon(fresh, bal).reconcile("BTC/USDT", internal_balance_usd=1000.0)
    assert result is None


def test_balance_provider_error_emits_alert() -> None:
    fresh = _StubFreshnessPort(is_stale=False)
    bal = _StubBalancePort(error=ConnectionError("exchange down"))
    result = _recon(fresh, bal).reconcile("BTC/USDT", internal_balance_usd=0.0)

    assert result is not None
    assert result.reason == "balance_provider_error"


def test_freshness_provider_error_emits_alert() -> None:
    fresh = _StubFreshnessPort(error=TimeoutError("ws timeout"))
    bal = _StubBalancePort(total=1000.0)
    result = _recon(fresh, bal).reconcile("BTC/USDT", internal_balance_usd=0.0)

    assert result is not None
    assert result.reason == "freshness_unavailable"
    assert bal.calls == 0


def test_unknown_symbol_emits_alert() -> None:
    fresh = _StubFreshnessPort(is_stale=False)
    bal = _StubBalancePort(total=1000.0)
    result = _recon(fresh, bal).reconcile("DOGE/USDT", internal_balance_usd=0.0)

    assert result is not None
    assert result.reason == "unknown_stream_identity"
    assert bal.calls == 0


def test_requires_freshness_and_balance_port() -> None:
    with pytest.raises(ValueError, match="freshness_port"):
        BalanceReconciliation(
            freshness_port=None,
            balance_port=_StubBalancePort(),
            stream_id_for_symbol={},
        )
    with pytest.raises(ValueError, match="balance_port"):
        BalanceReconciliation(
            freshness_port=_StubFreshnessPort(),
            balance_port=None,
            stream_id_for_symbol={},
        )
