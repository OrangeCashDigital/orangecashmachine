"""Tests de contrato para FreshnessPort + FreshnessSnapshot (ADR-0059, B-MD-001).

Verifican la forma del contrato: inmutabilidad, Protocol runtime_checkable,
y que un fake que implementa la interfaz satisface el port. No prueban
implementación real en GapAwareStream (eso es adapter, otro PR).
"""

from __future__ import annotations

import pytest

from shared.contracts.boundaries import FreshnessPort, FreshnessSnapshot


def _make_snapshot(
    stream_id: str = "ws:bybit:BTC/USDT",
    stream_type: str = "ws_trades",
    last_seen_ms: int = 1_000,
    lag_ms: int | None = None,
    is_stale: bool | None = None,
    staleness_reason: str | None = None,
    threshold_ms: int = 10_000,
    read_at_ms: int = 1_500,
    source: str = "unknown",
) -> FreshnessSnapshot:
    # Calcular lag_ms derivado si no se pasa explícitamente
    if lag_ms is None:
        lag_ms = read_at_ms - last_seen_ms
    # Calcular is_stale derivado si no se pasa explícitamente
    if is_stale is None:
        is_stale = lag_ms > threshold_ms
    # Calcular staleness_reason derivado si no se pasa explícitamente
    if staleness_reason is None and is_stale:
        staleness_reason = "lag_sobre_umbral"

    return FreshnessSnapshot(
        stream_id=stream_id,
        stream_type=stream_type,
        last_seen_ms=last_seen_ms,
        lag_ms=lag_ms,
        is_stale=is_stale,
        staleness_reason=staleness_reason,
        threshold_ms=threshold_ms,
        read_at_ms=read_at_ms,
        source=source,
    )


class TestFreshnessSnapshot:
    def test_frozen_inmutable(self) -> None:
        snap = _make_snapshot()
        with pytest.raises(AttributeError):
            snap.lag_ms = 999_999  # type: ignore[misc]

    def test_is_stale_false_when_lag_below_threshold(self) -> None:
        snap = _make_snapshot(lag_ms=100, threshold_ms=1000)
        assert snap.is_stale is False
        assert snap.staleness_reason is None

    def test_is_stale_true_when_lag_above_threshold(self) -> None:
        snap = _make_snapshot(lag_ms=5_000, threshold_ms=1_000)
        assert snap.is_stale is True
        assert snap.staleness_reason is not None

    def test_is_stale_true_when_lag_equals_threshold(self) -> None:
        # umbral exacto no es stale (strictly greater)
        snap = _make_snapshot(lag_ms=1_000, threshold_ms=1_000)
        assert snap.is_stale is False

    def test_source_default(self) -> None:
        snap = _make_snapshot()
        assert snap.source == "unknown"


class _FakeFreshnessPort:
    """Fake que implementa la interfaz — valida duck typing del Protocol."""

    def __init__(self, now_ms: int = 1_500) -> None:
        self._now_ms = now_ms
        self._snapshots: dict[str, FreshnessSnapshot] = {
            "ws:bybit:BTC/USDT": _make_snapshot(
                stream_id="ws:bybit:BTC/USDT",
                last_seen_ms=1_000,
                read_at_ms=now_ms,
            ),
        }

    def get_freshness(self, stream_id: str) -> FreshnessSnapshot:
        return self._snapshots[stream_id]


class TestFreshnessPortProtocol:
    def test_fake_satisface_port(self) -> None:
        fake = _FakeFreshnessPort()
        assert isinstance(fake, FreshnessPort)

    def test_objeto_arbitrario_no_satisface_port(self) -> None:
        assert not isinstance(object(), FreshnessPort)

    def test_fake_retorna_snapshot_valido(self) -> None:
        fake = _FakeFreshnessPort(now_ms=1_500)
        snap = fake.get_freshness("ws:bybit:BTC/USDT")
        assert isinstance(snap, FreshnessSnapshot)
        assert snap.stream_id == "ws:bybit:BTC/USDT"
        assert snap.lag_ms == 500  # 1_500 - 1_000
        assert snap.is_stale is False


class TestFreshnessPortFailureModes:
    """Casos borde del contract: fail-closed y valores extremos."""

    def test_get_freshness_puede_lanzar(self) -> None:
        """El contrato permite que get_freshness lance (fail-closed)."""

        class _FailingFreshnessPort:
            def get_freshness(self, stream_id: str) -> FreshnessSnapshot:
                raise ConnectionError("market data feed unreachable")

        port = _FailingFreshnessPort()
        # El contrato NO exige que get_freshness nunca lance — fail-closed es válido
        assert isinstance(port, FreshnessPort)
        with pytest.raises(ConnectionError):
            port.get_freshness("ws:bybit:BTC/USDT")

    def test_lag_ms_extremos(self) -> None:
        """lag_ms con valores extremos (fresco y muy viejo)."""
        snap = _make_snapshot(last_seen_ms=0, read_at_ms=0)
        # Edad 0 → fresco
        assert snap.lag_ms == 0
        assert snap.is_stale is False
        # Edad muy grande → viejo
        snap_viejo = _make_snapshot(last_seen_ms=0, read_at_ms=10**12, threshold_ms=1_000)
        assert snap_viejo.lag_ms == 10**12
        assert snap_viejo.is_stale is True
