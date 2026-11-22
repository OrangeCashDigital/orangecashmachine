"""Tests de contrato para BalancePort + BalanceSnapshot (ADR-0030, B-MD-009).

Verifican la forma del contrato: inmutabilidad, Protocol runtime_checkable,
y que un fake que implementa la interfaz satisface el port. No prueban
conexión real al exchange (eso es adapter, otro PR).
"""

from __future__ import annotations

import pytest

from shared.contracts.boundaries import BalancePort, BalanceSnapshot


def _make_snapshot(read_at_ms: int = 0, currency: str = "USDT") -> BalanceSnapshot:
    return BalanceSnapshot(
        currency=currency,
        total=10_000.0,
        available_operational=8_000.0,
        locked=2_000.0,
        unrealised_pnl=150.0,
        read_at_ms=read_at_ms,
    )


class TestBalanceSnapshot:
    def test_frozen_inmutable(self) -> None:
        snap = _make_snapshot()
        with pytest.raises(AttributeError):
            snap.total = 20_000.0  # type: ignore[misc]

    def test_age_ms(self) -> None:
        snap = _make_snapshot(read_at_ms=1_000)
        assert snap.age_ms(now_ms=1_500) == 500

    def test_age_ms_negativo_si_futuro(self) -> None:
        """Un snapshot con read_at_ms > now_ms (clock skew) da age negativo. Es informativo."""
        snap = _make_snapshot(read_at_ms=2_000)
        assert snap.age_ms(now_ms=1_500) == -500

    def test_source_default(self) -> None:
        snap = _make_snapshot()
        assert snap.source == "unknown"


class _FakeBalancePort:
    """Fake que implementa la interfaz — valida duck typing del Protocol."""

    def __init__(self, now_ms: int = 0) -> None:
        self._now_ms = now_ms
        self._snapshots: dict[str, BalanceSnapshot] = {
            "USDT": _make_snapshot(read_at_ms=now_ms, currency="USDT"),
        }

    def get_balance(self, currency: str = "USDT") -> BalanceSnapshot:
        return self._snapshots[currency]


class TestBalancePortProtocol:
    def test_fake_satisface_port(self) -> None:
        fake = _FakeBalancePort()
        assert isinstance(fake, BalancePort)

    def test_objeto_arbitrario_no_satisface_port(self) -> None:
        assert not isinstance(object(), BalancePort)

    def test_fake_retorna_snapshot_valido(self) -> None:
        fake = _FakeBalancePort(now_ms=1_000)
        snap = fake.get_balance("USDT")
        assert isinstance(snap, BalanceSnapshot)
        assert snap.currency == "USDT"
        assert snap.age_ms(now_ms=1_500) == 500


class TestBalancePortFailureModes:
    """Casos borde del contract: fail-closed y edades extremas."""

    def test_get_balance_puede_lanzar(self) -> None:
        """El contrato permite que get_balance lance (fail-closed)."""

        class _FailingBalancePort:
            def get_balance(self, currency: str = "USDT") -> BalanceSnapshot:
                raise ConnectionError("exchange unreachable")

        port = _FailingBalancePort()
        # El contrato NO exige que get_balance nunca lance — fail-closed es válido
        assert isinstance(port, BalancePort)
        with pytest.raises(ConnectionError):
            port.get_balance()

    def test_snapshot_age_ms_extremos(self) -> None:
        """age_ms con valores extremos (fresco y muy viejo)."""
        snap = _make_snapshot(read_at_ms=0)
        # Edad 0 → fresco
        assert snap.age_ms(now_ms=0) == 0
        # Edad muy grande → viejo
        assert snap.age_ms(now_ms=10**12) == 10**12
