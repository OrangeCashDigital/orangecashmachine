"""Tests del ciclo de vida de CCXTAdapter (connect / close).

Sin infraestructura ni red: el cliente ccxt se sustituye por un fake y
`_initialize` se reemplaza para no llamar a `load_markets()`.

Regresión cubierta: `close()` marcaba un flag `_closed = True` permanente.
Tras un `close()`, cualquier `connect()` posterior creaba un cliente que ya
no podía cerrarse (el segundo `close()` retornaba antes de tocar el cliente),
dejando una sesión aiohttp huérfana y el warning de ccxt en `__del__`.
La idempotencia debe descansar solo en `_client is None`.
"""

from __future__ import annotations

import pytest
from market_data.adapters.outbound.exchange.ccxt_adapter import CCXTAdapter


class _FakeClient:
    """Cliente ccxt fake que cuenta cuántas veces se cierra."""

    def __init__(self) -> None:
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1


class _FailingCloseClient(_FakeClient):
    """Cliente cuyo `close()` falla: el adapter debe tragarlo (SafeOps)."""

    async def close(self) -> None:
        await super().close()
        raise RuntimeError("session already broken")


@pytest.fixture
def adapter() -> CCXTAdapter:
    return CCXTAdapter(exchange_id="bybit")


async def test_close_on_never_connected_is_noop(adapter: CCXTAdapter) -> None:
    await adapter.close()

    assert adapter._client is None


async def test_close_is_idempotent(adapter: CCXTAdapter) -> None:
    client = _FakeClient()
    adapter._client = client

    await adapter.close()
    await adapter.close()

    assert client.close_calls == 1
    assert adapter._client is None


async def test_close_swallows_client_errors(adapter: CCXTAdapter) -> None:
    client = _FailingCloseClient()
    adapter._client = client

    await adapter.close()

    assert client.close_calls == 1
    assert adapter._client is None


async def test_connect_after_close_opens_a_closable_client(
    adapter: CCXTAdapter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _FakeClient()
    adapter._client = first
    await adapter.close()

    second = _FakeClient()

    async def _fake_initialize() -> None:
        adapter._client = second

    monkeypatch.setattr(adapter, "_initialize", _fake_initialize)

    await adapter.connect()
    assert adapter._client is second

    await adapter.close()
    assert second.close_calls == 1
    assert adapter._client is None
