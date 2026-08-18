"""Tests de CCXTAdapter.cancel_order (ADR-0029, B-MD-008 paso 2).

Sin infraestructura: se mockea _get_client con un fake client asíncrono.
"""

from __future__ import annotations

import asyncio
from typing import Any, Optional

import pytest
from market_data.adapters.outbound.exchange.ccxt_adapter import CCXTAdapter


class _FakeClient:
    """Cliente ccxt fake con cancel_order configurable."""

    def __init__(self, result: Optional[dict] = None, error: Optional[BaseException] = None) -> None:
        self._result = result or {"id": "e1", "status": "canceled"}
        self._error = error
        self.cancelled: list[tuple[str, str]] = []

    async def cancel_order(self, order_id: str, symbol: str, params: Any) -> dict:
        self.cancelled.append((order_id, symbol))
        if self._error is not None:
            raise self._error
        return self._result


@pytest.fixture
def adapter() -> CCXTAdapter:
    return CCXTAdapter(exchange_id="bybit")


async def _attach_fake(adapter: CCXTAdapter, fake: _FakeClient) -> None:
    """Sustituye _get_client por un fake sin conectar el adapter."""

    async def _get_client():
        return fake  # type: ignore[return-value]

    adapter._get_client = _get_client  # type: ignore[method-assign]


async def test_cancel_order_calls_exchange_and_returns_raw(
    adapter: CCXTAdapter,
) -> None:
    fake = _FakeClient()
    await _attach_fake(adapter, fake)

    raw = await adapter.cancel_order("e1", symbol="BTC/USDT")

    assert fake.cancelled == [("e1", "BTC/USDT")]
    assert raw["status"] == "canceled"


async def test_cancel_order_requires_symbol(adapter: CCXTAdapter) -> None:
    fake = _FakeClient()
    await _attach_fake(adapter, fake)

    await adapter.cancel_order("e1", symbol="ETH/USDT")

    assert fake.cancelled == [("e1", "ETH/USDT")]


async def test_cancel_order_propagates_params(adapter: CCXTAdapter) -> None:
    fake = _FakeClient()
    await _attach_fake(adapter, fake)

    await adapter.cancel_order("e1", symbol="BTC/USDT", params={"stopLoss": True})

    assert fake.cancelled[0][1] == "BTC/USDT"


async def test_cancel_order_fail_closed_on_exchange_error(
    adapter: CCXTAdapter,
) -> None:
    """Errores del exchange se propagan (no se silencian).

    El fail-closed SafeOps está en el transporte (`_BybitTransport.cancel`
    devuelve OrderState.ERROR), no en el adapter — consistente con
    create_order/fetch_order.
    """

    class _TooLate(Exception):
        pass

    fake = _FakeClient(error=_TooLate("too late to cancel"))
    await _attach_fake(adapter, fake)

    with pytest.raises(_TooLate):
        await asyncio.wait_for(
            adapter.cancel_order("e1", symbol="BTC/USDT"),
            timeout=5,
        )
