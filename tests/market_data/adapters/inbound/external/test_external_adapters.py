"""Tests de adapters inbound externos (Polling API) sin red real."""

from __future__ import annotations

import aiohttp
import pytest
from market_data.adapters.inbound.external.coinglass import CoinglassPollingSource
from market_data.adapters.inbound.external.coinmarketcap import (
    CoinMarketCapPollingSource,
)
from market_data.ports.inbound.external import (
    ExternalRateLimitError,
    ExternalSourceUnavailable,
    PollingRequest,
)


class _FakeResp:
    def __init__(self, status: int = 200, data=None, exc: Exception | None = None) -> None:
        self.status = status
        self._data = data
        self._exc = exc

    async def __aenter__(self) -> "_FakeResp":
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    def raise_for_status(self) -> None:
        if self._exc is not None:
            raise self._exc
        if self.status >= 400:
            raise aiohttp.ClientResponseError(request_info=None, history=(), status=self.status, message="boom")

    async def json(self) -> object:
        return self._data


class _FakeSession:
    def __init__(self, resp: _FakeResp) -> None:
        self._resp = resp
        self.closed = False
        self.urls: list[str] = []

    def get(self, url: str) -> _FakeResp:
        self.urls.append(url)
        return self._resp


def _attach(adapter, session: _FakeSession) -> None:
    adapter._session = session  # inyectar sesión fake (evita red)


class TestCoinglass:
    async def test_fetch_funding_fills_polling_result(self):
        adapter = CoinglassPollingSource(api_key="k")
        _attach(adapter, _FakeSession(_FakeResp(data={"data": [{"symbol": "A", "fundingRate": 0.1}]})))
        result = await adapter.fetch(PollingRequest("funding_rate"))
        assert result.source_id == "coinglass"
        assert result.metric == "funding_rate"
        assert result.payload[0]["fundingRate"] == 0.1

    async def test_fetch_returns_empty_when_no_data(self):
        adapter = CoinglassPollingSource(api_key="k")
        _attach(adapter, _FakeSession(_FakeResp(data=None)))
        result = await adapter.fetch(PollingRequest("funding_rate"))
        assert result.payload == []

    async def test_429_raises_rate_limit(self):
        adapter = CoinglassPollingSource(api_key="k")
        _attach(adapter, _FakeSession(_FakeResp(status=429)))
        with pytest.raises(ExternalRateLimitError):
            await adapter.fetch(PollingRequest("funding_rate"))

    async def test_client_error_raises_unavailable(self):
        adapter = CoinglassPollingSource(api_key="k")
        _attach(adapter, _FakeSession(_FakeResp(exc=aiohttp.ClientError("net"))))
        with pytest.raises(ExternalSourceUnavailable):
            await adapter.fetch(PollingRequest("funding_rate"))

    async def test_unsupported_metric_raises_valueerror(self):
        adapter = CoinglassPollingSource(api_key="k")
        with pytest.raises(ValueError):
            await adapter.fetch(PollingRequest("nope"))

    async def test_close_is_safe_without_session(self):
        adapter = CoinglassPollingSource(api_key="k")
        await adapter.close()  # no debe lanzar sin sesión


class TestCoinMarketCap:
    async def test_global_metrics_payload_single_row(self):
        adapter = CoinMarketCapPollingSource(api_key="k")
        _attach(adapter, _FakeSession(_FakeResp(data={"data": {"btc_dominance": 55.5}})))
        result = await adapter.fetch(PollingRequest("market_metrics"))
        assert result.payload == [{"btc_dominance": 55.5}]

    async def test_429_raises_rate_limit(self):
        adapter = CoinMarketCapPollingSource(api_key="k")
        _attach(adapter, _FakeSession(_FakeResp(status=429)))
        with pytest.raises(ExternalRateLimitError):
            await adapter.fetch(PollingRequest("market_metrics"))

    async def test_unsupported_metric_raises_valueerror(self):
        adapter = CoinMarketCapPollingSource(api_key="k")
        with pytest.raises(ValueError):
            await adapter.fetch(PollingRequest("funding_rate"))
