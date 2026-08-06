"""Tests de adapters inbound externos (Polling API) sin red real."""

from __future__ import annotations

import aiohttp
import pytest
from market_data.adapters.inbound.external.coinglass import CoinglassPollingSource
from market_data.adapters.inbound.external.coinmarketcap import (
    CoinMarketCapPollingSource,
)
from market_data.ports.inbound.external import (
    ExternalAuthenticationError,
    ExternalRateLimitError,
    ExternalRequestError,
    ExternalSourceUnavailable,
    PollingRequest,
)


class _FakeResp:
    def __init__(
        self,
        status: int = 200,
        data=None,
        exc: Exception | None = None,
        headers: dict | None = None,
    ) -> None:
        self.status = status
        self._data = data
        self._exc = exc
        self.headers = headers or {}

    async def __aenter__(self) -> "_FakeResp":
        if self._exc is not None:
            raise self._exc  # simula fallo de red/DNS/timeout en el request
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    async def json(self) -> object:
        return self._data


class _FakeSession:
    def __init__(self, resp: _FakeResp) -> None:
        self._resp = resp
        self.closed = False
        self.urls: list[str] = []

    def get(self, url: str, **kwargs: object) -> _FakeResp:
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

    async def test_429_raises_rate_limit_with_retry_after(self):
        adapter = CoinglassPollingSource(api_key="k")
        _attach(
            adapter,
            _FakeSession(_FakeResp(status=429, headers={"Retry-After": "17"})),
        )
        with pytest.raises(ExternalRateLimitError) as ei:
            await adapter.fetch(PollingRequest("funding_rate"))
        assert ei.value.retry_after_s == 17.0

    async def test_401_raises_authentication_error(self):
        adapter = CoinglassPollingSource(api_key="bad")
        _attach(adapter, _FakeSession(_FakeResp(status=401)))
        with pytest.raises(ExternalAuthenticationError):
            await adapter.fetch(PollingRequest("funding_rate"))

    async def test_400_raises_request_error(self):
        adapter = CoinglassPollingSource(api_key="k")
        _attach(adapter, _FakeSession(_FakeResp(status=400)))
        with pytest.raises(ExternalRequestError):
            await adapter.fetch(PollingRequest("funding_rate"))

    async def test_500_raises_unavailable(self):
        adapter = CoinglassPollingSource(api_key="k")
        _attach(adapter, _FakeSession(_FakeResp(status=500)))
        with pytest.raises(ExternalSourceUnavailable):
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

    async def test_403_raises_authentication_error(self):
        adapter = CoinMarketCapPollingSource(api_key="bad")
        _attach(adapter, _FakeSession(_FakeResp(status=403)))
        with pytest.raises(ExternalAuthenticationError):
            await adapter.fetch(PollingRequest("market_metrics"))

    async def test_unsupported_metric_raises_valueerror(self):
        adapter = CoinMarketCapPollingSource(api_key="k")
        with pytest.raises(ValueError):
            await adapter.fetch(PollingRequest("funding_rate"))


class TestHealth:
    async def test_health_ok(self):
        adapter = CoinglassPollingSource(api_key="k")
        _attach(adapter, _FakeSession(_FakeResp(status=200, data={"data": []})))
        status = await adapter.health()
        assert status.ok is True

    async def test_health_reports_auth_failure(self):
        adapter = CoinglassPollingSource(api_key="bad")
        _attach(adapter, _FakeSession(_FakeResp(status=401)))
        status = await adapter.health()
        assert status.ok is False
        assert "API key" in status.detail

    async def test_health_reports_unreachable(self):
        adapter = CoinglassPollingSource(api_key="k")
        _attach(adapter, _FakeSession(_FakeResp(exc=aiohttp.ClientError("boom"))))
        status = await adapter.health()
        assert status.ok is False
        assert "unreachable" in status.detail
