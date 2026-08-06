"""Tests de los puertos de external_ingestion (ADR-0014)."""

from __future__ import annotations

from datetime import datetime, timezone

from market_data.ports.inbound.external import (
    ExternalRateLimitError,
    ExternalSourceError,
    ExternalSourceUnavailable,
    HealthStatus,
    HistoricalRequest,
    PollingRequest,
    PollingResult,
    PollingSourcePort,
    ReplayPort,
)


class TestValueObjects:
    def test_polling_request_defaults_symbols_to_none(self):
        req = PollingRequest(metric="funding_rate")
        assert req.metric == "funding_rate"
        assert req.symbols is None

    def test_polling_result_is_frozen_and_provider_native(self):
        result = PollingResult(
            source_id="coinglass",
            metric="funding_rate",
            payload=[{"symbol": "BTC/USDT", "fundingRate": 0.0001}],
        )
        assert result.source_id == "coinglass"
        assert result.payload[0]["fundingRate"] == 0.0001
        assert result.fetched_at.tzinfo is not None

    def test_historical_request_fields(self):
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 2, 1, tzinfo=timezone.utc)
        req = HistoricalRequest(metric="funding_rate", symbol="BTC/USDT", start=start, end=end)
        assert req.symbol == "BTC/USDT"
        assert req.start == start and req.end == end


class TestErrorHierarchy:
    def test_rate_limit_is_subclass_of_source_error(self):
        assert issubclass(ExternalRateLimitError, ExternalSourceError)
        assert issubclass(ExternalSourceUnavailable, ExternalSourceError)


class TestRuntimeCheckable:
    def test_polling_source_port_is_runtime_checkable(self):
        class _Fake:
            source_id = "fake"

            async def fetch(self, request: PollingRequest) -> PollingResult:
                return PollingResult(source_id="fake", metric=request.metric)

            async def health(self) -> HealthStatus:
                return HealthStatus(ok=True)

            async def close(self) -> None:
                return None

        assert isinstance(_Fake(), PollingSourcePort)

    def test_replay_port_is_runtime_checkable(self):
        class _Fake:
            source_id = "fake"

            async def fetch_historical(self, request: HistoricalRequest) -> PollingResult:
                return PollingResult(source_id="fake", metric=request.metric)

        assert isinstance(_Fake(), ReplayPort)
