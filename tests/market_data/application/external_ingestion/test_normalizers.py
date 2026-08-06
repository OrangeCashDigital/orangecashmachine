"""Tests de normalizadores de external_ingestion (raw → evento canónico)."""

from __future__ import annotations

import pytest
from market_data.application.external_ingestion.normalizers import (
    get_normalizer,
    normalize,
)
from market_data.application.external_ingestion.normalizers.generic import (
    coerce_timestamp_ms,
)
from market_data.domain.events.external_events import ExternalMetricEvent


class TestCoinglass:
    def test_funding_rate_rows_become_canonical_events(self):
        payload = [
            {"symbol": "BTC-USDT-PERP", "fundingRate": 0.0001, "updateTime": 1700000000000},
            {"symbol": "ETH-USDT-PERP", "fundingRate": -0.00005, "updateTime": 1700000000000},
        ]
        events = normalize("coinglass", "funding_rate", payload, fetched_at_ms=1)
        assert len(events) == 2
        e0: ExternalMetricEvent = events[0]
        assert e0.source_id == "coinglass"
        assert e0.metric == "funding_rate"
        assert e0.symbol == "BTC/USDT"  # símbolo canónico convertido
        assert e0.value == "0.0001"
        assert e0.timestamp_ms == 1700000000000

    def test_symbol_filtering_applies(self):
        payload = [
            {"symbol": "BTC-USDT-PERP", "fundingRate": 0.0001, "updateTime": 1},
            {"symbol": "ETH-USDT-PERP", "fundingRate": 0.0002, "updateTime": 1},
        ]
        events = normalize("coinglass", "funding_rate", payload, symbols=["BTC/USDT"], fetched_at_ms=1)
        assert [e.symbol for e in events] == ["BTC/USDT"]

    def test_open_interest_rows_become_events(self):
        payload = [{"symbol": "BTC-USDT-PERP", "openInterest": 1234.5, "updateTime": 2}]
        events = normalize("coinglass", "open_interest", payload, fetched_at_ms=1)
        assert events[0].value == "1234.5"

    def test_unknown_metric_raises_valueerror(self):
        with pytest.raises(ValueError):
            normalize("coinglass", "nope", [], fetched_at_ms=1)


class TestCoinmarketcap:
    def test_global_metrics_become_scalar_events(self):
        payload = [{"btc_dominance": 55.2, "total_market_cap_usd": 900000000000}]
        events = normalize(
            "coinmarketcap",
            "market_metrics",
            payload,
            fetched_at_ms=1700000000000,
        )
        metrics = {e.metric: e for e in events}
        assert metrics["btc_dominance"].value == "55.2"
        assert metrics["btc_dominance"].symbol is None  # global
        assert metrics["btc_dominance"].timestamp_ms == 1700000000000
        assert metrics["total_market_cap_usd"].value == "900000000000"

    def test_non_scalar_metrics_are_skipped(self):
        payload = [{"key": [1, 2, 3], "btc_dominance": 10.0}]
        events = normalize("coinmarketcap", "market_metrics", payload, fetched_at_ms=5)
        assert [e.metric for e in events] == ["btc_dominance"]
        assert events[0].timestamp_ms == 5

    def test_deterministic_given_same_inputs(self):
        # El timestamp se inyecta explícitamente: mismas entradas → mismos
        # datos canónicos. No depende del reloj (sin datetime.now()).
        # Se comparan los campos de dominio; event_id/occurred_at son
        # identidad auto-generada por el base de eventos (fuera de scope).
        payload = [{"btc_dominance": 55.2, "eth_dominance": 8.1}]
        a = normalize("coinmarketcap", "market_metrics", payload, fetched_at_ms=1700000000000)
        b = normalize("coinmarketcap", "market_metrics", payload, fetched_at_ms=1700000000000)

        def data(events: list[ExternalMetricEvent]) -> list[tuple]:
            return [(e.metric, e.value, e.symbol, e.timestamp_ms) for e in events]

        assert data(a) == data(b)
        assert all(e.timestamp_ms == 1700000000000 for e in a)


class TestRegistry:
    def test_get_normalizer(self):
        from market_data.application.external_ingestion.normalizers.coinglass import (
            normalize_coinglass,
        )

        assert get_normalizer("coinglass") is normalize_coinglass

    def test_unknown_source_raises(self):
        with pytest.raises(ValueError):
            get_normalizer("nope")


class TestCoerceTimestamp:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [(1700000000000, 1700000000000), (1700000000000.0, 1700000000000), ("1700000000000", 1700000000000)],
    )
    def test_coerce(self, value, expected):
        assert coerce_timestamp_ms(value) == expected

    def test_rejects_bool_and_non_numeric(self):
        with pytest.raises(TypeError):
            coerce_timestamp_ms(True)
        with pytest.raises(TypeError):
            coerce_timestamp_ms("abc")
