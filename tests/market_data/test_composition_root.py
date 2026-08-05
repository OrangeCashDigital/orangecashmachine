"""
tests/market_data/test_composition_root.py
─────────────────────────────────────────────
Tests de comportamiento para CompositionRoot.build_feed_orchestrator
y CompositionRoot.build_ws_producers.

POST-MIGRACIÓN (paso 5) — build_feed_orchestrator ya no lee YAML
directamente (yaml.safe_load, Path.exists/open eliminados). Lee
config.feeds, poblado por el Configuration Service (AppConfig).
Estos tests inyectan config.feeds directamente vía FeedsConfig real
(ocm.config.schema) en vez de mockear yaml.safe_load/Path — mismo
alcance de aserciones que la versión anterior, distinto mecanismo
de inyección.
"""

from __future__ import annotations

import pytest
from market_data.application.feed_orchestrator import FeedOrchestrator
from market_data.infrastructure.bootstrap.composition_root import CompositionRoot

from ocm.config.schema import (
    ExchangeFeedEntryConfig,
    FeedsConfig,
    FeedsKafkaConfig,
)

PUBLISHER_PATH = "market_data.adapters.outbound.kafka_trade_publisher.KafkaTradePublisher"


class _DummyKafkaIntegration:
    bootstrap_servers = "kafka:9092"


class _DummyIntegrations:
    kafka = _DummyKafkaIntegration()


class _DummyAppConfig:
    """Stub mínimo — solo expone lo que build_feed_orchestrator lee de AppConfig."""

    def __init__(self, feeds: FeedsConfig) -> None:
        self.integrations = _DummyIntegrations()
        self.feeds = feeds


def _make_config(
    *,
    ingestion_mode: str = "rest",
    topic_trades: str | None = None,
    feeds: dict[str, ExchangeFeedEntryConfig] | None = None,
) -> _DummyAppConfig:
    kafka = FeedsKafkaConfig(topic_trades=topic_trades) if topic_trades else FeedsKafkaConfig()
    return _DummyAppConfig(
        feeds=FeedsConfig(
            ingestion_mode=ingestion_mode,
            kafka=kafka,
            feeds=feeds or {},
        )
    )


class TestBuildFeedOrchestrator:
    def test_returns_none_when_ingestion_mode_is_rest(self):
        config = _make_config(ingestion_mode="rest")
        assert CompositionRoot.build_feed_orchestrator(config) is None

    def test_returns_none_when_no_feeds_enabled(self):
        config = _make_config(
            ingestion_mode="dual",
            feeds={"bybit": ExchangeFeedEntryConfig(enabled=False, symbols=["BTC-USDT-PERP"])},
        )
        assert CompositionRoot.build_feed_orchestrator(config) is None

    def test_returns_orchestrator_with_only_enabled_feeds(self):
        config = _make_config(
            ingestion_mode="dual",
            topic_trades="trades.raw",
            feeds={
                "bybit": ExchangeFeedEntryConfig(enabled=True, symbols=["BTC-USDT-PERP", "ETH-USDT-PERP"]),
                "kucoin": ExchangeFeedEntryConfig(enabled=False, symbols=["BTC-USDT"]),
            },
        )
        result = CompositionRoot.build_feed_orchestrator(config)
        assert isinstance(result, FeedOrchestrator)
        enabled_exchanges = [f.exchange for f in result._config.feeds if f.enabled]
        assert enabled_exchanges == ["bybit"]

    def test_publisher_uses_kafka_bootstrap_servers_from_appconfig(self, monkeypatch):
        config = _make_config(
            ingestion_mode="websocket",
            topic_trades="custom.topic",
            feeds={"bybit": ExchangeFeedEntryConfig(enabled=True, symbols=["BTC-USDT-PERP"])},
        )
        captured: dict = {}

        class _FakePublisher:
            def __init__(self, bootstrap_servers: str, topic: str) -> None:
                captured["bootstrap_servers"] = bootstrap_servers
                captured["topic"] = topic

        monkeypatch.setattr(PUBLISHER_PATH, _FakePublisher)
        CompositionRoot.build_feed_orchestrator(config)

        # bootstrap_servers: SSOT de infra → AppConfig.integrations.kafka
        assert captured["bootstrap_servers"] == "kafka:9092"
        # topic: SSOT de WS feeds → AppConfig.feeds.kafka.topic_trades
        assert captured["topic"] == "custom.topic"

    def test_publisher_falls_back_to_default_topic_when_missing(self, monkeypatch):
        from shared.kafka.topics import TOPIC_TRADES_RAW

        config = _make_config(
            ingestion_mode="websocket",
            feeds={"bybit": ExchangeFeedEntryConfig(enabled=True, symbols=["BTC-USDT-PERP"])},
        )
        captured: dict = {}

        class _FakePublisher:
            def __init__(self, bootstrap_servers: str, topic: str) -> None:
                captured["topic"] = topic

        monkeypatch.setattr(PUBLISHER_PATH, _FakePublisher)
        CompositionRoot.build_feed_orchestrator(config)
        assert captured["topic"] == TOPIC_TRADES_RAW


class TestBuildWsProducers:
    def test_raises_valueerror_on_empty_bootstrap_servers(self):
        with pytest.raises(ValueError):
            CompositionRoot.build_ws_producers(bootstrap_servers="")
