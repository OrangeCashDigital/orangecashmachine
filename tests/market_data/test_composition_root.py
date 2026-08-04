"""
tests/market_data/test_composition_root.py
─────────────────────────────────────────────
Tests de comportamiento para CompositionRoot.build_feed_orchestrator
y CompositionRoot.build_ws_producers.

IMPORTANTE — estos tests capturan el comportamiento ACTUAL (pre-migración
a AppConfig.feeds). Deben seguir en verde sin modificarse después de que
build_feed_orchestrator pase a leer config.feeds en vez de yaml.safe_load
directo sobre config/market_data/feeds.yaml.

NOTA — BUG CONOCIDO (pre-existente, fuera de alcance de este test file):
_repo_root = Path(__file__).resolve().parents[5] en composition_root.py
tiene un off-by-one — debería ser parents[4]. Esto hace que en producción
feeds_path apunte un nivel por encima del repo real y el archivo nunca
se encuentre (Fail-Soft silencioso). El bug desaparece en el paso 5 de
la migración (feeds pasa a leerse de AppConfig, sin resolución de Path).
Por eso aquí parcheamos Path.exists()/Path.open() explícitamente en vez
de depender del filesystem real — así el test valida la LÓGICA DE NEGOCIO
(ingestion_mode, feeds habilitados, wiring del publisher) de forma
aislada del bug de resolución de rutas, que es un problema distinto.
"""

from __future__ import annotations

import io

import pytest
from market_data.application.feed_orchestrator import FeedOrchestrator
from market_data.infrastructure.bootstrap.composition_root import CompositionRoot

MODULE = "market_data.infrastructure.bootstrap.composition_root"
PUBLISHER_PATH = "market_data.adapters.outbound.kafka_trade_publisher.KafkaTradePublisher"


class _DummyKafkaIntegration:
    bootstrap_servers = "kafka:9092"


class _DummyIntegrations:
    kafka = _DummyKafkaIntegration()


class _DummyAppConfig:
    """Stub mínimo — solo expone lo que build_feed_orchestrator lee de AppConfig."""

    integrations = _DummyIntegrations()


@pytest.fixture
def dummy_config() -> _DummyAppConfig:
    return _DummyAppConfig()


class TestBuildFeedOrchestrator:
    @pytest.fixture(autouse=True)
    def _assume_feeds_file_present(self, monkeypatch):
        """Baseline: simula que feeds.yaml existe y es legible.

        Desacopla estos tests del bug de resolución de path documentado
        arriba. test_returns_none_when_feeds_yaml_missing sobreescribe
        esto explícitamente para probar el caso contrario.
        """
        monkeypatch.setattr("pathlib.Path.exists", lambda self: True)
        monkeypatch.setattr("pathlib.Path.open", lambda self, *a, **kw: io.StringIO(""))

    def test_returns_none_when_feeds_yaml_missing(self, monkeypatch, dummy_config):
        monkeypatch.setattr("pathlib.Path.exists", lambda self: False)
        assert CompositionRoot.build_feed_orchestrator(dummy_config) is None

    def test_returns_none_when_ingestion_mode_is_rest(self, monkeypatch, dummy_config):
        monkeypatch.setattr("yaml.safe_load", lambda f: {"ingestion_mode": "rest"})
        assert CompositionRoot.build_feed_orchestrator(dummy_config) is None

    def test_returns_none_when_no_feeds_enabled(self, monkeypatch, dummy_config):
        monkeypatch.setattr(
            "yaml.safe_load",
            lambda f: {
                "ingestion_mode": "dual",
                "feeds": {"bybit": {"enabled": False, "symbols": ["BTC-USDT-PERP"]}},
            },
        )
        assert CompositionRoot.build_feed_orchestrator(dummy_config) is None

    def test_returns_orchestrator_with_only_enabled_feeds(self, monkeypatch, dummy_config):
        monkeypatch.setattr(
            "yaml.safe_load",
            lambda f: {
                "ingestion_mode": "dual",
                "kafka": {"topic_trades": "trades.raw"},
                "feeds": {
                    "bybit": {"enabled": True, "symbols": ["BTC-USDT-PERP", "ETH-USDT-PERP"]},
                    "kucoin": {"enabled": False, "symbols": ["BTC-USDT"]},
                },
            },
        )
        result = CompositionRoot.build_feed_orchestrator(dummy_config)
        assert isinstance(result, FeedOrchestrator)
        enabled_exchanges = [f.exchange for f in result._config.feeds if f.enabled]
        assert enabled_exchanges == ["bybit"]

    def test_publisher_uses_kafka_bootstrap_servers_from_appconfig(self, monkeypatch, dummy_config):
        monkeypatch.setattr(
            "yaml.safe_load",
            lambda f: {
                "ingestion_mode": "websocket",
                "kafka": {"topic_trades": "custom.topic"},
                "feeds": {"bybit": {"enabled": True, "symbols": ["BTC-USDT-PERP"]}},
            },
        )
        captured: dict = {}

        class _FakePublisher:
            def __init__(self, bootstrap_servers: str, topic: str) -> None:
                captured["bootstrap_servers"] = bootstrap_servers
                captured["topic"] = topic

        monkeypatch.setattr(PUBLISHER_PATH, _FakePublisher)
        CompositionRoot.build_feed_orchestrator(dummy_config)

        # bootstrap_servers: SSOT de infra → AppConfig.integrations.kafka (nunca feeds.yaml)
        assert captured["bootstrap_servers"] == "kafka:9092"
        # topic: SSOT de WS feeds → feeds.yaml (hasta que migre a AppConfig.feeds)
        assert captured["topic"] == "custom.topic"

    def test_publisher_falls_back_to_default_topic_when_missing(self, monkeypatch, dummy_config):
        from shared.kafka.topics import TOPIC_TRADES_RAW

        monkeypatch.setattr(
            "yaml.safe_load",
            lambda f: {
                "ingestion_mode": "websocket",
                "feeds": {"bybit": {"enabled": True, "symbols": ["BTC-USDT-PERP"]}},
            },
        )
        captured: dict = {}

        class _FakePublisher:
            def __init__(self, bootstrap_servers: str, topic: str) -> None:
                captured["topic"] = topic

        monkeypatch.setattr(PUBLISHER_PATH, _FakePublisher)
        CompositionRoot.build_feed_orchestrator(dummy_config)
        assert captured["topic"] == TOPIC_TRADES_RAW


class TestBuildWsProducers:
    def test_raises_valueerror_on_empty_bootstrap_servers(self):
        with pytest.raises(ValueError):
            CompositionRoot.build_ws_producers(bootstrap_servers="")
