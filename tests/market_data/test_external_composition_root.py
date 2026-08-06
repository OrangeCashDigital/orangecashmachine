"""Tests de CompositionRoot.build_external_ingestion_orchestrator (Fail-Soft)."""

from __future__ import annotations

from market_data.infrastructure.bootstrap.composition_root import CompositionRoot

from ocm.config.schema import ExternalIngestionConfig, ExternalSourceConfig


class _DummyKafkaIntegration:
    bootstrap_servers = "kafka:9092"


class _DummyIntegrations:
    kafka = _DummyKafkaIntegration()


class _DummyAppConfig:
    def __init__(self, external_ingestion: ExternalIngestionConfig) -> None:
        self.integrations = _DummyIntegrations()
        self.external_ingestion = external_ingestion


class TestBuildExternalIngestion:
    def test_returns_none_when_disabled(self):
        config = _DummyAppConfig(ExternalIngestionConfig(enabled=False))
        assert CompositionRoot.build_external_ingestion_orchestrator(config) is None

    def test_returns_none_when_no_sources_enabled(self):
        config = _DummyAppConfig(
            ExternalIngestionConfig(
                enabled=True,
                sources={"coinglass": ExternalSourceConfig(enabled=False)},
            )
        )
        assert CompositionRoot.build_external_ingestion_orchestrator(config) is None

    def test_returns_none_when_source_missing_api_key(self, monkeypatch):
        monkeypatch.delenv("COINGLASS_API_KEY", raising=False)
        config = _DummyAppConfig(
            ExternalIngestionConfig(
                enabled=True,
                sources={"coinglass": ExternalSourceConfig(enabled=True, metric="funding_rate")},
            )
        )
        assert CompositionRoot.build_external_ingestion_orchestrator(config) is None

    def test_returns_orchestrator_with_api_key(self, monkeypatch):
        monkeypatch.setenv("COINGLASS_API_KEY", "secret")
        config = _DummyAppConfig(
            ExternalIngestionConfig(
                enabled=True,
                sources={"coinglass": ExternalSourceConfig(enabled=True, metric="funding_rate")},
            )
        )
        orch = CompositionRoot.build_external_ingestion_orchestrator(config)
        from market_data.application.external_ingestion.orchestrator import (
            ExternalIngestionOrchestrator,
        )

        assert isinstance(orch, ExternalIngestionOrchestrator)

    def test_returns_none_when_unknown_source(self, monkeypatch):
        monkeypatch.setenv("NOPE_API_KEY", "x")
        config = _DummyAppConfig(
            ExternalIngestionConfig(
                enabled=True,
                sources={"nope": ExternalSourceConfig(enabled=True, metric="funding_rate")},
            )
        )
        assert CompositionRoot.build_external_ingestion_orchestrator(config) is None
