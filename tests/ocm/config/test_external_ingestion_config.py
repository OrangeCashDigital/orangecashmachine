"""Tests de config de external_ingestion (schema OCM)."""

from __future__ import annotations

import pytest

from ocm.config.schema import (
    ExternalIngestionConfig,
    ExternalSourceConfig,
    ExternalSourceRateLimit,
    ExternalSourceSchedule,
)
from shared.kafka.topics import TOPIC_EXTERNAL_RAW


class TestExternalSourceConfig:
    def test_defaults(self):
        cfg = ExternalSourceConfig()
        assert cfg.enabled is False
        assert cfg.metric == ""
        assert cfg.schedule.every == 300
        assert cfg.rate_limit.per_minute == 60
        assert cfg.topic == TOPIC_EXTERNAL_RAW  # SSOT (BC-35)

    def test_topic_overridable(self):
        assert ExternalSourceConfig(topic="custom.topic").topic == "custom.topic"

    def test_schedule_requires_positive(self):
        with pytest.raises(ValueError):
            ExternalSourceSchedule(every=0)

    def test_rate_limit_requires_positive(self):
        with pytest.raises(ValueError):
            ExternalSourceRateLimit(per_minute=0)

    def test_extra_fields_forbidden(self):
        with pytest.raises(ValueError):
            ExternalSourceConfig(nonexistent=True)

    def test_frozen(self):
        cfg = ExternalSourceConfig()
        with pytest.raises(Exception):
            cfg.enabled = True


class TestExternalIngestionConfig:
    def test_defaults_disabled(self):
        cfg = ExternalIngestionConfig()
        assert cfg.enabled is False
        assert cfg.sources == {}

    def test_with_source(self):
        cfg = ExternalIngestionConfig(
            enabled=True,
            sources={"coinglass": ExternalSourceConfig(metric="funding_rate")},
        )
        assert cfg.sources["coinglass"].metric == "funding_rate"
