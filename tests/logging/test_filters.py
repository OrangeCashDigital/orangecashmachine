from __future__ import annotations

"""tests/logging/test_filters.py — Unit tests para filtros de sinks."""

import pytest
from ocm_platform.observability.filters import pipeline_filter, strict_pipeline_filter


def _record(name: str, extra: dict | None = None) -> dict:
    """Construye un record mínimo de Loguru para tests."""
    return {"name": name, "extra": extra or {}}


# ── pipeline_filter ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("module", [
    "market_data.fetcher",
    "services.exchange.bybit",
    "services.state.manager",
    "pipeline.runner",
    "core.pipeline.orchestrator",
])
def test_pipeline_filter_accepts_pipeline_modules(module):
    assert pipeline_filter(_record(module)) is True


@pytest.mark.parametrize("module", [
    "core.config.loader",
    "main",
    "infra.observability.runtime",
    "core.observability.logger",
])
def test_pipeline_filter_rejects_non_pipeline_modules(module):
    assert pipeline_filter(_record(module)) is False


def test_pipeline_filter_accepts_without_extra_context():
    """pipeline_filter NO requiere exchange ni dataset — diseño intencional."""
    assert pipeline_filter(_record("market_data.fetcher")) is True


# ── strict_pipeline_filter ────────────────────────────────────────────────────

def test_strict_filter_requires_exchange_and_dataset():
    record = _record(
        "market_data.fetcher",
        extra={"exchange": "bybit", "dataset": "ohlcv"},
    )
    assert strict_pipeline_filter(record) is True


def test_strict_filter_rejects_missing_exchange():
    assert strict_pipeline_filter(_record("market_data.fetcher", extra={"dataset": "ohlcv"})) is False


def test_strict_filter_rejects_missing_dataset():
    assert strict_pipeline_filter(_record("market_data.fetcher", extra={"exchange": "bybit"})) is False


def test_strict_filter_rejects_empty_values():
    record = _record("market_data.fetcher", extra={"exchange": "", "dataset": ""})
    assert strict_pipeline_filter(record) is False


def test_strict_filter_rejects_non_pipeline_module():
    record = _record("core.config.loader", extra={"exchange": "bybit", "dataset": "ohlcv"})
    assert strict_pipeline_filter(record) is False
