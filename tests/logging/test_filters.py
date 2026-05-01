from __future__ import annotations

"""
tests/logging/test_filters.py
==============================

Unit tests para pipeline_filter y strict_pipeline_filter.

Cobertura:
  • Todos los prefijos reales de _PIPELINE_MODULES (1 representante cada uno)
  • Módulos limítrofes que casi coinciden — previenen falsos positivos
  • Casos de strict_pipeline_filter con exchange/dataset completo, parcial y vacío
  • Invariantes de diseño documentados como tests explícitos
"""

import pytest
from ocm_platform.observability.filters import pipeline_filter, strict_pipeline_filter


# ── Helpers ───────────────────────────────────────────────────────────────────

def _record(name: str, extra: dict | None = None) -> dict:
    """Construye un record mínimo de Loguru para tests."""
    return {"name": name, "extra": extra or {}}


# ── pipeline_filter — aceptados ───────────────────────────────────────────────

@pytest.mark.parametrize("module", [
    # Un representante real por cada prefijo de _PIPELINE_MODULES
    "market_data.ingestion.rest.ohlcv_fetcher",
    "market_data.processing.pipelines.ohlcv_pipeline",
    "market_data.storage.silver.trades_storage",
    "market_data.quality.validators.ohlcv_validator",
    "market_data.adapters.exchange.ccxt_adapter",
    "ocm_platform.control_plane.orchestration.entrypoint",
])
def test_pipeline_filter_accepts_pipeline_modules(module):
    """pipeline_filter acepta exactamente los módulos del pipeline OCM."""
    assert pipeline_filter(_record(module)) is True


# ── pipeline_filter — rechazados ─────────────────────────────────────────────

@pytest.mark.parametrize("module", [
    # Módulos legítimos fuera del pipeline
    "ocm_platform.config.loader",
    "ocm_platform.observability.logger",
    "infra.observability.runtime",
    "main",
    # Módulos eliminados del pipeline (prefijos pre-refactor)
    "services.exchange.bybit",
    "services.state.manager",
    "pipeline.runner",
    "core.pipeline.orchestrator",
    # Falsos positivos por substring — NO deben pasar
    "market_data_extra.ingestion.fetcher",   # guión bajo extra
    "xmarket_data.ingestion.fetcher",        # prefijo diferente
    "notmarket_data.processing.foo",         # cadena que contiene el prefijo
])
def test_pipeline_filter_rejects_non_pipeline_modules(module):
    """pipeline_filter rechaza módulos fuera del pipeline y falsos positivos."""
    assert pipeline_filter(_record(module)) is False


# ── pipeline_filter — invariantes de diseño ───────────────────────────────────

def test_pipeline_filter_does_not_require_extra_context():
    """
    DISEÑO: pipeline_filter NO requiere exchange ni dataset.
    Silenciar logs sin contexto completo es un bug — startup, warnings del
    scheduler y errores de inicialización legítimamente carecen de exchange.
    """
    assert pipeline_filter(_record("market_data.ingestion.rest.ohlcv_fetcher")) is True


# ── strict_pipeline_filter ────────────────────────────────────────────────────

_PIPELINE_MODULE = "market_data.processing.pipelines.ohlcv_pipeline"
_NON_PIPELINE    = "ocm_platform.config.loader"


def test_strict_filter_accepts_full_context():
    """strict_pipeline_filter acepta módulo de pipeline con exchange y dataset."""
    record = _record(_PIPELINE_MODULE, extra={"exchange": "bybit", "dataset": "ohlcv"})
    assert strict_pipeline_filter(record) is True


def test_strict_filter_rejects_missing_exchange():
    record = _record(_PIPELINE_MODULE, extra={"dataset": "ohlcv"})
    assert strict_pipeline_filter(record) is False


def test_strict_filter_rejects_missing_dataset():
    record = _record(_PIPELINE_MODULE, extra={"exchange": "bybit"})
    assert strict_pipeline_filter(record) is False


def test_strict_filter_rejects_empty_exchange():
    record = _record(_PIPELINE_MODULE, extra={"exchange": "", "dataset": "ohlcv"})
    assert strict_pipeline_filter(record) is False


def test_strict_filter_rejects_empty_dataset():
    record = _record(_PIPELINE_MODULE, extra={"exchange": "bybit", "dataset": ""})
    assert strict_pipeline_filter(record) is False


def test_strict_filter_rejects_both_empty():
    record = _record(_PIPELINE_MODULE, extra={"exchange": "", "dataset": ""})
    assert strict_pipeline_filter(record) is False


def test_strict_filter_rejects_non_pipeline_module_even_with_full_context():
    """
    Un módulo fuera del pipeline con contexto completo NO debe pasar.
    Evita que sinks de métricas capturen logs de config/infra.
    """
    record = _record(_NON_PIPELINE, extra={"exchange": "bybit", "dataset": "ohlcv"})
    assert strict_pipeline_filter(record) is False


def test_strict_filter_rejects_no_extra_at_all():
    record = _record(_PIPELINE_MODULE)
    assert strict_pipeline_filter(record) is False
