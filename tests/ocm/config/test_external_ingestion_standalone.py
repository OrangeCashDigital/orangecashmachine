# -*- coding: utf-8 -*-
"""
tests/ocm/config/test_external_ingestion_standalone.py
=========================================================

B-09 (H-05): el standalone loader (`load_appconfig_standalone`) debe aplicar
`config/market_data/external_ingestion.yaml` — no dejar la configuración YAML
silenciosamente inerte.

Contexto
--------
Antes de B-09, `market_data/external_ingestion` faltaba de `_MODULE_GLOBS`:
el path standalone (live/paper/streaming) producía `external_ingestion` con
defaults Pydantic (`enabled=False, sources={}`) mientras el path Hydra
(`uv run ocm`) cargaba el YAML real. Este test demuestra que tras la paridad
el standalone aplica los valores del YAML.
"""

from __future__ import annotations

import os

import pytest


def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Aísla el test de overrides L2 del entorno de la máquina."""
    for key in list(os.environ.keys()):
        if key.startswith("OCM_") or key.startswith(
            ("REDIS_", "KAFKA_", "BYBIT_", "KUCOIN_", "COINGLASS_", "COINMARKETCAP_")
        ):
            monkeypatch.delenv(key, raising=False)


@pytest.fixture(autouse=True)
def _isolated_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _clean_env(monkeypatch)


def _standalone_config():
    from ocm.config.hydra_loader import load_appconfig_standalone

    return load_appconfig_standalone(env="development", write_snapshot=False)


def test_standalone_loads_external_ingestion_sources_from_yaml() -> None:
    """external_ingestion.sources refleja el YAML real, no defaults vacíos.

    Antes de B-09 este test fallaba: `sources` era `{}` porque el módulo no
    estaba en `_MODULE_GLOBS` y el YAML quedaba inerte en el standalone path.
    """
    cfg = _standalone_config()

    sources = cfg.external_ingestion.sources
    assert "coinglass" in sources, "coinglass debe cargarse desde external_ingestion.yaml"
    assert "coinmarketcap" in sources, "coinmarketcap debe cargarse desde external_ingestion.yaml"

    coinglass = sources["coinglass"]
    assert coinglass.metric == "funding_rate"
    assert coinglass.schedule.every == 300
    assert coinglass.rate_limit.per_minute == 60
    assert "BTC/USDT" in coinglass.symbols


def test_standalone_loads_portfolio_from_yaml() -> None:
    """config.portfolio refleja portfolio/portfolio.yaml (paridad B-09)."""
    cfg = _standalone_config()

    assert cfg.portfolio.capital_usd == pytest.approx(10_000.0)
    assert cfg.portfolio.exchange == "bybit"
    assert cfg.portfolio.position_ttl_days == 7
