# -*- coding: utf-8 -*-
"""
tests/ocm/config/test_production_config.py
==========================================

Regresión para el incidente production-readiness: ocm-streaming.service
fallaba con "At least one exchange must be enabled" porque production.yaml
nunca habilitaba ningún exchange.

Caso C+E (FASE 4): producción fue diseñada sin exchanges hasta completar
el deployment config; el deploy (B-57/B-59) activó el servicio antes de
completar la configuración canónica.

Fix (FASE 5): config/env/production.yaml habilita bybit (mecanismo canónico:
exchanges/*.yaml + env override, documentado en config/env/test.yaml).
Credenciales se resuelven por env (BYBIT_API_KEY/BYBIT_API_SECRET) — sin
ellas, fail-fast claro en producción (Caso B).
"""

from __future__ import annotations

import os

import pytest
from omegaconf import OmegaConf

from ocm.config.hydra_loader import (
    load_appconfig_from_hydra,
    load_appconfig_standalone,
)
from ocm.config.pipeline import ConfigPipelineError


def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Aísla el test de overrides OCM_* y credenciales del entorno de la máquina."""
    for key in list(os.environ.keys()):
        if key.startswith("OCM_") or key.startswith(("BYBIT_", "KUCOIN_", "REDIS_", "KAFKA_", "POSTGRES_")):
            monkeypatch.delenv(key, raising=False)


@pytest.fixture(autouse=True)
def _isolated_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _clean_env(monkeypatch)


def _minimal_production_dict(
    *,
    with_exchange: bool = True,
    with_credentials: bool = False,
    kafka_enabled: bool = True,
) -> OmegaConf.DictConfig:
    """
    DictConfig mínimo para producción que pasa L1-L5.

    Replica el patrón de test_business_rules.py:_production_dict_config
    pero parametrizable para pruebas de regresión del fix FASE 5.
    """
    exchanges_cfg = {}
    if with_exchange:
        exchanges_cfg = {
            "bybit": {
                "enabled": True,
            }
        }
    if with_credentials:
        exchanges_cfg["bybit"]["api_key"] = "test-api-key"
        exchanges_cfg["bybit"]["api_secret"] = "test-api-secret"

    return OmegaConf.create(
        {
            "exchanges": exchanges_cfg,
            "pipeline": {
                "historical": {"start_date": "auto", "timeframes": ["1m"]},
                "resample": {"targets": ["5m"], "source_tf": "1m"},
                "realtime": {},
            },
            "environment": {"name": "production"},
            "safety": {
                "dry_run": False,
                "max_backfill_days": 90,
                "require_confirmation": True,
            },
            "integrations": {"kafka": {"enabled": kafka_enabled}},
        }
    )


def _load_config(cfg: OmegaConf.DictConfig) -> object:
    """Carga via pipeline real (L1→L5) — mismo path que streaming canary."""
    return load_appconfig_from_hydra(cfg, env="production", write_snapshot=False)


class TestProductionConfigExchangeInvariant:
    """AppConfig.validate_exchanges exige >=1 exchange habilitado en producción."""

    def test_production_loads_with_bybit_enabled_real_config(self):
        """
        Producción real (production.yaml) + bybit habilitado: pasa L4 (validate_exchanges).

        Este test valida el fix FASE 5: production.yaml ahora habilita bybit
        (mecanismo canónico documentado en test.yaml:60-63).
        """
        cfg = load_appconfig_standalone(env="production", write_snapshot=False)
        assert cfg.exchanges
        assert any(e.name.value == "bybit" for e in cfg.exchanges)
        assert any(e.enabled for e in cfg.exchanges)

    def test_production_without_exchanges_fails_at_l4(self):
        """
        Producción sin exchanges: falla en L4 con mensaje claro 'At least one exchange must be enabled'.

        Simula el estado previo al fix (production.yaml sin exchanges).
        """
        cfg = _minimal_production_dict(with_exchange=False)
        with pytest.raises(ConfigPipelineError) as exc_info:
            _load_config(cfg)

        assert exc_info.value.stage.name == "VALIDATED"
        cause = exc_info.value.__cause__
        assert cause is not None
        assert "At least one exchange must be enabled" in str(cause)

    def test_production_exchange_enabled_requires_credentials(self, monkeypatch):
        """
        Producción + bybit habilitado SIN credenciales: falla en L4 con
        'Exchange bybit is enabled but credentials are missing' (Caso B).

        Credenciales se resuelven por env (BYBIT_API_KEY/BYBIT_API_SECRET).
        Sin ellas, fail-fast claro y accionable.
        """
        # validate_credentials lee OCM_ENV desde os.environ para decidir si es prod
        monkeypatch.setenv("OCM_ENV", "production")
        cfg = _minimal_production_dict(with_exchange=True, with_credentials=False)
        with pytest.raises(ConfigPipelineError) as exc_info:
            _load_config(cfg)

        assert exc_info.value.stage.name == "VALIDATED"
        cause = exc_info.value.__cause__
        assert cause is not None
        assert "credentials are missing" in str(cause).lower()

    def test_production_exchange_with_credentials_passes(self):
        """
        Producción + bybit habilitado CON credenciales: config válida (pasa L4/L5).

        Credenciales por env -> AppConfig.validate_credentials pasa.
        kafka.enabled default true en production.yaml -> L5 pasa.
        """
        cfg = _minimal_production_dict(with_exchange=True, with_credentials=True)
        app_cfg = _load_config(cfg)
        assert app_cfg.exchanges
        assert any(e.name.value == "bybit" and e.enabled for e in app_cfg.exchanges)
        assert app_cfg.integrations.kafka.enabled is True


class TestProductionConfigKafkaInvariant:
    """L5 _rule_production_requires_kafka_publisher exige kafka.enabled=true en producción."""

    def test_production_kafka_enabled_by_default(self):
        """producción.yaml kafka.enabled default true (F-031 fix canónico)."""
        cfg = load_appconfig_standalone(env="production", write_snapshot=False)
        assert cfg.integrations.kafka.enabled is True

    def test_production_kafka_can_be_overridden_to_false(self):
        """KAFKA_ENABLED=false override deshabilita -> L5 falla."""
        cfg = _minimal_production_dict(kafka_enabled=False)
        with pytest.raises(ConfigPipelineError) as exc_info:
            _load_config(cfg)
        cause = exc_info.value.__cause__
        assert cause is not None
        assert "PRODUCTION_REQUIRES_KAFKA_PUBLISHER" in str(cause)
