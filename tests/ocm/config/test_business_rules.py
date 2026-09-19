# -*- coding: utf-8 -*-
"""
tests/ocm/config/test_business_rules.py
=========================================

Cobertura de la capa L5 (ocm/config/layers/rules.py): apply_business_rules()
y cada _rule_* individual. Antes de este archivo, las 5 reglas de producción
existentes (dry_run, backfill, confirmation, resample, order_range) no tenían
ningún test que instanciara AppConfig y las ejerciera — solo
test_exception_wrapping.py cubría la jerarquía de ConfigRuleViolation como
clase, sin pasar por ninguna regla real.

Guardrail #6 (Engineering Guardrails, F-031): agrega cobertura para
_rule_production_requires_kafka_publisher, la regla que adelanta a
config-load el fail-fast que antes solo ocurría en runtime en
ConcretePipelineFactory._build_ohlcv().

Patrón de construcción de AppConfig tomado de
tests/architecture/test_kafka_publisher_wiring.py::_minimal_app_config —
AppConfig real, sin mocks.
"""

from __future__ import annotations

import os

import pytest
from omegaconf import OmegaConf

from ocm.config.layers.rules import ConfigRuleViolation, apply_business_rules
from ocm.config.pipeline import ConfigPipelineError, ConfigStage
from ocm.config.schema import (
    AppConfig,
    EnvironmentConfig,
    ExchangeConfig,
    ExchangeFeedEntryConfig,
    IntegrationsConfig,
    KafkaConfig,
    MarketConfig,
    MarketsConfig,
    PipelineConfig,
    SafetyConfig,
)

# ── Helpers ───────────────────────────────────────────────────────────────────


def _minimal_app_config(
    *,
    environment_name: str = "base",
    kafka_enabled: bool = True,
    dry_run: bool = False,
    max_backfill_days: int = 30,
    require_confirmation: bool = True,
) -> AppConfig:
    """AppConfig real mínimo válido, con los campos que tocan L5 parametrizados."""
    return AppConfig(
        environment=EnvironmentConfig(name=environment_name),
        exchanges=[
            ExchangeConfig(
                name="bybit",
                enabled=True,
                markets=MarketsConfig(
                    spot=MarketConfig(enabled=True, symbols=["BTC/USDT"]),
                ),
            )
        ],
        pipeline=PipelineConfig(),
        integrations=IntegrationsConfig(kafka=KafkaConfig(enabled=kafka_enabled)),
        safety=SafetyConfig(
            dry_run=dry_run,
            max_backfill_days=max_backfill_days,
            require_confirmation=require_confirmation,
        ),
    )


# ══════════════════════════════════════════════════════════════════════════════
# _rule_production_never_dry_run
# ══════════════════════════════════════════════════════════════════════════════


class TestRuleProductionNeverDryRun:
    def test_production_with_dry_run_raises(self) -> None:
        cfg = _minimal_app_config(environment_name="production", dry_run=True)
        with pytest.raises(ConfigRuleViolation) as exc_info:
            apply_business_rules(cfg)
        assert exc_info.value.rule == "PRODUCTION_DRY_RUN"

    def test_production_without_dry_run_passes(self) -> None:
        cfg = _minimal_app_config(environment_name="production", dry_run=False)
        apply_business_rules(cfg)  # no debe lanzar

    def test_non_production_with_dry_run_passes(self) -> None:
        cfg = _minimal_app_config(environment_name="development", dry_run=True)
        apply_business_rules(cfg)  # no debe lanzar


# ══════════════════════════════════════════════════════════════════════════════
# _rule_max_backfill_production
# ══════════════════════════════════════════════════════════════════════════════


class TestRuleMaxBackfillProduction:
    def test_production_over_90_days_raises(self) -> None:
        cfg = _minimal_app_config(environment_name="production", max_backfill_days=91)
        with pytest.raises(ConfigRuleViolation) as exc_info:
            apply_business_rules(cfg)
        assert exc_info.value.rule == "PRODUCTION_BACKFILL_LIMIT"

    def test_production_at_90_days_passes(self) -> None:
        cfg = _minimal_app_config(environment_name="production", max_backfill_days=90)
        apply_business_rules(cfg)  # no debe lanzar

    def test_non_production_over_90_days_passes(self) -> None:
        cfg = _minimal_app_config(environment_name="development", max_backfill_days=365)
        apply_business_rules(cfg)  # no debe lanzar


# ══════════════════════════════════════════════════════════════════════════════
# _rule_require_confirmation_in_prod
# ══════════════════════════════════════════════════════════════════════════════


class TestRuleRequireConfirmationInProd:
    def test_production_without_confirmation_raises(self) -> None:
        cfg = _minimal_app_config(environment_name="production", require_confirmation=False)
        with pytest.raises(ConfigRuleViolation) as exc_info:
            apply_business_rules(cfg)
        assert exc_info.value.rule == "PRODUCTION_REQUIRE_CONFIRMATION"

    def test_production_with_confirmation_passes(self) -> None:
        cfg = _minimal_app_config(environment_name="production", require_confirmation=True)
        apply_business_rules(cfg)  # no debe lanzar

    def test_non_production_without_confirmation_passes(self) -> None:
        cfg = _minimal_app_config(environment_name="development", require_confirmation=False)
        apply_business_rules(cfg)  # no debe lanzar


# ══════════════════════════════════════════════════════════════════════════════
# _rule_production_requires_kafka_publisher — Guardrail #6 (F-031)
# ══════════════════════════════════════════════════════════════════════════════


class TestRuleProductionRequiresKafkaPublisher:
    """
    Cierra el hallazgo colateral de EnvironmentConfig.name: producción con
    Kafka deshabilitado debe fallar en config-load (aquí), no recién al
    construir el pipeline en ConcretePipelineFactory._build_ohlcv (F-031).
    """

    def test_production_with_kafka_disabled_raises(self) -> None:
        cfg = _minimal_app_config(environment_name="production", kafka_enabled=False)
        with pytest.raises(ConfigRuleViolation) as exc_info:
            apply_business_rules(cfg)
        assert exc_info.value.rule == "PRODUCTION_REQUIRES_KAFKA_PUBLISHER"

    def test_production_with_kafka_enabled_passes(self) -> None:
        cfg = _minimal_app_config(environment_name="production", kafka_enabled=True)
        apply_business_rules(cfg)  # no debe lanzar

    def test_non_production_with_kafka_disabled_passes(self) -> None:
        """Fuera de producción, Kafka deshabilitado es degradación válida (F-031)."""
        cfg = _minimal_app_config(environment_name="development", kafka_enabled=False)
        apply_business_rules(cfg)  # no debe lanzar

    def test_error_message_mentions_f031(self) -> None:
        cfg = _minimal_app_config(environment_name="production", kafka_enabled=False)
        with pytest.raises(ConfigRuleViolation) as exc_info:
            apply_business_rules(cfg)
        assert "F-031" in str(exc_info.value)


# ══════════════════════════════════════════════════════════════════════════════
# E2E — Guardrail #6 por el pipeline real L1→L5 (F-031)
#
# Los tests anteriores prueban la regla en aislamiento (AppConfig manual →
# apply_business_rules). Esto NO demostraba que el flujo real de config-load
# (ConfigPipeline.run → L1 compose → L2 env override → L3 coercion → L4 pydantic
# → L5 reglas) surface la violación al cargar config en producción. Estos tests
# recorren load_appconfig_from_hydra() completo con un DictConfig válido que
# pasa L1-L4 y solo difiere en integrations.kafka.enabled.
#
# Importante (F-031): kafka.enabled=True en config NO garantiza broker
# disponible — la conectividad es un fallo runtime, no de config. Aquí solo
# demostramos que la config permite el publisher; la disponibilidad del broker
# sigue siendo responsabilidad de la capa de infraestructura (pipeline_factory).
# ══════════════════════════════════════════════════════════════════════════════


def _production_dict_config(*, kafka_enabled: bool):
    """DictConfig mínimo que pasa L1-L5 con environment=production.

    Replica el patrón de test_snapshot_contract (bybit habilitado con
    credenciales + pipeline coherente) y añade safety.dry_run=False y
    require_confirmation=True — de otro modo L5 abortaría en otra regla
    de producción antes de evaluar la de Kafka.
    """
    return OmegaConf.create(
        {
            "exchanges": {
                "bybit": {
                    "enabled": True,
                    "api_key": "test-key",
                    "api_secret": "test-secret",
                    "api_password": "test-password",
                }
            },
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


class TestKafkaRuleEndToEndPipeline:
    """Guardrail #6 a través del flujo real de config-load (L1→L5)."""

    @pytest.fixture(autouse=True)
    def _clean_ocm_override_env(self, monkeypatch):
        """L2 aplica overrides OCM_* del entorno — aíslan el test de la máquina."""
        for key in list(os.environ.keys()):
            if key.startswith("OCM_"):
                monkeypatch.delenv(key, raising=False)

    def _load(self, cfg):
        from ocm.config.hydra_loader import load_appconfig_from_hydra

        return load_appconfig_from_hydra(cfg, env="production", write_snapshot=False)

    def test_production_kafka_disabled_raises_at_config_load(self) -> None:
        """Producción + kafka disabled: fallo en config-load (L5), no en runtime."""
        with pytest.raises(ConfigPipelineError) as exc_info:
            self._load(_production_dict_config(kafka_enabled=False))
        assert exc_info.value.stage == ConfigStage.FROZEN
        cause = exc_info.value.__cause__
        assert isinstance(cause, ConfigRuleViolation)
        assert cause.rule == "PRODUCTION_REQUIRES_KAFKA_PUBLISHER"
        assert "F-031" in str(cause)

    def test_production_kafka_enabled_returns_valid_config(self) -> None:
        """Producción + kafka enabled: config válida, publisher permitido."""
        app_cfg = self._load(_production_dict_config(kafka_enabled=True))
        assert app_cfg.environment.name == "production"
        assert app_cfg.integrations.kafka.enabled is True

    def test_production_kafka_missing_defaults_to_disabled_raises(self) -> None:
        """Sin bloque integrations.kafka, el default es disabled → viola en L5.

        Cubre el caso real del deploy: si KAFKA_ENABLED nunca se setea,
        kafka.enabled resuelve a False (default del schema) y producción
        aborta en config-load.
        """
        cfg = _production_dict_config(kafka_enabled=False)
        del cfg.integrations
        with pytest.raises(ConfigPipelineError) as exc_info:
            self._load(cfg)
        cause = exc_info.value.__cause__
        assert isinstance(cause, ConfigRuleViolation)
        assert cause.rule == "PRODUCTION_REQUIRES_KAFKA_PUBLISHER"

    def test_non_production_kafka_disabled_passes(self) -> None:
        """Fuera de producción, kafka disabled es degradación válida (F-031)."""
        from ocm.config.hydra_loader import load_appconfig_from_hydra

        cfg = _production_dict_config(kafka_enabled=False)
        cfg.environment.name = "development"
        cfg.safety.dry_run = True
        app_cfg = load_appconfig_from_hydra(cfg, env="development", write_snapshot=False)
        assert app_cfg.integrations.kafka.enabled is False
        assert app_cfg.environment.name == "development"


# ══════════════════════════════════════════════════════════════════════════════
# apply_business_rules — fail-fast en la primera violación
# ══════════════════════════════════════════════════════════════════════════════


class TestApplyBusinessRulesFailFast:
    def test_valid_production_config_passes_all_rules(self) -> None:
        """Una config de producción que cumple todo no lanza nada."""
        cfg = _minimal_app_config(
            environment_name="production",
            kafka_enabled=True,
            dry_run=False,
            max_backfill_days=30,
            require_confirmation=True,
        )
        apply_business_rules(cfg)  # no debe lanzar

    def test_first_violation_in_rule_order_is_raised(self) -> None:
        """Con múltiples violaciones simultáneas, se lanza la primera en orden
        de _rules (dry_run precede a kafka_publisher en apply_business_rules)."""
        cfg = _minimal_app_config(
            environment_name="production",
            dry_run=True,
            kafka_enabled=False,
        )
        with pytest.raises(ConfigRuleViolation) as exc_info:
            apply_business_rules(cfg)
        assert exc_info.value.rule == "PRODUCTION_DRY_RUN"


# ══════════════════════════════════════════════════════════════════════════════
# AppConfig.validate_exchanges — F-DPL-01/B-59: acepta feeds de market-data
# como alternativa a exchanges de trading (streaming-only, sin credenciales)
# ══════════════════════════════════════════════════════════════════════════════


def _app_config_with(
    *,
    exchanges: list[ExchangeConfig] | None = None,
    feeds: dict[str, ExchangeFeedEntryConfig] | None = None,
    environment_name: str = "development",
) -> AppConfig:
    """AppConfig real variando exchanges/feeds, sin pasar por _minimal_app_config
    (que siempre trae un exchange habilitado)."""
    from ocm.config.schema import FeedsConfig

    return AppConfig(
        environment=EnvironmentConfig(name=environment_name),
        exchanges=exchanges or [],
        feeds=FeedsConfig(feeds=feeds or {}),
        pipeline=PipelineConfig(),
        integrations=IntegrationsConfig(kafka=KafkaConfig(enabled=True)),
        safety=SafetyConfig(
            dry_run=(environment_name != "production"),
            max_backfill_days=30,
            require_confirmation=True,
        ),
    )


class TestValidateExchangesMarketDataFeeds:
    def test_solo_trading_exchange_habilitado_pasa(self) -> None:
        cfg = _app_config_with(
            exchanges=[
                ExchangeConfig(
                    name="bybit",
                    enabled=True,
                    markets=MarketsConfig(
                        spot=MarketConfig(enabled=True, symbols=["BTC/USDT"]),
                    ),
                )
            ],
            feeds={},
        )
        assert cfg.exchanges

    def test_solo_market_data_feed_habilitado_pasa(self) -> None:
        """Caso real de F-DPL-01: streaming --env production sin exchanges
        de trading, solo con un feed publico habilitado."""

        cfg = _app_config_with(
            exchanges=[],
            feeds={"bybit": ExchangeFeedEntryConfig(enabled=True, symbols=["BTC/USDT"])},
        )
        assert not cfg.exchanges
        assert cfg.feeds.feeds["bybit"].enabled

    def test_ningun_exchange_ni_feed_habilitado_falla(self) -> None:

        with pytest.raises(ValueError, match="At least one exchange must be enabled"):
            _app_config_with(
                exchanges=[],
                feeds={"bybit": ExchangeFeedEntryConfig(enabled=False, symbols=["BTC/USDT"])},
            )

    def test_exchange_deshabilitado_y_sin_feeds_falla(self) -> None:
        with pytest.raises(ValueError, match="At least one exchange must be enabled"):
            _app_config_with(exchanges=[], feeds={})

    def test_ambos_habilitados_pasa(self) -> None:

        cfg = _app_config_with(
            exchanges=[
                ExchangeConfig(
                    name="bybit",
                    enabled=True,
                    markets=MarketsConfig(
                        spot=MarketConfig(enabled=True, symbols=["BTC/USDT"]),
                    ),
                )
            ],
            feeds={"kucoin": ExchangeFeedEntryConfig(enabled=True, symbols=["ETH/USDT"])},
        )
        assert cfg.exchanges
        assert cfg.feeds.feeds["kucoin"].enabled
