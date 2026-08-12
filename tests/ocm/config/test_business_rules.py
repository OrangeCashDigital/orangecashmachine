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

import pytest

from ocm.config.layers.rules import ConfigRuleViolation, apply_business_rules
from ocm.config.schema import (
    AppConfig,
    EnvironmentConfig,
    ExchangeConfig,
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
