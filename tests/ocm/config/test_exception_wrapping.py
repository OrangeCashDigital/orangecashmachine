"""
tests/ocm/config/test_exception_wrapping.py
=============================================

Contrato de jerarquía de excepciones del bounded context ocm.config.

  Exception
  └── ConfigurationError          (base — loader/exceptions.py)
      ├── ConfigFileNotFoundError
      ├── ConfigParseError
      ├── ConfigValidationError
      │   └── ConfigRuleViolation (L5 — layers/rules.py)
      └── ConfigPipelineError     (L1-L5 stage-aware — pipeline.py)

Propiedades verificadas:
  E1  Todo tipo de excepción del bounded context hereda de ConfigurationError.
  E2  ConfigRuleViolation ⊂ ConfigValidationError (L5 es error de validación).
  E3  ConfigPipelineError ⊄ ConfigValidationError (ramas distintas).
  E4  ConfigRuleViolation almacena .rule y .fix como atributos de instancia.
  E5  El mensaje sigue el formato [ConfigRule:{rule}] ... FIX: ... .
  E6  Raise/catch round-trip: ConfigRuleViolation catcheable como ConfigurationError.
  E7  El módulo _contract.py importa sin errores (freeze de interfaz pública).
"""

from __future__ import annotations

import importlib

import pytest

from ocm.config.loader.exceptions import (
    ConfigurationError,
    ConfigFileNotFoundError,
    ConfigParseError,
    ConfigValidationError,
)
from ocm.config.pipeline import ConfigPipelineError, ConfigStage
from ocm.config.layers.rules import ConfigRuleViolation

# ── E1: toda excepción del bounded context hereda de ConfigurationError ────────


@pytest.mark.parametrize(
    "exc_cls",
    [
        ConfigFileNotFoundError,
        ConfigParseError,
        ConfigValidationError,
        ConfigRuleViolation,
        ConfigPipelineError,
    ],
)
def test_all_bounded_context_exceptions_inherit_configuration_error(exc_cls):
    """Todo `except ConfigurationError` en main.py cubre el bounded context completo."""
    assert issubclass(exc_cls, ConfigurationError), (
        f"{exc_cls.__name__} debe heredar de ConfigurationError — "
        f"sin ello, un `except ConfigurationError` en startup lo dejaría pasar"
    )


# ── E2: ConfigRuleViolation ⊂ ConfigValidationError ──────────────────────────


def test_config_rule_violation_is_subclass_of_config_validation_error():
    """ConfigRuleViolation es un subtipo de ConfigValidationError.

    Semánticamente: violar una regla de negocio ES una falla de validación
    — el sistema recibió una config que no satisface sus invariantes.
    """
    assert issubclass(ConfigRuleViolation, ConfigValidationError)


# ── E3: ConfigPipelineError ⊄ ConfigValidationError ──────────────────────────


def test_config_pipeline_error_is_not_subclass_of_config_validation_error():
    """ConfigPipelineError y ConfigValidationError son ramas distintas.

    ConfigPipelineError cubre fallos de infraestructura del pipeline (L1-L5),
    no violaciones de schema o reglas de negocio.
    Un `except ConfigValidationError` NO debe capturar errores del pipeline.
    """
    assert not issubclass(ConfigPipelineError, ConfigValidationError), (
        "ConfigPipelineError no debe ser subclase de ConfigValidationError — "
        "son bounded contexts distintos: infra-pipeline vs. business-validation"
    )


# ── E4: atributos de instancia de ConfigRuleViolation ────────────────────────


def test_config_rule_violation_stores_rule_attribute():
    exc = ConfigRuleViolation(
        rule="ORDER_RANGE",
        message="max_order_usd <= min_order_usd",
        fix="Setear max_order_usd > min_order_usd",
    )
    assert exc.rule == "ORDER_RANGE"


def test_config_rule_violation_stores_fix_attribute():
    exc = ConfigRuleViolation(
        rule="PRODUCTION_DRY_RUN",
        message="dry_run=True en production",
        fix="Setear safety.dry_run=false",
    )
    assert exc.fix == "Setear safety.dry_run=false"


# ── E5: formato del mensaje ───────────────────────────────────────────────────


def test_config_rule_violation_message_contains_rule_name():
    exc = ConfigRuleViolation(rule="MY_RULE", message="msg", fix="fix")
    assert "[ConfigRule:MY_RULE]" in str(exc)


def test_config_rule_violation_message_contains_message():
    exc = ConfigRuleViolation(rule="R", message="descripción del problema", fix="f")
    assert "descripción del problema" in str(exc)


def test_config_rule_violation_message_contains_fix():
    exc = ConfigRuleViolation(rule="R", message="m", fix="cómo corregirlo")
    assert "FIX:" in str(exc)
    assert "cómo corregirlo" in str(exc)


# ── E6: catcheable como ConfigurationError ────────────────────────────────────


def test_config_rule_violation_caught_as_configuration_error():
    """Raise/catch round-trip — el caller genérico no necesita conocer la subclase."""
    with pytest.raises(ConfigurationError):
        raise ConfigRuleViolation(rule="T", message="t", fix="t")


def test_config_pipeline_error_caught_as_configuration_error():
    """ConfigPipelineError también es catcheable como ConfigurationError."""
    with pytest.raises(ConfigurationError):
        raise ConfigPipelineError(
            stage=ConfigStage.VALIDATED,
            message="test pipeline error",
        )


def test_config_validation_error_caught_as_configuration_error():
    with pytest.raises(ConfigurationError):
        raise ConfigValidationError("schema inválido")


# ── E7: contrato de interfaz pública (_contract.py) ───────────────────────────


def test_public_contract_module_importable():
    """Todos los símbolos declarados en __all__ de cada módulo son importables.

    Si este test falla: un símbolo público fue eliminado o renombrado sin
    actualizar _contract.py → rotura de interfaz detectada en CI.
    """
    importlib.import_module("ocm.config._contract")


def test_contract_exception_symbols_present():
    """Los símbolos de excepción declarados en exceptions.__all__ están presentes."""
    from ocm.config.loader import exceptions

    for name in exceptions.__all__:
        assert hasattr(exceptions, name), f"exceptions.__all__ declara '{name}' pero el atributo no existe"


def test_contract_coercion_symbols_present():
    """Los símbolos de coerción declarados en coercion.__all__ están presentes."""
    from ocm.config.layers import coercion

    for name in coercion.__all__:
        assert hasattr(coercion, name), f"coercion.__all__ declara '{name}' pero el atributo no existe"


def test_contract_rules_symbols_present():
    """Los símbolos de reglas declarados en rules.__all__ están presentes."""
    from ocm.config.layers import rules

    for name in rules.__all__:
        assert hasattr(rules, name), f"rules.__all__ declara '{name}' pero el atributo no existe"
