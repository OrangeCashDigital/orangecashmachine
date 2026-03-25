from __future__ import annotations

"""
core/config/rules.py
====================

Reglas de negocio para la configuración de OrangeCashMachine.

Cada regla es una función pura: (AppConfig) -> list[str]
El loader las ejecuta en orden. SRP: el loader no conoce las reglas.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.config.schema import AppConfig


def _rule_no_placeholder_credentials(config: "AppConfig") -> list[str]:
    """Producción no puede tener credenciales placeholder (CHANGE_ME).

    Nota: credenciales vacías ya son rechazadas por ExchangeConfig.validate_credentials
    (Pydantic model_validator). Esta regla cubre el caso específico de placeholders
    que pasan la validación de 'no vacío' pero son valores falsos.
    """
    errors: list[str] = []
    if config.environment.name == "production":
        for ex in config.exchanges:
            key = ex.api_key.get_secret_value()
            secret = ex.api_secret.get_secret_value()
            if key.startswith("CHANGE_ME") or secret.startswith("CHANGE_ME"):
                errors.append(
                    f"exchange {ex.name.value}: api_key/secret must not use "
                    "CHANGE_ME placeholder in production"
                )
        pg = config.integrations.postgres
        if pg.enabled and pg.password and str(pg.password).startswith("CHANGE_ME"):
            errors.append("integrations.postgres.password must not use placeholder in production")
    return errors


def _rule_debug_not_allowed_in_production(config: "AppConfig") -> list[str]:
    """Producción no puede tener debug=True en EnvironmentConfig."""
    errors: list[str] = []
    if config.environment.name == "production" and config.environment.debug:
        errors.append(
            "environment.debug must be False in production — "
            "use OCM_DEBUG=false or remove debug flag from YAML"
        )
    return errors


# Registro de reglas activas — agregar aquí nuevas reglas sin tocar el loader
BUSINESS_RULES = [
    _rule_no_placeholder_credentials,
    _rule_debug_not_allowed_in_production,
]


def check_all_rules(config: "AppConfig", source: str) -> list[str]:
    """Ejecuta todas las reglas. Devuelve lista de errores acumulados."""
    errors: list[str] = []
    for rule in BUSINESS_RULES:
        errors.extend(rule(config))
    return errors
