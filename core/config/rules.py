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


def _rule_no_placeholder_password(config: "AppConfig") -> list[str]:
    """Producción no puede tener contraseñas placeholder."""
    errors: list[str] = []
    if config.environment.name == "production":
        db = getattr(config, "db", None)
        if db is not None:
            password = getattr(db, "password", None)
            if isinstance(password, str) and password.startswith("CHANGE_ME"):
                errors.append("db.password must not use placeholder in production")
    return errors


def _rule_production_requires_real_credentials(config: "AppConfig") -> list[str]:
    """Producción no puede tener exchanges sin credenciales reales."""
    errors: list[str] = []
    if config.environment.name == "production":
        for ex in config.exchanges:
            api_key = getattr(ex, "api_key", None)
            if not api_key or str(api_key).startswith("CHANGE_ME"):
                errors.append(
                    f"exchange {ex.name.value}: api_key required in production"
                )
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
    _rule_no_placeholder_password,
    _rule_production_requires_real_credentials,
    _rule_debug_not_allowed_in_production,
]


def check_all_rules(config: "AppConfig", source: str) -> list[str]:
    """Ejecuta todas las reglas. Devuelve lista de errores acumulados."""
    errors: list[str] = []
    for rule in BUSINESS_RULES:
        errors.extend(rule(config))
    return errors
