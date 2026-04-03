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


_PLACEHOLDER_PREFIXES = ("CHANGE_ME", "YOUR_", "PLACEHOLDER", "FIXME")


def _has_placeholder(value: str) -> bool:
    return any(value.startswith(p) for p in _PLACEHOLDER_PREFIXES)


def _rule_no_placeholder_credentials(config: "AppConfig") -> list[str]:
    """Credenciales placeholder deben detectarse en todos los entornos.

    En producción → error bloqueante.
    En otros entornos → warning (no bloquea, pero queda registrado en rules output).

    Nota: credenciales vacías ya son rechazadas por ExchangeConfig.validate_credentials
    (Pydantic model_validator). Esta regla cubre placeholders que pasan la validación
    de 'no vacío' pero son valores semánticamente falsos.
    """
    errors: list[str] = []
    is_production = config.environment.name == "production"
    prefix = "ERROR" if is_production else "WARNING"

    for ex in config.exchanges:
        key    = ex.api_key.get_secret_value()
        secret = ex.api_secret.get_secret_value()
        if _has_placeholder(key) or _has_placeholder(secret):
            errors.append(
                f"[{prefix}] exchange {ex.name.value}: api_key/secret contains placeholder value"
            )

    pg = config.integrations.postgres
    if pg.enabled and pg.password and _has_placeholder(str(pg.password)):
        errors.append(f"[{prefix}] integrations.postgres.password contains placeholder value")

    # Devolver siempre la lista completa — la severidad la decide el caller.
    # En production: el loader eleva ConfigValidationError si hay errores.
    # En otros entornos: el loader puede loguear como warning sin bloquear.
    # SRP: esta función detecta, no decide.
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
