from __future__ import annotations

"""core/config/rules.py

Reglas de negocio para la configuracion de OrangeCashMachine.

Cada regla es una funcion pura: (AppConfig) -> list[str]
El loader las ejecuta en orden. SRP: el loader no conoce las reglas.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.config.schema import AppConfig


def _rule_no_placeholder_password(config: "AppConfig") -> list[str]:
    """Produccion no puede tener contrasenas placeholder."""
    errors: list[str] = []
    if getattr(config, "env", None) == "production":
        db = getattr(config, "db", None)
        if db is not None:
            password = getattr(db, "password", None)
            if isinstance(password, str) and password.startswith("CHANGE_ME"):
                errors.append("db.password must not use placeholder in production")
    return errors


def _rule_production_requires_real_credentials(config: "AppConfig") -> list[str]:
    """Produccion no puede tener exchanges sin credenciales reales."""
    errors: list[str] = []
    if getattr(config, "env", None) == "production":
        exchanges = getattr(config, "exchanges", []) or []
        for ex in exchanges:
            api_key = getattr(ex, "api_key", None)
            if not api_key or str(api_key).startswith("CHANGE_ME"):
                errors.append(f"exchange {getattr(ex, 'name', '?')}: api_key required in production")
    return errors


# Registro de reglas activas — anadir aqui nuevas reglas sin tocar el loader
BUSINESS_RULES = [
    _rule_no_placeholder_password,
    _rule_production_requires_real_credentials,
]


def check_all_rules(config: "AppConfig", source: str) -> list[str]:
    """Ejecuta todas las reglas. Devuelve lista de errores acumulados."""
    errors: list[str] = []
    for rule in BUSINESS_RULES:
        errors.extend(rule(config))
    return errors
