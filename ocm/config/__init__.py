from __future__ import annotations

"""
ocm/config/__init__.py
========================

API pública del paquete ``ocm.config``.

Superficie de acceso canónica para todo el subsistema de configuración.
Los consumidores importan desde aquí — nunca desde los módulos internos.

Consumo canónico::

    # Tipos principales
    from ocm.config import AppConfig, ExchangeConfig, RiskConfig

    # Excepciones (para manejo de errores en main.py y tests)
    from ocm.config import ConfigurationError, ConfigValidationError
    from ocm.config import ConfigFileNotFoundError, ConfigParseError

    # Loader (para uso directo en bootstrap de proceso)
    from ocm.config import bootstrap_dotenv, resolve_env

    # Snapshot (para auditoría de runs)
    from ocm.config import write_config_snapshot

Principios: Clean Architecture (facade) · DIP · Encapsulamiento ·
            Bounded Context (DDD).
"""

# -- Tipos de configuración (schema) --------------------------------------
from .schema import (
    AppConfig,
    ExchangeConfig,
    RiskConfig,
    SafetyConfig,
    RedisConfig,
    CONFIG_PATH,
)

# -- Excepciones (todas desde loader — SSOT) ------------------------------
from .loader import (
    ConfigurationError,
    ConfigFileNotFoundError,
    ConfigParseError,
    ConfigValidationError,
)

# -- Bootstrap de entorno y dotenv ----------------------------------------
from .loader import (
    bootstrap_dotenv,
    load_dotenv_for_env,
    resolve_env,
)

# -- Snapshot de run -------------------------------------------------------
from .loader import write_config_snapshot

__all__ = [
    # schema
    "AppConfig",
    "ExchangeConfig",
    "RiskConfig",
    "SafetyConfig",
    "RedisConfig",
    "CONFIG_PATH",
    # exceptions
    "ConfigurationError",
    "ConfigFileNotFoundError",
    "ConfigParseError",
    "ConfigValidationError",
    # loader / bootstrap
    "bootstrap_dotenv",
    "load_dotenv_for_env",
    "resolve_env",
    # snapshot
    "write_config_snapshot",
]
