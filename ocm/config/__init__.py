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
# -- Excepciones (todas desde loader — SSOT) ------------------------------
# -- Bootstrap de entorno y dotenv ----------------------------------------
# -- Snapshot de run -------------------------------------------------------
from .loader import (
    ConfigFileNotFoundError,
    ConfigParseError,
    ConfigurationError,
    ConfigValidationError,
    bootstrap_dotenv,
    load_dotenv_for_env,
    resolve_env,
    write_config_snapshot,
)
from .pipeline import ConfigPipelineError
from .schema import (
    CONFIG_PATH,
    AppConfig,
    ExchangeConfig,
    RedisConfig,
    RiskConfig,
    SafetyConfig,
)

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
    "ConfigPipelineError",
    # loader / bootstrap
    "bootstrap_dotenv",
    "load_dotenv_for_env",
    "resolve_env",
    # snapshot
    "write_config_snapshot",
]
