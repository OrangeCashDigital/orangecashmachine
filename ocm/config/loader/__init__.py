from __future__ import annotations

"""
ocm/config/loader/__init__.py
==============================

API pública del subpaquete ``ocm.config.loader``.

Re-exporta los símbolos que los consumidores externos necesitan,
ocultando la estructura interna de módulos (Clean Architecture —
bounded context con superficie controlada).

Consumo canónico::

    from ocm.config.loader import (
        load, merge, compute_hash,   # yaml_loader
        YamlLoader,                  # shim compat
        EnvResolver,                 # env_resolver
        bootstrap_dotenv,
        load_dotenv_for_env,
        resolve_env,
        ConfigurationError,          # exceptions
        ConfigFileNotFoundError,
        ConfigParseError,
        ConfigValidationError,
        write_config_snapshot,       # snapshot
    )

Principios: Clean Architecture (facade) · DIP · Encapsulamiento.
"""

# -- yaml_loader ----------------------------------------------------------
# -- env_resolver ---------------------------------------------------------
from .env_resolver import (
    EnvResolver,
    bootstrap_dotenv,
    load_dotenv_for_env,
    resolve_env,
)

# -- exceptions -----------------------------------------------------------
from .exceptions import (
    ConfigFileNotFoundError,
    ConfigParseError,
    ConfigurationError,
    ConfigValidationError,
)

# -- snapshot -------------------------------------------------------------
from .snapshot import write_config_snapshot
from .yaml_loader import (
    YamlLoader,  # shim de compatibilidad — ver yaml_loader.py
    compute_hash,
    load,
    merge,
)

__all__ = [
    # yaml_loader
    "load",
    "merge",
    "compute_hash",
    "YamlLoader",
    # env_resolver
    "EnvResolver",
    "bootstrap_dotenv",
    "load_dotenv_for_env",
    "resolve_env",
    # exceptions
    "ConfigurationError",
    "ConfigFileNotFoundError",
    "ConfigParseError",
    "ConfigValidationError",
    # snapshot
    "write_config_snapshot",
]
