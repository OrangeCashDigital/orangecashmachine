from __future__ import annotations

"""
core/config/loader/__init__.py
==============================
Paquete de carga de configuración — API mínima post-refactor.

Punto de entrada canónico:
    from core.config.hydra_loader import load_appconfig_standalone

Módulos internos activos:
    env_resolver  — resolución de entorno y dotenv
    env_overrides — OCM_* env vars sobre dict YAML
    yaml_loader   — carga y merge recursivo de YAML
    snapshot      — snapshot inmutable por run_id
    exceptions    — ConfigurationError, ConfigValidationError

Módulos eliminados (eran dead code):
    watch, metrics, cache, audit, secret_masker,
    prefect_integration, validator

Nota sobre _reset_for_testing:
    env_resolver expone _reset_for_testing() para fixtures de pytest.
    No está en __all__ — es API interna de test, nunca de producción.
"""

from .env_resolver  import (
    EnvResolver,
    resolve_env,
    load_dotenv_for_env,
    bootstrap_dotenv,
)
from .env_overrides import apply_env_overrides
from .exceptions    import ConfigurationError, ConfigValidationError
from .snapshot      import write_config_snapshot
from .yaml_loader   import YamlLoader, compute_hash


__all__ = [
    # resolución de entorno
    "EnvResolver",
    "resolve_env",
    "load_dotenv_for_env",
    "bootstrap_dotenv",
    # overrides
    "apply_env_overrides",
    # excepciones
    "ConfigurationError",
    "ConfigValidationError",
    # snapshot
    "write_config_snapshot",
    # YAML
    "YamlLoader",
    "compute_hash",
]
