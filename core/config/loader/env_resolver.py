from __future__ import annotations

"""core/config/loader/env_resolver.py — Resolución de variables de entorno y entorno activo."""

import os
import re
from pathlib import Path
from typing import Any, Optional

import yaml
from dotenv import load_dotenv

from .exceptions import ConfigurationError
from ..env_vars import OCM_ENV as _OCM_ENV_VAR
from core.observability.bootstrap import pre_log

from loguru import logger

_ENV_PATTERN = re.compile(r"\$\{([^}:]+)(:-([^}]*))?\}")
_ALLOWED_ENVS = {"development", "test", "staging", "production"}

_resolved_env_cache: dict[str, str] = {}

# Valores de OCM_ENV inyectados por .env
_DOTENV_INJECTED: set[str] = set()

# Guard de idempotencia: archivos .env ya cargados (evita duplicación en re-runs)
_DOTENV_LOADED_FILES: set[str] = set()


# --------------------------------------------------
# ENV RESOLUTION ENGINE
# --------------------------------------------------
class EnvResolver:
    @staticmethod
    def _replace(match: re.Match) -> str:
        var, _, default = match.groups()
        val = os.getenv(var)
        if val is not None:
            return val
        if default is not None:
            return default
        raise ConfigurationError(f"Missing required env var: {var}")

    @classmethod
    def resolve(cls, data: Any) -> Any:
        if isinstance(data, dict):
            return {k: cls.resolve(v) for k, v in data.items()}
        if isinstance(data, list):
            return [cls.resolve(v) for v in data]
        if isinstance(data, str):
            return _ENV_PATTERN.sub(cls._replace, data)
        return data


# --------------------------------------------------
# DOTENV BOOTSTRAP (CRÍTICO)
# --------------------------------------------------
def bootstrap_dotenv() -> None:
    """Carga .env antes de construir RunConfig."""
    p = Path(".env")
    if not p.exists():
        return

    before = os.environ.get(_OCM_ENV_VAR)
    load_dotenv(p, override=False)
    after = os.environ.get(_OCM_ENV_VAR)

    if before is None and after is not None:
        _DOTENV_INJECTED.add(after)

    # Registrar en guard de idempotencia para que load_dotenv_for_env lo salte
    _DOTENV_LOADED_FILES.add(str(p.resolve()))

    # pre_log: loguru no tiene sinks aún — va a stderr + buffer para replay
    pre_log("config.dotenv_bootstrap", file=str(p), override=False)


def load_dotenv_for_env(env: str) -> None:
    """Carga archivos .env específicos del entorno. Idempotente por diseño."""
    for filename in (".env", f".env.{env}", f".env.{env}.local"):
        p = Path(filename)
        if not p.exists():
            continue

        canon = str(p.resolve())
        if canon in _DOTENV_LOADED_FILES:
            logger.debug("dotenv_skip | file={} reason=already_loaded", filename)
            continue

        before = os.environ.get(_OCM_ENV_VAR)
        load_dotenv(p, override=False)
        after = os.environ.get(_OCM_ENV_VAR)

        if before is None and after is not None:
            _DOTENV_INJECTED.add(after)

        _DOTENV_LOADED_FILES.add(canon)
        logger.debug("dotenv_loaded | file={} override=false", filename)


# --------------------------------------------------
# SETTINGS FALLBACK
# --------------------------------------------------
def read_default_env_from_settings(config_dir: Optional[Path] = None) -> Optional[str]:
    from core.config.schema import CONFIG_PATH

    settings_path = (config_dir or CONFIG_PATH.parent) / "settings.yaml"
    cache_key = str(settings_path.resolve())

    if cache_key in _resolved_env_cache:
        return _resolved_env_cache[cache_key] or None

    if not settings_path.exists():
        return None

    try:
        data = yaml.safe_load(settings_path.read_text(encoding="utf-8")) or {}
        env_section = data.get("environment", {})

        value = env_section.get("default_env") if isinstance(env_section, dict) else None

        if value:
            value = str(value)
            if value not in _ALLOWED_ENVS:
                logger.warning(
                    "env_invalid | source=settings_yaml value={} allowed={} action=fallback_development",
                    value,
                    sorted(_ALLOWED_ENVS),
                )
                value = None

        _resolved_env_cache[cache_key] = value or ""
        return value or None

    except yaml.YAMLError as exc:
        logger.warning("settings_yaml_read_error | error={}", exc)
        return None


# --------------------------------------------------
# MAIN RESOLVER
# --------------------------------------------------
def resolve_env(explicit_env: Optional[str] = None, config_dir: Optional[Path] = None) -> str:
    """
    Cascada de resolución de entorno activo.

    Prioridad:
    1. explicit
    2. env_var
    3. dotenv
    4. settings_yaml
    5. default
    """
    if explicit_env:
        pre_log("config.env_resolved", source="explicit", value=explicit_env)
        return explicit_env

    if ocm := os.getenv(_OCM_ENV_VAR):
        source = "dotenv" if ocm in _DOTENV_INJECTED else "env_var"
        pre_log("config.env_resolved", source=source, value=ocm)
        return ocm

    if yaml_env := read_default_env_from_settings(config_dir):
        pre_log("config.env_resolved", source="settings_yaml", value=yaml_env)
        return yaml_env

    pre_log("config.env_resolved", source="default", value="development")
    return "development"


# --------------------------------------------------
# TEST UTILITIES
# --------------------------------------------------
def _reset_for_testing() -> None:
    """Resetea el estado global del módulo para tests aislados.

    Limpia los tres sets/dicts de estado mutable de módulo:
      - _resolved_env_cache : caché de settings.yaml → entorno
      - _DOTENV_LOADED_FILES: guard de idempotencia de archivos .env
      - _DOTENV_INJECTED    : tracking de vars inyectadas por .env

    REGLA: llamar ÚNICAMENTE desde fixtures de pytest (autouse=True).
    Nunca llamar en código de producción — no está en __all__.

    Principios: SafeOps (tests aislados), SSOT (un solo punto de reset).
    """
    _resolved_env_cache.clear()
    _DOTENV_LOADED_FILES.clear()
    _DOTENV_INJECTED.clear()
