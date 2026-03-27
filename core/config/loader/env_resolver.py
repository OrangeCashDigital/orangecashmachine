from __future__ import annotations

"""core/config/loader/env_resolver.py — Resolución de variables de entorno y entorno activo."""

import os
import re
from pathlib import Path
from typing import Any, Optional

import yaml
from dotenv import load_dotenv

from .exceptions import ConfigurationError
from core.config.env_vars import OCM_ENV as _OCM_ENV_VAR

from loguru import logger

_ENV_PATTERN  = re.compile(r"\$\{([^}:]+)(:-([^}]*))?\}")
_ALLOWED_ENVS = {"development", "test", "staging", "production"}

_resolved_env_cache: dict[str, str] = {}

# Valores de OCM_ENV inyectados por .env (no presentes antes de load_dotenv).
# Permite distinguir source=env_var (proceso) vs source=dotenv (archivo).
_DOTENV_INJECTED: set[str] = set()


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


def bootstrap_dotenv() -> None:
    """Carga .env base antes de RunConfig para que resolve_env vea source=dotenv."""
    p = Path(".env")
    if p.exists():
        before = os.environ.get(_OCM_ENV_VAR)
        load_dotenv(p, override=False)
        after = os.environ.get(_OCM_ENV_VAR)
        if before is None and after is not None:
            _DOTENV_INJECTED.add(after)


def load_dotenv_for_env(env: str) -> None:
    for filename in (".env", f".env.{env}", f".env.{env}.local"):
        p = Path(filename)
        if p.exists():
            # override=False: os.environ tiene prioridad sobre .env
            # SSOT: el proceso (deployment/test/operador) es la fuente de mayor autoridad.
            # .env es un convenio local que solo rellena vars ausentes, nunca las sobreescribe.
            before = os.environ.get(_OCM_ENV_VAR)
            load_dotenv(p, override=False)
            after = os.environ.get(_OCM_ENV_VAR)
            # Si OCM_ENV no existía antes y ahora existe, fue inyectada por .env
            if before is None and after is not None:
                _DOTENV_INJECTED.add(after)
            logger.debug("dotenv_loaded | file={} override=false", filename)


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
                    "env_invalid | source=settings_yaml value=%s allowed=%s action=fallback_development",
                    value, sorted(_ALLOWED_ENVS),
                )
                value = None
        _resolved_env_cache[cache_key] = value or ""
        return value or None
    except yaml.YAMLError as exc:
        logger.warning("settings_yaml_read_error | error={}", exc)
        return None


def resolve_env(explicit_env: Optional[str] = None, config_dir: Optional[Path] = None) -> str:
    """
    Cascada de resolución de entorno activo.

    Prioridad (mayor → menor):
    1. explicit      — argumento directo (tests, bootstrap interno con env conocido)
    2. env_var       — OCM_ENV presente en el proceso ANTES de cargar .env
    3. dotenv        — OCM_ENV inyectada por archivo .env
    4. settings_yaml — environment.default_env en settings.yaml
    5. default       — "development"
    """
    if explicit_env:
        logger.debug("env_resolved | source=explicit value={}", explicit_env)
        return explicit_env
    if ocm := os.getenv(_OCM_ENV_VAR):
        source = "dotenv" if ocm in _DOTENV_INJECTED else "env_var"
        logger.debug("env_resolved | source={} value={}", source, ocm)
        return ocm
    if yaml_env := read_default_env_from_settings(config_dir):
        logger.debug("env_resolved | source=settings_yaml value={}", yaml_env)
        return yaml_env
    logger.debug("env_resolved | source=default value=development")
    return "development"
