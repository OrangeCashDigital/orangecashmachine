from __future__ import annotations

"""
core/config/loader.py (PRO+)

Sistema de configuración production-grade con soporte multi-entorno automático.

Mejoras:
• Auto-detección de entorno (OCM_ENV)
• Validación de entorno
• Logging estructurado
• SafeOps reforzado
• Preparado para cache distribuido (Redis-ready)

Principios:
SOLID · KISS · DRY · SafeOps
"""

import hashlib
import logging
import os
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Union

import yaml
from dotenv import load_dotenv
from pydantic import ValidationError
from prefect import task, get_run_logger
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from core.config.schema import AppConfig, AuditEntry, CONFIG_PATH

# -----------------------------------------------------------------------------
# Init
# -----------------------------------------------------------------------------

load_dotenv()
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------

_ENV_PATTERN = re.compile(r"\$\{([^}:]+)(:-([^}]+))?\}")
_DEBOUNCE_SECONDS = 0.5
_ALLOWED_ENVS = {"development", "test", "production", "staging"}


# -----------------------------------------------------------------------------
# Exceptions
# -----------------------------------------------------------------------------

class ConfigurationError(RuntimeError):
    """Error crítico de configuración."""


# -----------------------------------------------------------------------------
# Cache (Thread-safe)
# -----------------------------------------------------------------------------

class ConfigCache:

    def __init__(self) -> None:
        self._cache: dict[str, AppConfig] = {}
        self._hashes: dict[str, str] = {}
        self._lock = threading.RLock()

    def get(self, key: str, hash_value: str) -> Optional[AppConfig]:
        with self._lock:
            if self._hashes.get(key) == hash_value:
                return self._cache.get(key)
            return None

    def set(self, key: str, hash_value: str, config: AppConfig) -> None:
        with self._lock:
            self._cache[key] = config
            self._hashes[key] = hash_value


_config_cache = ConfigCache()


# -----------------------------------------------------------------------------
# Env Resolver
# -----------------------------------------------------------------------------

class EnvResolver:

    @staticmethod
    def _replace(match: re.Match[str]) -> str:
        var, _, default = match.groups()
        value = os.getenv(var)

        if value is not None:
            return value
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


# -----------------------------------------------------------------------------
# YAML Loader
# -----------------------------------------------------------------------------

class YamlLoader:

    @staticmethod
    def load(path: Path, required: bool = True) -> dict[str, Any]:
        if not path.exists():
            if required:
                raise ConfigurationError(f"Config file not found: {path}")
            logger.warning("Optional config not found: %s", path)
            return {}

        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in {path}: {e}") from e

        if not isinstance(data, dict):
            raise ConfigurationError(f"Root must be dict: {path}")

        return data

    @classmethod
    def merge(cls, base: dict, override: dict) -> dict:
        result = dict(base)
        for k, v in override.items():
            if isinstance(result.get(k), dict) and isinstance(v, dict):
                result[k] = cls.merge(result[k], v)
            else:
                result[k] = v
        return result


# -----------------------------------------------------------------------------
# Validation
# -----------------------------------------------------------------------------

class ConfigValidator:

    @staticmethod
    def validate(data: dict, source: Path) -> AppConfig:
        try:
            return AppConfig.model_validate(data)
        except ValidationError as e:
            errors = "\n".join(
                f"[{' -> '.join(map(str, err['loc']))}] {err['msg']}"
                for err in e.errors()
            )
            raise ConfigurationError(f"Validation error ({source}):\n{errors}") from e


# -----------------------------------------------------------------------------
# Hash
# -----------------------------------------------------------------------------

def compute_hash(data: dict) -> str:
    return hashlib.sha256(
        yaml.dump(data, sort_keys=True).encode()
    ).hexdigest()


# -----------------------------------------------------------------------------
# Audit
# -----------------------------------------------------------------------------

def audit_config(config: AppConfig, key: str, h: str, source: Path) -> AppConfig:
    entry = AuditEntry(
        timestamp=datetime.now(timezone.utc),
        cache_key=key,
        hash=h,
        source_file=str(source),
    )

    return config.model_copy(update={
        "audit_log": [*config.audit_log, entry],
        "last_reload": datetime.now(timezone.utc),
    })


# -----------------------------------------------------------------------------
# Paths
# -----------------------------------------------------------------------------

def _resolve_paths(
    path: Optional[Union[str, Path]],
    env: Optional[str],
) -> tuple[Path, Optional[Path], Path]:

    config_dir = Path(path).resolve() if path else CONFIG_PATH.parent

    return (
        config_dir / "base.yaml",
        config_dir / f"{env}.yaml" if env else None,
        config_dir / "settings.yaml",
    )


# -----------------------------------------------------------------------------
# Core Loader
# -----------------------------------------------------------------------------

def load_config(
    env: Optional[str] = None,
    path: Optional[Union[str, Path]] = None,
    use_cache: bool = True,
    force_reload: bool = False,
) -> AppConfig:

    # 🔥 AUTO-DETECCIÓN DE ENTORNO
    env = env or os.getenv("OCM_ENV") or "development"

    if env not in _ALLOWED_ENVS:
        raise ConfigurationError(f"Invalid environment: '{env}'")

    base, env_path, settings = _resolve_paths(path, env)
    cache_key = f"default:{env}"

    logger.debug("Loading config | env=%s path=%s", env, path or CONFIG_PATH.parent)

    # --- Load ---
    merged = YamlLoader.load(base)

    if env_path:
        merged = YamlLoader.merge(
            merged,
            YamlLoader.load(env_path, required=False),
        )

    merged = YamlLoader.merge(
        merged,
        YamlLoader.load(settings, required=False),
    )

    # --- Resolve env ---
    merged = EnvResolver.resolve(merged)

    # --- Hash ---
    h = compute_hash(merged)

    # --- Cache ---
    if use_cache and not force_reload:
        cached = _config_cache.get(cache_key, h)
        if cached:
            logger.debug("Config cache hit | env=%s", env)
            return cached

    # --- Validate ---
    source = settings if settings.exists() else (env_path or base)
    config = ConfigValidator.validate(merged, source)

    # --- Audit ---
    config = audit_config(config, cache_key, h, source)

    # --- Cache ---
    if use_cache:
        _config_cache.set(cache_key, h, config)

    logger.info(
        "Config loaded | env=%s hash=%s source=%s",
        env,
        h[:8],
        source.name,
    )

    return config


# -----------------------------------------------------------------------------
# Watchdog
# -----------------------------------------------------------------------------

class ConfigChangeHandler(FileSystemEventHandler):

    def __init__(self, env: Optional[str], path: Optional[Union[str, Path]]):
        self.env = env
        self.path = path
        self._last = 0.0

    def on_modified(self, event):
        if not event.src_path.endswith(".yaml"):
            return

        now = time.monotonic()
        if now - self._last < _DEBOUNCE_SECONDS:
            return
        self._last = now

        try:
            load_config(env=self.env, path=self.path, force_reload=True)
            logger.info("Config reloaded: %s", event.src_path)
        except ConfigurationError as e:
            logger.error("Config reload failed: %s", e)


def watch_config_files(
    env: Optional[str] = None,
    path: Optional[Union[str, Path]] = None,
) -> Observer:

    env = env or os.getenv("OCM_ENV")

    config_dir = Path(path).resolve() if path else CONFIG_PATH.parent

    observer = Observer()
    observer.schedule(
        ConfigChangeHandler(env, path),
        str(config_dir),
        recursive=False,
    )
    observer.start()

    logger.info("Watching config dir: %s | env=%s", config_dir, env)
    return observer


# -----------------------------------------------------------------------------
# Prefect Task
# -----------------------------------------------------------------------------

@task(name="load_config", retries=2, retry_delay_seconds=[5, 30])
async def load_and_validate_config_task(
    env: Optional[str] = None,
    path: Optional[Union[str, Path]] = None,
    use_cache: bool = True,
    force_reload: bool = False,
) -> AppConfig:

    log = get_run_logger()

    config = load_config(env, path, use_cache, force_reload)

    log.info(
        "Config loaded | env=%s exchanges=%s",
        env or os.getenv("OCM_ENV"),
        getattr(config, "exchange_names", []),
    )

    return config