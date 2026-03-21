from __future__ import annotations

"""
core/config/loader.py

Production-grade configuration loader for OrangeCashMachine.

Principles: SOLID - KISS - DRY - SafeOps
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

try:
    from prometheus_client import Counter, Histogram
    _PROMETHEUS_AVAILABLE = True
except ImportError:
    _PROMETHEUS_AVAILABLE = False

try:
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer
    _WATCHDOG_AVAILABLE = True
except ImportError:
    _WATCHDOG_AVAILABLE = False
    FileSystemEventHandler = object

try:
    from prefect import get_run_logger, task
    _PREFECT_AVAILABLE = True
except ImportError:
    _PREFECT_AVAILABLE = False

from core.config.schema import AppConfig, AuditEntry, CONFIG_PATH

logger = logging.getLogger(__name__)

if _PROMETHEUS_AVAILABLE:
    CONFIG_LOAD_COUNT = Counter("config_loads_total", "Config load attempts", ["env", "status"])
    CONFIG_LOAD_DURATION = Histogram("config_load_duration_seconds", "Config load duration", ["env"])
    CONFIG_CACHE_HITS = Counter("config_cache_hits_total", "Cache hits", ["env"])
else:
    class _NoOpMetric:
        def labels(self, **_):
            return self
        def inc(self, _=1):
            pass
        def observe(self, _):
            pass
    CONFIG_LOAD_COUNT = _NoOpMetric()
    CONFIG_LOAD_DURATION = _NoOpMetric()
    CONFIG_CACHE_HITS = _NoOpMetric()

_ENV_PATTERN = re.compile(r"\$\{([^}:]+)(:-([^}]+))?\}")
_ALLOWED_ENVS = {"development", "test", "staging", "production"}
_WATCH_ENABLED_ENVS = {"development", "test", "local"}
_DEBOUNCE_SECONDS = 0.5
_SENSITIVE_KEYS = frozenset({
    "password", "passwd", "secret", "token", "api_key", "apikey",
    "private_key", "auth", "credential", "credentials", "access_key",
    "secret_key", "jwt", "bearer", "passphrase", "encryption_key",
})


class ConfigurationError(RuntimeError):
    """Critical configuration error."""


class ConfigValidationError(ConfigurationError):
    """Business rule violation."""


class SecretMasker:
    MASK = "***REDACTED***"

    @classmethod
    def mask(cls, data: Any, _depth: int = 0) -> Any:
        if _depth > 10:
            return data
        if isinstance(data, dict):
            return {
                k: cls.MASK if cls._is_sensitive(k) else cls.mask(v, _depth + 1)
                for k, v in data.items()
            }
        if isinstance(data, list):
            return [cls.mask(v, _depth + 1) for v in data]
        return data

    @classmethod
    def _is_sensitive(cls, key: str) -> bool:
        return any(s in key.lower() for s in _SENSITIVE_KEYS)


class ConfigCache:
    def __init__(self) -> None:
        self._cache: dict[str, AppConfig] = {}
        self._hashes: dict[str, str] = {}
        self._lock = threading.RLock()

    def get(self, key: str, h: str) -> Optional[AppConfig]:
        with self._lock:
            if self._hashes.get(key) == h:
                return self._cache.get(key)
            return None

    def set(self, key: str, h: str, config: AppConfig) -> None:
        with self._lock:
            self._cache[key] = config
            self._hashes[key] = h

    def invalidate(self, key: str) -> None:
        with self._lock:
            self._cache.pop(key, None)
            self._hashes.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
            self._hashes.clear()


_config_cache = ConfigCache()


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


class YamlLoader:
    @staticmethod
    def load(path: Path, required: bool = True) -> dict[str, Any]:
        if not path.exists():
            if required:
                raise ConfigurationError(f"Missing config file: {path}")
            logger.debug("Optional config not found, skipping: %s", path)
            return {}
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError as exc:
            raise ConfigurationError(f"Invalid YAML in {path}: {exc}") from exc
        if not isinstance(data, dict):
            raise ConfigurationError(f"Root must be a mapping in: {path}")
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


class ConfigValidator:
    @staticmethod
    def validate(data: dict, source: str) -> AppConfig:
        try:
            config = AppConfig.model_validate(data)
        except ValidationError as exc:
            errors = "\n".join(
                f"  [{' -> '.join(map(str, err['loc']))}] {err['msg']}"
                for err in exc.errors()
            )
            raise ConfigurationError(f"Validation error ({source}):\n{errors}") from exc
        ConfigValidator._check_business_rules(config, source)
        return config

    @staticmethod
    def _check_business_rules(config: AppConfig, source: str) -> None:
        errors: list[str] = []
        if getattr(config, "env", None) == "production":
            db = getattr(config, "db", None)
            if db is not None:
                password = getattr(db, "password", None)
                if isinstance(password, str) and password.startswith("CHANGE_ME"):
                    errors.append("db.password must not use placeholder in production")
        if errors:
            raise ConfigValidationError(
                f"Business rule violations ({source}):\n" + "\n".join(f"  - {e}" for e in errors)
            )


def _compute_hash(data: dict) -> str:
    return hashlib.sha256(yaml.dump(data, sort_keys=True).encode()).hexdigest()


def _make_cache_key(env: str, config_dir: Path) -> str:
    dir_hash = hashlib.sha256(str(config_dir.resolve()).encode()).hexdigest()[:8]
    return f"{dir_hash}:{env}"


def _load_dotenv_for_env(env: str) -> None:
    for filename in (".env", f".env.{env}", f".env.{env}.local"):
        p = Path(filename)
        if p.exists():
            load_dotenv(p, override=True)
            logger.debug("Loaded dotenv: %s", filename)


def _audit(config: AppConfig, key: str, h: str, source: str) -> AppConfig:
    entry = AuditEntry(
        timestamp=datetime.now(timezone.utc),
        cache_key=key,
        hash=h,
        source_file=source,
    )
    return config.model_copy(update={
        "audit_log": [*config.audit_log, entry],
        "last_reload": datetime.now(timezone.utc),
    })


def load_config(
    env: Optional[str] = None,
    path: Optional[Union[str, Path]] = None,
    use_cache: bool = True,
    force_reload: bool = False,
) -> AppConfig:
    """Load, merge, validate, audit and cache config. Priority: settings > {env} > base."""
    env = env or os.getenv("OCM_ENV") or "development"
    if env not in _ALLOWED_ENVS:
        raise ConfigurationError(f"Invalid environment: '{env}'. Allowed: {sorted(_ALLOWED_ENVS)}")

    config_dir = Path(path).resolve() if path else CONFIG_PATH.parent
    cache_key = _make_cache_key(env, config_dir)
    _load_dotenv_for_env(env)

    base = config_dir / "base.yaml"
    env_file = config_dir / f"{env}.yaml"
    settings = config_dir / "settings.yaml"

    start = time.monotonic()
    try:
        merged = YamlLoader.load(base, required=True)
        merged = YamlLoader.merge(merged, YamlLoader.load(env_file, required=False))
        merged = YamlLoader.merge(merged, YamlLoader.load(settings, required=False))
        merged = EnvResolver.resolve(merged)
        h = _compute_hash(merged)

        if use_cache and not force_reload:
            cached = _config_cache.get(cache_key, h)
            if cached:
                CONFIG_CACHE_HITS.labels(env=env).inc()
                CONFIG_LOAD_COUNT.labels(env=env, status="cache_hit").inc()
                logger.debug("Config cache hit | env=%s hash=%s", env, h[:8])
                return cached

        source_name = next(
            (p.name for p in (settings, env_file, base) if p.exists()), base.name
        )
        config = ConfigValidator.validate(merged, source_name)
        config = _audit(config, cache_key, h, source_name)

        if use_cache:
            _config_cache.set(cache_key, h, config)

        duration = time.monotonic() - start
        CONFIG_LOAD_COUNT.labels(env=env, status="success").inc()
        CONFIG_LOAD_DURATION.labels(env=env).observe(duration)
        logger.info("Config loaded | env=%s hash=%s source=%s duration=%.3fs", env, h[:8], source_name, duration)
        return config

    except Exception as exc:
        CONFIG_LOAD_COUNT.labels(env=env, status="error").inc()
        logger.error("Config load failed | env=%s error=%s", env, exc)
        raise


class ConfigChangeHandler(FileSystemEventHandler):
    def __init__(self, env: Optional[str], path: Optional[Union[str, Path]]) -> None:
        self.env = env
        self.path = path
        self._last = 0.0

    def on_modified(self, event: Any) -> None:
        if not str(event.src_path).endswith(".yaml"):
            return
        now = time.monotonic()
        if now - self._last < _DEBOUNCE_SECONDS:
            return
        self._last = now
        config_dir = Path(self.path).resolve() if self.path else CONFIG_PATH.parent
        _config_cache.invalidate(_make_cache_key(self.env or "development", config_dir))
        try:
            load_config(self.env, self.path, force_reload=True)
            logger.info("Config hot-reloaded | file=%s", event.src_path)
        except ConfigurationError as exc:
            logger.error("Config hot-reload failed | file=%s error=%s", event.src_path, exc)


def watch_config_files(
    env: Optional[str] = None,
    path: Optional[Union[str, Path]] = None,
    watch: Optional[bool] = None,
) -> Optional[Any]:
    if not _WATCHDOG_AVAILABLE:
        logger.warning("Hot-reload unavailable: run pip install watchdog")
        return None
    env = env or os.getenv("OCM_ENV") or "development"
    enabled = watch if watch is not None else (env in _WATCH_ENABLED_ENVS)
    if not enabled:
        logger.info("Hot-reload disabled | env=%s", env)
        return None
    config_dir = Path(path).resolve() if path else CONFIG_PATH.parent
    observer = Observer()
    observer.schedule(ConfigChangeHandler(env, path), str(config_dir), recursive=False)
    observer.start()
    logger.info("Hot-reload started | dir=%s env=%s", config_dir, env)
    return observer


def stop_config_watcher(observer: Optional[Any]) -> None:
    if observer and observer.is_alive():
        observer.stop()
        observer.join(timeout=2.0)
        logger.info("Hot-reload stopped")


if _PREFECT_AVAILABLE:
    @task(name="load_config", retries=2, retry_delay_seconds=[5, 30])
    async def load_and_validate_config_task(
        env: Optional[str] = None,
        path: Optional[Union[str, Path]] = None,
        use_cache: bool = True,
        force_reload: bool = False,
    ) -> AppConfig:
        prefect_log = get_run_logger()
        config = load_config(env, path, use_cache, force_reload)
        prefect_log.info("Config loaded | env=%s exchanges=%s", env or os.getenv("OCM_ENV"), getattr(config, "exchange_names", []))
        return config


__all__ = [
    "load_config",
    "watch_config_files",
    "stop_config_watcher",
    "ConfigCache",
    "ConfigurationError",
    "ConfigValidationError",
    "SecretMasker",
    "CONFIG_LOAD_COUNT",
    "CONFIG_LOAD_DURATION",
    "CONFIG_CACHE_HITS",
    *(["load_and_validate_config_task"] if _PREFECT_AVAILABLE else []),
]
