from __future__ import annotations

"""
core/config/loader/__init__.py
==============================
Paquete de carga de configuración. Flujo:
  YamlLoader → EnvResolver → ConfigValidator → audit → ConfigCache
"""

import time
from pathlib import Path
from typing import Optional, Union

from pydantic import ValidationError

from .audit         import record as _audit
from .cache         import ConfigCache, _config_cache, make_cache_key
from .env_resolver  import EnvResolver, load_dotenv_for_env, resolve_env, _ALLOWED_ENVS
from .env_overrides import apply_env_overrides
from .exceptions    import ConfigurationError, ConfigValidationError
from .metrics       import (
    CONFIG_CACHE_HITS, CONFIG_LOAD_COUNT, CONFIG_LOAD_DURATION,
    CONFIG_RELOAD_TIME, _PROMETHEUS_AVAILABLE,
)
from .secret_masker import SecretMasker
from .validator     import ConfigValidator
from .watch         import watch_config_files, stop_config_watcher
from .yaml_loader   import YamlLoader, compute_hash

_make_cache_key      = make_cache_key
_load_dotenv_for_env = load_dotenv_for_env

from loguru import logger


def load_config(
    env:          Optional[str]             = None,
    path:         Optional[Union[str, Path]] = None,
    use_cache:    bool                       = True,
    force_reload: bool                       = False,
    market_type:  Optional[str]             = None,
):
    from core.config.schema import CONFIG_PATH

    env = resolve_env(env, Path(path).resolve() if path else None)
    if env not in _ALLOWED_ENVS:
        raise ConfigurationError(
            f"Invalid environment: '{env}'. Allowed: {sorted(_ALLOWED_ENVS)}"
        )

    config_dir = Path(path).resolve() if path else CONFIG_PATH.parent
    cache_key  = make_cache_key(env, config_dir, market_type)
    load_dotenv_for_env(env)

    base     = config_dir / "base.yaml"
    env_file = config_dir / f"{env}.yaml"
    settings = config_dir / "settings.yaml"

    start = time.monotonic()
    try:
        merged = YamlLoader.load(base, required=True)
        env_data = YamlLoader.load(env_file, required=False)
        merged = YamlLoader.merge(merged, env_data)
        merged = YamlLoader.merge(merged, YamlLoader.load(settings, required=False))
        # environment.name y environment.debug son campos de identidad del entorno.
        # settings.yaml puede overridear parámetros operacionales pero no puede
        # cambiar quién es el entorno — eso lo fija <env>.yaml con precedencia absoluta.
        if "environment" in env_data:
            merged.setdefault("environment", {})
            for field in ("name", "debug"):
                if field in env_data["environment"]:
                    merged["environment"][field] = env_data["environment"][field]
        merged = EnvResolver.resolve(merged)
        merged = apply_env_overrides(merged)
        h      = compute_hash(merged)

        if use_cache and not force_reload:
            cached = _config_cache.get(cache_key, h)
            if cached:
                CONFIG_CACHE_HITS.labels(env=env).inc()
                CONFIG_LOAD_COUNT.labels(env=env, status="cache_hit").inc()
                logger.debug("config_cache_hit | env={} hash={}", env, h[:8])
                return cached

        # Audit honesto: refleja todos los archivos mergeados, no solo el primero que existe
        source_name = "+".join(
            p.name for p in (base, env_file, settings) if p.exists()
        ) or base.name
        config = ConfigValidator.validate(merged, source_name)
        config = _audit(config, cache_key, h, source_name)

        if use_cache:
            _config_cache.set(cache_key, h, config)

        duration = time.monotonic() - start
        CONFIG_LOAD_COUNT.labels(env=env, status="success").inc()
        CONFIG_LOAD_DURATION.labels(env=env).observe(duration)
        logger.info(
            "config_loaded | env={} hash={} source={} duration_seconds={:.3f}",
            env, h[:8], source_name, duration,
        )
        return config

    except (ConfigurationError, ConfigValidationError):
        # Ya tienen tipo preciso — dejar propagar sin envolver
        CONFIG_LOAD_COUNT.labels(env=env, status="error").inc()
        raise

    except FileNotFoundError as exc:
        CONFIG_LOAD_COUNT.labels(env=env, status="error").inc()
        logger.error("config_file_not_found | env={} error={}", env, exc)
        raise ConfigurationError(
            f"Config file not found | env={env} path={exc.filename}"
        ) from exc

    except ValidationError as exc:
        # ValidationError de pydantic — error de schema, no de IO
        CONFIG_LOAD_COUNT.labels(env=env, status="error").inc()
        logger.error("config_validation_failed | env={} errors={}", env, exc.error_count())
        raise ConfigValidationError(
            f"Config schema validation failed | env={env} errors={exc.error_count()}"
        ) from exc

    except Exception as exc:
        # Residual: errores de YAML malformado, permisos, etc.
        CONFIG_LOAD_COUNT.labels(env=env, status="error").inc()
        logger.error(
            "config_load_failed | env={} type={} error={}",
            env, type(exc).__name__, exc,
        )
        raise ConfigurationError(
            f"Config load failed | env={env} type={type(exc).__name__}"
        ) from exc


try:
    from .prefect_integration import load_and_validate_config_task
    _PREFECT_AVAILABLE_LOADER = True
except ImportError:
    _PREFECT_AVAILABLE_LOADER = False


__all__ = [
    "load_config", "watch_config_files", "stop_config_watcher",
    "ConfigCache", "ConfigurationError", "ConfigValidationError",
    "SecretMasker", "YamlLoader", "EnvResolver", "ConfigValidator",
    "_make_cache_key", "_load_dotenv_for_env",
    "CONFIG_LOAD_COUNT", "CONFIG_LOAD_DURATION", "CONFIG_RELOAD_TIME",
    "CONFIG_CACHE_HITS", "_PROMETHEUS_AVAILABLE",
    *(["load_and_validate_config_task"] if _PREFECT_AVAILABLE_LOADER else []),
]
