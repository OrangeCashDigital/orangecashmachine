from __future__ import annotations

"""
Loader central de configuración profesional para OrangeCashMachine.

Responsabilidades
----------------
- Cargar YAML (base + env opcional + settings opcional)
- Resolver variables de entorno (${VAR} o ${VAR:-default})
- Validar con Pydantic
- Cachear configuración para evitar recargas repetidas
- Detectar cambios en disco y recargar automáticamente
- Auditoría de cambios con timestamp y hash en AppConfig
- Soportar Prefect Tasks async
- Merge multi-nivel: base → env → settings

Principios aplicados
-------------------
SOLID, KISS, DRY, SafeOps
"""

import os
import re
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Union

import yaml
from pydantic import ValidationError
from prefect import task, get_run_logger
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from core.config.schema import AppConfig, CONFIG_PATH, AuditEntry


# ============================================================================
# EXCEPCIONES
# ============================================================================

class ConfigurationError(RuntimeError):
    """Error relacionado con la configuración del sistema."""


# ============================================================================
# EXPRESIONES REGULARES PARA VARIABLES DE ENTORNO
# ============================================================================

_ENV_PATTERN = re.compile(r"\$\{([^}:]+)(:-([^}]+))?\}")


# ============================================================================
# CACHE Y HASH
# ============================================================================

_config_cache: dict[str, AppConfig] = {}
_config_hash: dict[str, str] = {}


# ============================================================================
# RESOLUCIÓN DE VARIABLES DE ENTORNO
# ============================================================================

def _resolve_env_value(value: str) -> str:
    """Resuelve variables de entorno en strings con soporte para default."""
    def replacer(match: re.Match[str]) -> str:
        var_name, _, default = match.groups()
        env_value = os.getenv(var_name)
        if env_value is not None:
            return env_value
        if default is not None:
            return default
        raise ConfigurationError(f"Variable de entorno requerida no definida: {var_name}")
    return _ENV_PATTERN.sub(replacer, value)


def _resolve_env_in_structure(data: Any) -> Any:
    """Resuelve variables de entorno recursivamente en dicts, listas o strings."""
    if isinstance(data, dict):
        return {k: _resolve_env_in_structure(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_resolve_env_in_structure(v) for v in data]
    if isinstance(data, str):
        return _resolve_env_value(data)
    return data


# ============================================================================
# CARGA Y MERGE DE YAML
# ============================================================================

def _load_yaml(path: Path, required: bool = True) -> dict[str, Any]:
    """Carga un YAML y valida que la raíz sea un dict."""
    if not path.exists() or not path.is_file():
        if required:
            raise ConfigurationError(f"Archivo de configuración no válido: {path}")
        return {}
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigurationError(f"Error al parsear YAML: {path}") from exc
    if not isinstance(data, dict):
        raise ConfigurationError(f"La raíz del YAML debe ser un dict: {path}")
    return data


def _merge_dicts(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Merge recursivo de diccionarios: override > base."""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _merge_dicts(result[key], value)
        else:
            result[key] = value
    return result


# ============================================================================
# VALIDACIÓN
# ============================================================================

def _validate_config(data: dict[str, Any], path: Path) -> AppConfig:
    """Valida con Pydantic y formatea errores detallados para SafeOps."""
    try:
        return AppConfig.model_validate(data)
    except ValidationError as exc:
        error_lines = [f"Validación de configuración fallida: {path}"]
        for err in exc.errors():
            location = " -> ".join(str(loc) for loc in err["loc"])
            message = err["msg"]
            error_lines.append(f"  [{location}] {message}")
        raise ConfigurationError("\n".join(error_lines)) from exc


# ============================================================================
# HASH / AUDITORÍA
# ============================================================================

def _compute_sha256(data: dict[str, Any]) -> str:
    """Calcula hash SHA256 del YAML para detección de cambios."""
    yaml_bytes = yaml.dump(data, sort_keys=True).encode("utf-8")
    return hashlib.sha256(yaml_bytes).hexdigest()


def _log_audit_in_config(config: AppConfig, cache_key: str, config_hash: str, path: Path) -> None:
    """Registra auditoría dentro del AppConfig."""
    entry = AuditEntry(
        timestamp=datetime.now(timezone.utc),
        cache_key=cache_key,
        hash=config_hash,
        source_file=str(path)
    )
    config.audit_log.append(entry)
    config.last_reload = datetime.now(timezone.utc)


# ============================================================================
# FUNCIÓN PRINCIPAL DE CARGA
# ============================================================================

def load_config(
    env: Optional[str] = None,
    path: Optional[Union[str, Path]] = None,
    use_cache: bool = True,
    force_reload: bool = False
) -> AppConfig:
    """
    Carga, mergea, resuelve y valida la configuración multi-level con cache
    y auditoría de cambios.
    """
    cache_key = f"default:{env}" if env else "default"
    config_dir = Path(path).resolve() if path else CONFIG_PATH.parent

    # Carga y merge
    base_config = _load_yaml(config_dir / "base.yaml")
    merged_config = base_config
    env_path = config_dir / f"{env}.yaml" if env else None
    if env_path:
        env_config = _load_yaml(env_path, required=False)
        merged_config = _merge_dicts(merged_config, env_config)
    settings_path = config_dir / "settings.yaml"
    settings_config = _load_yaml(settings_path, required=False)
    merged_config = _merge_dicts(merged_config, settings_config)

    # Resolver variables de entorno
    merged_config = _resolve_env_in_structure(merged_config)

    # Detectar cambios mediante hash
    current_hash = _compute_sha256(merged_config)
    if use_cache and not force_reload:
        if _config_hash.get(cache_key) == current_hash and cache_key in _config_cache:
            return _config_cache[cache_key]

    # Validar
    validation_path = settings_path if settings_config else (env_path if env else config_dir / "base.yaml")
    config = _validate_config(merged_config, validation_path)

    # Guardar cache y auditoría en AppConfig
    if use_cache:
        _config_cache[cache_key] = config
        _config_hash[cache_key] = current_hash
        _log_audit_in_config(config, cache_key, current_hash, validation_path)

    return config


# ============================================================================
# WATCH FILESYSTEM PARA RECARGA AUTOMÁTICA
# ============================================================================

class ConfigChangeHandler(FileSystemEventHandler):
    """Recarga la configuración automáticamente si un YAML cambia."""
    def __init__(self, env: Optional[str] = None, path: Optional[Union[str, Path]] = None):
        self.env = env
        self.path = path

    def on_modified(self, event):
        if event.src_path.endswith(".yaml"):
            try:
                cfg = load_config(env=self.env, path=self.path, force_reload=True)
                print(f"[CONFIG] Recargada por cambio en {event.src_path}, hash: {_config_hash.get(f'default:{self.env}' if self.env else 'default')}")
            except ConfigurationError as exc:
                print(f"[CONFIG ERROR] {exc}")


def watch_config_files(env: Optional[str] = None, path: Optional[Union[str, Path]] = None) -> Observer:
    """Inicia watcher para recarga automática de archivos YAML de configuración."""
    config_dir = Path(path).resolve() if path else CONFIG_PATH.parent
    event_handler = ConfigChangeHandler(env=env, path=path)
    observer = Observer()
    observer.schedule(event_handler, str(config_dir), recursive=False)
    observer.start()
    print(f"[CONFIG WATCHER] Observando cambios en {config_dir}")
    return observer


# ============================================================================
# PREFECT TASK ASYNC
# ============================================================================

@task(
    name="load_and_validate_config",
    retries=2,
    retry_delay_seconds=[5, 30],
    description="Carga YAML, resuelve variables de entorno y valida con AppConfig",
)
async def load_and_validate_config_task(
    env: Optional[str] = None,
    path: Optional[Union[str, Path]] = None,
    use_cache: bool = True,
    force_reload: bool = False
) -> AppConfig:
    log = get_run_logger()
    config_dir = Path(path).resolve() if path else CONFIG_PATH.parent
    log.info("Cargando configuración desde %s (entorno: %s)", config_dir, env or "base")
    config = load_config(env=env, path=config_dir, use_cache=use_cache, force_reload=force_reload)
    log.info(
        "Configuración cargada correctamente. Hash: %s",
        _config_hash.get(f"default:{env}" if env else "default")
    )
    return config