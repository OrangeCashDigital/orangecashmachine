# orchestration/tasks/config_tasks.py
# ===================================
"""
Loader de configuración profesional para OrangeCashMachine.

Responsabilidad
---------------
Cargar, resolver variables de entorno y validar la configuración YAML contra AppConfig.

Principios aplicados
--------------------
SOLID
    SRP – Solo carga y valida configuración.
KISS
    API simple: load_config(path) → AppConfig
DRY
    Validación delegada a Pydantic; resolución de entorno centralizada
SafeOps
    Validaciones tempranas, logs estructurados, protección contra YAML inválido
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional, Union
import os
import re
import yaml

from pydantic import ValidationError
from prefect import task, get_run_logger

from core.config.schema import AppConfig, CONFIG_PATH

# ============================================================================
# EXCEPCIONES
# ============================================================================

class ConfigurationError(RuntimeError):
    """Error relacionado con la configuración del sistema."""

# ============================================================================
# EXPRESIONES REGULARES PARA ENV
# ============================================================================

_ENV_PATTERN = re.compile(r"\$\{([^}:]+)(:-([^}]+))?\}")

# ============================================================================
# RESOLUCIÓN DE VARIABLES DE ENTORNO
# ============================================================================

def _resolve_env_value(value: str) -> str:
    """Resuelve variables de entorno en strings, soportando ${VAR} y ${VAR:-default}"""
    def replacer(match: re.Match[str]) -> str:
        var_name, default = match.group(1), match.group(3)
        env_value = os.getenv(var_name)
        if env_value is not None:
            return env_value
        if default is not None:
            return default
        raise ConfigurationError(f"Variable de entorno requerida no definida: {var_name}")
    return _ENV_PATTERN.sub(replacer, value)

def _resolve_env_in_structure(data: Any) -> Any:
    """Resuelve variables de entorno recursivamente en dicts, listas o strings"""
    if isinstance(data, dict):
        return {k: _resolve_env_in_structure(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_resolve_env_in_structure(v) for v in data]
    if isinstance(data, str):
        return _resolve_env_value(data)
    return data

# ============================================================================
# CARGA Y VALIDACIÓN DE YAML
# ============================================================================

def _load_yaml(path: Path) -> Dict[str, Any]:
    """Carga y valida que el YAML exista y sea un dict"""
    if not path.exists():
        raise ConfigurationError(f"Archivo de configuración no encontrado: {path}")
    if not path.is_file():
        raise ConfigurationError(f"Path de configuración no es un archivo: {path}")
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigurationError(f"Error al parsear YAML: {path}") from exc
    if not isinstance(data, dict):
        raise ConfigurationError(f"La raíz del YAML debe ser un dict: {path}")
    return data

def _validate_config(data: Dict[str, Any], path: Path) -> AppConfig:
    """Valida la configuración usando Pydantic y devuelve un AppConfig"""
    try:
        return AppConfig.model_validate(data)
    except ValidationError as exc:
        lines = [f"Validación de configuración fallida: {path}"]
        for err in exc.errors():
            location = " -> ".join(str(loc) for loc in err["loc"])
            message = err["msg"]
            lines.append(f"  [{location}] {message}")
        raise ConfigurationError("\n".join(lines)) from exc

def load_config(path: Optional[Union[str, Path]] = None) -> AppConfig:
    """
    Carga, resuelve y valida la configuración.
    Flujo:
      1. Determinar ruta del archivo
      2. Cargar YAML
      3. Resolver variables de entorno
      4. Validar con Pydantic
    """
    resolved_path = Path(path).resolve() if path else CONFIG_PATH
    raw_data = _load_yaml(resolved_path)
    raw_data = _resolve_env_in_structure(raw_data)
    return _validate_config(raw_data, resolved_path)

# ============================================================================
# PREFECT TASK
# ============================================================================

@task(
    name="load_and_validate_config",
    retries=2,
    retry_delay_seconds=[5, 30],
    description="Loads YAML config, resolves env variables, and validates against AppConfig schema.",
)
async def load_and_validate_config_task(path: Optional[Union[str, Path]] = None) -> AppConfig:
    """
    Prefect Task que carga y valida la configuración usando `load_config`.
    """
    log = get_run_logger()
    resolved_path = Path(path).resolve() if path else CONFIG_PATH
    log.info("Loading configuration from %s", resolved_path)
    config = load_config(resolved_path)
    log.info("Configuration loaded and validated successfully")
    return config