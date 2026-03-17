from __future__ import annotations

"""
Loader de configuración para OrangeCashMachine.

Responsabilidades:
- Cargar archivo YAML de configuración
- Resolver variables de entorno (${VAR} o ${VAR:-default})
- Validar configuración con Pydantic
- Devolver un objeto AppConfig tipado

Principios aplicados:
SOLID   → funciones pequeñas y responsabilidad única
KISS    → lógica simple y predecible
DRY     → utilidades reutilizables
SafeOps → fallos explícitos ante configuraciones inválidas
"""

import os
import re
from pathlib import Path
from typing import Any, Dict, Optional, Union

import yaml
from pydantic import ValidationError

from core.config.schema import AppConfig, CONFIG_PATH


# ============================================================================
# EXCEPCIONES
# ============================================================================

class ConfigurationError(RuntimeError):
    """Error relacionado con la configuración del sistema."""


# ============================================================================
# EXPRESIONES REGULARES
# ============================================================================

# ${VAR}
# ${VAR:-default}
_ENV_PATTERN = re.compile(r"\$\{([^}:]+)(:-([^}]+))?\}")


# ============================================================================
# RESOLUCIÓN DE VARIABLES DE ENTORNO
# ============================================================================

def _resolve_env_value(value: str) -> str:
    """
    Resuelve variables de entorno dentro de un string.

    Soporta:
        ${VAR}
        ${VAR:-default}

    Si la variable no existe y no hay default → lanza ConfigurationError.
    """

    def replacer(match: re.Match[str]) -> str:
        var_name = match.group(1)
        default = match.group(3)

        env_value = os.getenv(var_name)

        if env_value is not None:
            return env_value

        if default is not None:
            return default

        raise ConfigurationError(
            f"Variable de entorno requerida no definida: {var_name}"
        )

    return _ENV_PATTERN.sub(replacer, value)


def _resolve_env_in_structure(data: Any) -> Any:
    """
    Recorre recursivamente la estructura YAML y resuelve variables
    de entorno únicamente en valores de tipo string.
    """

    if isinstance(data, dict):
        return {k: _resolve_env_in_structure(v) for k, v in data.items()}

    if isinstance(data, list):
        return [_resolve_env_in_structure(v) for v in data]

    if isinstance(data, str):
        return _resolve_env_value(data)

    return data


# ============================================================================
# CARGA DE YAML
# ============================================================================

def _load_yaml(path: Path) -> Dict[str, Any]:
    """
    Carga un archivo YAML de configuración.
    """

    if not path.exists():
        raise ConfigurationError(f"Archivo de configuración no encontrado: {path}")

    try:
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except yaml.YAMLError as exc:
        raise ConfigurationError(f"Error al parsear YAML: {path}") from exc

    if not isinstance(data, dict):
        raise ConfigurationError(
            f"La raíz del archivo YAML debe ser un objeto: {path}"
        )

    return data


# ============================================================================
# VALIDACIÓN
# ============================================================================

def _validate_config(data: Dict[str, Any], path: Path) -> AppConfig:
    """
    Valida la configuración utilizando el schema Pydantic.
    """

    try:
        return AppConfig.model_validate(data)

    except ValidationError as exc:

        lines = [f"Validación de configuración fallida: {path}"]

        for err in exc.errors():
            location = " -> ".join(str(loc) for loc in err["loc"])
            message = err["msg"]

            lines.append(f"  [{location}] {message}")

        raise ConfigurationError("\n".join(lines)) from exc


# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def load_config(path: Optional[Union[str, Path]] = None) -> AppConfig:
    """
    Carga, procesa y valida la configuración del sistema.

    Flujo:
        1. Determinar ruta del archivo de configuración
        2. Cargar YAML
        3. Resolver variables de entorno
        4. Validar con Pydantic
        5. Retornar AppConfig

    Parameters
    ----------
    path : Optional[str | Path]
        Ruta personalizada al archivo de configuración.

    Returns
    -------
    AppConfig
        Objeto tipado con la configuración final.
    """

    resolved_path = Path(path).resolve() if path else CONFIG_PATH

    # ------------------------------------------------------------
    # Cargar YAML
    # ------------------------------------------------------------

    config_data = _load_yaml(resolved_path)

    # ------------------------------------------------------------
    # Resolver variables de entorno
    # ------------------------------------------------------------

    config_data = _resolve_env_in_structure(config_data)

    # ------------------------------------------------------------
    # Validar configuración
    # ------------------------------------------------------------

    return _validate_config(config_data, resolved_path)