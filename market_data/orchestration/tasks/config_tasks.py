"""
orchestration/tasks/config_tasks.py
===================================

Responsabilidad única
---------------------
Cargar el archivo YAML de configuración y validarlo contra el schema
Pydantic AppConfig.

Este módulo no conoce los pipelines ni contiene lógica de negocio.

Principios aplicados
--------------------
SOLID
    SRP – Solo carga y valida configuración.

KISS
    API simple: path → AppConfig.

DRY
    Validación delegada al schema Pydantic.

SafeOps
    Validaciones tempranas
    logs estructurados
    protección contra YAML inválido
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from prefect import task, get_run_logger
from pydantic import ValidationError

from core.config.schema import AppConfig


# ==========================================================
# Prefect Task
# ==========================================================

@task(
    name="load_and_validate_config",
    retries=2,
    retry_delay_seconds=[5, 30],
    description="Loads settings.yaml and validates it against AppConfig schema.",
)
def load_and_validate_config(path: Path) -> AppConfig:
    """
    Load and validate application configuration.

    Parameters
    ----------
    path : Path
        Ruta al archivo YAML de configuración.

    Returns
    -------
    AppConfig
        Configuración validada mediante Pydantic.

    Raises
    ------
    FileNotFoundError
        Si el archivo no existe.

    yaml.YAMLError
        Si el YAML está corrupto o mal formado.

    ValidationError
        Si el schema no cumple AppConfig.
    """

    log = get_run_logger()

    _validate_path(path)

    try:
        raw_data = _load_yaml_file(path)

    except yaml.YAMLError as exc:

        log.critical("YAML parsing failed | file=%s error=%s", path, exc)

        raise

    try:

        config = AppConfig.model_validate(raw_data)

    except ValidationError as exc:

        log.critical("Config validation failed | file=%s error=%s", path, exc)

        raise

    log.info("Config loaded and validated | file=%s", path)

    return config


# ==========================================================
# Internal Helpers
# ==========================================================

def _validate_path(path: Path) -> None:
    """
    Validate configuration file path.

    Raises
    ------
    FileNotFoundError
        Si el archivo no existe.
    """

    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    if not path.is_file():
        raise FileNotFoundError(f"Config path is not a file: {path}")


def _load_yaml_file(path: Path) -> dict[str, Any]:
    """
    Load YAML configuration safely.

    Returns
    -------
    dict
        Contenido del YAML.

    Raises
    ------
    yaml.YAMLError
        Si el YAML no es válido.
    """

    text = path.read_text(encoding="utf-8")

    if not text.strip():
        return {}

    data = yaml.safe_load(text)

    if data is None:
        return {}

    if not isinstance(data, dict):
        raise yaml.YAMLError(
            "Configuration root must be a YAML mapping (dict)"
        )

    return data