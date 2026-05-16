from __future__ import annotations

"""
ocm/config/loader/yaml_loader.py
==================================

Carga, merge recursivo y hashing de archivos YAML de configuración.

Funciones públicas
------------------
- ``load(path, *, required)``  — carga un archivo YAML → dict
- ``merge(base, override)``    — deep merge recursivo de dos dicts
- ``compute_hash(data)``       — hash SHA-256 determinista del config mergeado

Diseño
------
``YamlLoader`` era una clase con solo métodos estáticos/de-clase sin estado
(anti-patrón: fuerza instanciación innecesaria, viola KISS).
Reemplazado por funciones libres con ``__all__`` explícito.

``compute_hash`` usaba ``yaml.dump`` como serializador — no determinista
para tipos no-string en todas las versiones de PyYAML.
Reemplazado por ``json.dumps(sort_keys=True, default=str)`` (determinista,
independiente de versión de PyYAML).

``merge`` ahora hace deep copy de listas para evitar referencias compartidas
entre base y resultado (SafeOps — mutación accidental imposible).

Principios: KISS · DRY · SSOT · SafeOps · Fail-Fast.
"""

import hashlib
import json
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml
from loguru import logger

from .exceptions import ConfigFileNotFoundError, ConfigParseError

__all__ = ["load", "merge", "compute_hash"]


def load(path: Path, *, required: bool = True) -> dict[str, Any]:
    """Carga un archivo YAML y lo devuelve como dict.

    Args:
        path: Ruta al archivo YAML.
        required: Si ``True`` (default), lanza :exc:`ConfigFileNotFoundError`
            cuando el archivo no existe.

    Returns:
        Dict con el contenido del archivo, o ``{}`` si no existe
        y ``required=False``.

    Raises:
        ConfigFileNotFoundError: Si el archivo no existe con ``required=True``.
        ConfigParseError: Si el YAML es inválido o la raíz no es un mapping.
    """
    if not path.exists():
        if required:
            raise ConfigFileNotFoundError(f"Missing config file: {path}")
        logger.debug("yaml_load_skipped | file={} required=false", path)
        return {}

    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigParseError(f"Invalid YAML in {path}: {exc}") from exc

    if not isinstance(data, dict):
        raise ConfigParseError(f"Root element must be a mapping in: {path}")
    return data


def merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Fusiona dos dicts de forma recursiva (deep merge).

    Los dicts anidados se fusionan recursivamente en lugar de reemplazarse.
    Las listas y otros valores se copian en profundidad para evitar
    referencias compartidas entre ``base`` y el resultado (SafeOps).

    Args:
        base: Dict de configuración base.
        override: Dict con valores de mayor prioridad.

    Returns:
        Nuevo dict fusionado. Los originales no se modifican.
    """
    result = deepcopy(base)
    for k, v in override.items():
        if isinstance(result.get(k), dict) and isinstance(v, dict):
            result[k] = merge(result[k], v)
        else:
            result[k] = deepcopy(v)
    return result


def compute_hash(data: dict[str, Any]) -> str:
    """Calcula un hash SHA-256 determinista del dict de configuración.

    Usa ``json.dumps(sort_keys=True, default=str)`` como serializador —
    determinista independientemente de la versión de PyYAML y para todos
    los tipos de valor (floats, datetimes, Decimals, enums).

    Args:
        data: Dict de configuración a hashear.

    Returns:
        Hash SHA-256 en hexadecimal (64 chars).
    """
    serialized = json.dumps(data, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(serialized).hexdigest()


# ---------------------------------------------------------------------------
# Backward-compat shim: YamlLoader delegaba aquí — eliminado en este audit.
# Importadores directos de YamlLoader.load / YamlLoader.merge deben migrar
# a las funciones libres ``load`` / ``merge`` de este módulo.
# ---------------------------------------------------------------------------
class YamlLoader:
    """Shim de compatibilidad — usar las funciones libres directamente.

    .. deprecated::
        Clase eliminada (KISS — sin estado, solo métodos estáticos).
        Usar ``from ocm.config.loader.yaml_loader import load, merge``.
    """

    load = staticmethod(load)
    merge = staticmethod(merge)
