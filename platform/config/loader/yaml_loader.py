from __future__ import annotations

"""
core/config/loader/yaml_loader.py
==================================

Carga y merge recursivo de archivos YAML de configuración.

El merge recursivo garantiza que campos anidados definidos en ``base.yaml``
no se pierdan cuando un override solo sobreescribe parte de un dict.
"""

import hashlib
from pathlib import Path
from typing import Any

import yaml
from loguru import logger

from .exceptions import ConfigurationError


class YamlLoader:
    """Carga y fusiona archivos YAML con merge recursivo de dicts."""

    @staticmethod
    def load(path: Path, *, required: bool = True) -> dict[str, Any]:
        """Carga un archivo YAML y lo devuelve como dict.

        Args:
            path: Ruta al archivo YAML.
            required: Si True (default), lanza :exc:`ConfigurationError`
                cuando el archivo no existe.

        Returns:
            Dict con el contenido del archivo, o ``{}`` si no existe
            y ``required=False``.

        Raises:
            ConfigurationError: Si el archivo no existe (con ``required=True``),
                contiene YAML inválido, o la raíz no es un mapping.
        """
        if not path.exists():
            if required:
                raise ConfigurationError(f"Missing config file: {path}")
            logger.debug("yaml_load_skipped | file={} required=false", path)
            return {}

        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError as exc:
            raise ConfigurationError(f"Invalid YAML in {path}: {exc}") from exc

        if not isinstance(data, dict):
            raise ConfigurationError(f"Root element must be a mapping in: {path}")
        return data

    @classmethod
    def merge(cls, base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
        """Fusiona dos dicts de forma recursiva (deep merge).

        Los dicts anidados se fusionan recursivamente en lugar de reemplazarse.

        Args:
            base: Dict de configuración base.
            override: Dict con valores de mayor prioridad.

        Returns:
            Nuevo dict fusionado. Los originales no se modifican.
        """
        result = dict(base)
        for k, v in override.items():
            if isinstance(result.get(k), dict) and isinstance(v, dict):
                result[k] = cls.merge(result[k], v)
            else:
                result[k] = v
        return result


def compute_hash(data: dict[str, Any]) -> str:
    """Calcula un hash SHA-256 determinista del dict de configuración.

    Args:
        data: Dict de configuración a hashear.

    Returns:
        Hash SHA-256 en hexadecimal (64 chars).
    """
    return hashlib.sha256(
        yaml.dump(data, sort_keys=True).encode()
    ).hexdigest()
