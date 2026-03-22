from __future__ import annotations

"""core/config/loader/yaml_loader.py — Carga y merge recursivo de YAML."""

import hashlib
import logging
from pathlib import Path
from typing import Any

import yaml

from .exceptions import ConfigurationError

logger = logging.getLogger(__name__)


class YamlLoader:
    """
    Merge recursivo de dicts garantiza que campos anidados como
    markets.futures.defaultType definidos en base.yaml NO se pierdan
    si un override sólo sobreescribe parte del dict.
    """

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


def compute_hash(data: dict) -> str:
    return hashlib.sha256(yaml.dump(data, sort_keys=True).encode()).hexdigest()
