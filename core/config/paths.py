from __future__ import annotations

"""
core/config/paths.py
====================

Single Source of Truth para los paths del Data Lakehouse.

Orden de resolución para data_lake_root()
------------------------------------------
1. OCM_DATA_LAKE_PATH (env var) — override absoluto, máxima prioridad
2. storage.data_lake.path del YAML mergeado — configurable por entorno
   • base.yaml     → data_platform/data_lake
   • settings.yaml → data_platform/local_data_lake  (en development)
3. repo_root() / "data_platform" / "data_lake" — fallback seguro

Paths derivados
---------------
  bronze_ohlcv_root()  → <lake_root>/bronze/ohlcv/
  gold_features_root() → <lake_root>/gold/features/ohlcv/

Diseño
------
• Ningún path tiene parents[N] — siempre relativo a un anchor explícito
• Configurable por entorno sin cambiar código
• Compatible con Docker (montar volumen + OCM_DATA_LAKE_PATH)
• Las funciones son lazy — no leen disco en import time
• core/utils.py delega aquí para mantener compatibilidad de imports
"""

import os
import re
from pathlib import Path
from typing import Optional

from core.config.env_vars import (
    OCM_CONFIG_DIR,
    OCM_CONFIG_PATH,
    OCM_DATA_LAKE_PATH,
    OCM_GOLD_PATH,
)
from core.utils import repo_root


# ==========================================================
# API pública
# ==========================================================

def data_lake_root() -> Path:
    """Raíz del Data Lake. Configurable por entorno, sin hardcode.

    Resolución (en orden de prioridad):
    1. OCM_DATA_LAKE_PATH — override absoluto via env var
    2. storage.data_lake.path del YAML mergeado
    3. repo_root() / data_platform / data_lake — fallback seguro
    """
    env_override = os.getenv(OCM_DATA_LAKE_PATH)
    if env_override:
        return Path(env_override).resolve()

    yaml_path = _read_yaml_lake_path()
    if yaml_path:
        p = Path(yaml_path)
        if not p.is_absolute():
            p = repo_root() / p
        return p.resolve()

    return repo_root() / "data_platform" / "data_lake"


def bronze_ohlcv_root() -> Path:
    """Path absoluto a bronze/ohlcv/."""
    return data_lake_root() / "bronze" / "ohlcv"


def gold_features_root() -> Path:
    """Path absoluto a gold/features/ohlcv/.

    Resolución:
    1. OCM_GOLD_PATH — override absoluto
    2. Derivado de data_lake_root()
    """
    env_override = os.getenv(OCM_GOLD_PATH)
    if env_override:
        return Path(env_override).resolve()
    return data_lake_root() / "gold" / "features" / "ohlcv"


# ==========================================================
# Helpers internos
# ==========================================================

def _expand_env(value: str) -> str:
    """Resuelve variables de entorno con sintaxis bash y OmegaConf.

    Soporta:
        ${VAR:-default}           — bash
        ${oc.env:VAR,default}     — OmegaConf / Hydra
        ${oc.env:VAR}             — OmegaConf sin default
    """
    def _sub_omegaconf(m: re.Match) -> str:
        var     = m.group(1)
        default = m.group(2) if m.group(2) is not None else ""
        return os.environ.get(var, default)

    value = re.sub(r'\$\{oc\.env:([^},]+)(?:,([^}]*))?\}', _sub_omegaconf, value)

    def _sub_bash(m: re.Match) -> str:
        var     = m.group(1)
        default = m.group(2) or ""
        return os.environ.get(var, default)

    return re.sub(r'\$\{([^}:]+)(?::-([^}]*))?\}', _sub_bash, value)


def _find_config_dir() -> Optional[Path]:
    """Localiza el directorio de config YAML.

    Busca en este orden:
    1. OCM_CONFIG_PATH / OCM_CONFIG_DIR (env vars)
    2. repo_root() / config

    NOTA: No delega a RunConfig.from_env() intencionalmente.
    paths.py se importa antes de que RunConfig exista (bootstrap, import
    time en scripts standalone). La duplicación está justificada por el
    orden de inicialización — no "limpiar" sin entender este invariante.
    """
    raw = os.getenv(OCM_CONFIG_PATH) or os.getenv(OCM_CONFIG_DIR)
    if raw:
        p = Path(raw)
        return p if p.is_dir() else None

    candidate = repo_root() / "config"
    return candidate if candidate.is_dir() else None


def _read_yaml_lake_path() -> Optional[str]:
    """Lee storage.data_lake.path del YAML mergeado (base → env → settings).

    Import lazy para evitar ciclo: paths.py → yaml_loader → paths.py.
    Falla silenciosamente — si el YAML no es accesible se usa el fallback
    estructural. Loguea en DEBUG para que el silencio no oculte errores
    de configuración durante desarrollo.
    """
    try:
        from core.config.loader.yaml_loader import YamlLoader
        from core.config.loader.env_resolver import resolve_env

        config_dir = _find_config_dir()
        if config_dir is None:
            return None

        env = resolve_env()

        data: dict = {}
        for fname in ("base.yaml", f"env/{env}.yaml", "settings.yaml"):
            fpath = config_dir / fname
            chunk = YamlLoader.load(fpath, required=False)
            if chunk:
                data = YamlLoader.merge(data, chunk)

        raw = (
            data.get("storage", {})
                .get("data_lake", {})
                .get("path")
        )
        return _expand_env(raw) if raw else None

    except Exception as exc:
        try:
            from loguru import logger
            logger.debug("paths._read_yaml_lake_path fallback | reason={}", exc)
        except Exception:
            pass
        return None


__all__ = [
    "data_lake_root",
    "bronze_ohlcv_root",
    "gold_features_root",
]
