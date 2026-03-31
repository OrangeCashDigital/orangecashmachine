from __future__ import annotations

"""
core/config/paths.py
====================

Single Source of Truth para los paths del Data Lakehouse.

Orden de resolución para data_lake_root()
------------------------------------------
1. OCM_DATA_LAKE_PATH (env var) — override absoluto, máxima prioridad
2. storage.data_lake.path del YAML mergeado — configurable por entorno
   • base.yaml   → data_platform/data_lake
   • settings.yaml → data_platform/local_data_lake  (en development)
3. repo_root() / "data_platform" / "data_lake" — fallback seguro

Paths derivados
---------------
  bronze_ohlcv_root() → <lake_root>/bronze/ohlcv/
  silver_ohlcv_root() → <lake_root>/silver/ohlcv/
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
from pathlib import Path
from typing import Optional

from core.config.env_vars import OCM_DATA_LAKE_PATH, OCM_GOLD_PATH
from core.utils import repo_root


def data_lake_root() -> Path:
    """
    Raíz del Data Lake. Configurable por entorno, sin hardcode.

    Resolución (en orden de prioridad):
    1. OCM_DATA_LAKE_PATH — override absoluto via env var
    2. storage.data_lake.path del YAML mergeado
    3. repo_root() / data_platform / data_lake — fallback seguro
    """
    # Prioridad 1: override explícito via env var
    env_override = os.getenv(OCM_DATA_LAKE_PATH)
    if env_override:
        return Path(env_override).resolve()

    # Prioridad 2: config YAML mergeada
    yaml_path = _read_yaml_lake_path()
    if yaml_path:
        p = Path(yaml_path)
        # Path relativo → relativo al repo root
        if not p.is_absolute():
            p = repo_root() / p
        return p.resolve()

    # Prioridad 3: fallback estructural
    return repo_root() / "data_platform" / "data_lake"


def bronze_ohlcv_root() -> Path:
    """Path absoluto a bronze/ohlcv/."""
    return data_lake_root() / "bronze" / "ohlcv"


def silver_ohlcv_root() -> Path:
    """Path absoluto a silver/ohlcv/."""
    return data_lake_root() / "silver" / "ohlcv"


def gold_features_root() -> Path:
    """
    Path absoluto a gold/features/ohlcv/.

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
    """
    Resuelve variables de entorno con sintaxis bash ${VAR:-default}.

    Ejemplos:
        ${LOCAL_DATA_LAKE_PATH:-data_platform/local_data_lake}
        → valor de LOCAL_DATA_LAKE_PATH, o 'data_platform/local_data_lake' si no está seteada

        ${DATA_LAKE_PATH}
        → valor de DATA_LAKE_PATH, o string vacío si no está seteada
    """
    import re
    def _sub(m: re.Match) -> str:
        var    = m.group(1)
        default = m.group(2) or ""
        return os.environ.get(var, default)
    return re.sub(r'\$\{([^}:]+)(?::-([^}]*))?\}', _sub, value)


def _read_yaml_lake_path() -> Optional[str]:
    """
    Lee storage.data_lake.path del YAML mergeado (base → env → settings).

    Import lazy para evitar ciclo: paths.py → yaml_loader → paths.py.
    Falla silenciosamente — si el YAML no es accesible se usa el fallback.
    """
    try:
        from core.config.loader.yaml_loader import YamlLoader
        from core.config.loader.env_resolver import resolve_env

        config_dir = _find_config_dir()
        if config_dir is None:
            return None

        env = resolve_env()

        data: dict = {}
        for fname in ("base.yaml", f"{env}.yaml", "settings.yaml"):
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
    except Exception:
        return None


def _find_config_dir() -> Optional[Path]:
    """
    Localiza el directorio de config YAML.

    Busca en este orden:
    1. OCM_CONFIG_PATH / OCM_CONFIG_DIR (env vars)
    2. repo_root() / config
    """
    from core.config.env_vars import OCM_CONFIG_PATH, OCM_CONFIG_DIR

    raw = os.getenv(OCM_CONFIG_PATH) or os.getenv(OCM_CONFIG_DIR)
    if raw:
        p = Path(raw)
        return p if p.is_dir() else None

    candidate = repo_root() / "config"
    return candidate if candidate.is_dir() else None


__all__ = [
    "data_lake_root",
    "bronze_ohlcv_root",
    "silver_ohlcv_root",
    "gold_features_root",
]
