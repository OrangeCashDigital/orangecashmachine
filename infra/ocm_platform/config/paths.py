from __future__ import annotations

"""
ocm_platform/config/paths.py
====================

Single Source of Truth para los paths del Data Lakehouse.

Orden de resolución para data_lake_root()
------------------------------------------
1. OCM_STORAGE__DATA_LAKE__PATH (env var) — canónica, L2-aligned, máxima prioridad
2. OCM_DATA_LAKE_PATH (env var)           — DEPRECATED: legacy, emite DeprecationWarning
3. storage.data_lake.path del YAML mergeado — configurable por entorno
   • base.yaml            → data_platform/data_lake
   • env/{env}.yaml       → override por entorno
   • storage/datalake.yaml → SSOT del path anchor (Hydra compose)
4. repo_root() / "data_platform" / "data_lake" — fallback estructural seguro

Uso
---
  data_lake_root() — anchor para environment_validator (_check_storage_path)
                     y para catalog.py (iceberg_warehouse / iceberg_catalog).

Diseño
------
• Configurable por entorno sin cambiar código
• Compatible con Docker (montar volumen + OCM_DATA_LAKE_PATH)
• Las funciones son lazy — no leen disco en import time
"""

import os
import re
from pathlib import Path
from typing import Optional

from ocm_platform.config.env_vars import (
    OCM_CONFIG_DIR,
    OCM_CONFIG_PATH,
    OCM_STORAGE__DATA_LAKE__PATH,
    OCM_DATA_LAKE_PATH,          # DEPRECATED — legacy, transicion controlada
    OCM_GOLD_PATH,
)
def repo_root() -> Path:
    """Devuelve la raíz del repositorio localizando el directorio .git.

    Fail-Fast explícito: nunca usa parents[N] hardcodeado.
    Robusto ante reubicación de archivos.

    Returns:
        Path absoluto a la raíz del repositorio.

    Raises:
        RuntimeError: Si no se encuentra .git en ningún ancestro.
    """
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / ".git").exists():
            return parent
    raise RuntimeError(
        f"No se encontró .git subiendo desde {here}. "
        "¿Estás ejecutando desde fuera del repositorio?"
    )


# ==========================================================
# API pública
# ==========================================================

def data_lake_root() -> Path:
    """Raíz del Data Lake. Configurable por entorno, sin hardcode.

    Resolución (en orden de prioridad):
    1. OCM_STORAGE__DATA_LAKE__PATH — variable canónica (L2-aligned)
    2. OCM_DATA_LAKE_PATH           — DEPRECATED: legacy, emite warning
    3. storage.data_lake.path del YAML mergeado (base → env → datalake)
    4. repo_root() / data_platform / data_lake — fallback estructural seguro

    Contrato de transición:
        Si OCM_DATA_LAKE_PATH está presente y OCM_STORAGE__DATA_LAKE__PATH
        no lo está, se usa el valor legacy con DeprecationWarning visible.
        Si ambas están presentes, OCM_STORAGE__DATA_LAKE__PATH tiene prioridad
        y se emite warning de variable redundante.
    """
    import warnings

    canonical = os.getenv(OCM_STORAGE__DATA_LAKE__PATH)
    legacy    = os.getenv(OCM_DATA_LAKE_PATH)

    if canonical and legacy and canonical != legacy:
        warnings.warn(
            "OCM_DATA_LAKE_PATH y OCM_STORAGE__DATA_LAKE__PATH están ambas definidas "
            "con valores distintos. OCM_STORAGE__DATA_LAKE__PATH tiene prioridad. "
            "Eliminar OCM_DATA_LAKE_PATH del entorno.",
            UserWarning,
            stacklevel=2,
        )

    if canonical:
        return Path(canonical).resolve()

    if legacy:
        warnings.warn(
            "OCM_DATA_LAKE_PATH está deprecada. "
            "Usar OCM_STORAGE__DATA_LAKE__PATH en su lugar. "
            "Será eliminada en el próximo release.",
            DeprecationWarning,
            stacklevel=2,
        )
        return Path(legacy).resolve()

    yaml_path = _read_yaml_lake_path()
    if yaml_path:
        p = Path(yaml_path)
        if not p.is_absolute():
            p = repo_root() / p
        return p.resolve()

    return repo_root() / "data_platform" / "data_lake"


def bronze_ohlcv_root() -> Path:
    """Raiz de la capa Bronze para OHLCV.
    Estructura: <data_lake_root>/bronze/ohlcv
    """
    return data_lake_root() / "bronze" / "ohlcv"


def silver_ohlcv_root() -> Path:
    """Raiz de la capa Silver para OHLCV.
    Estructura: <data_lake_root>/silver/ohlcv
    """
    return data_lake_root() / "silver" / "ohlcv"


def gold_features_root() -> Path:
    """Raiz de la capa Gold para features OHLCV.
    Resolucion:
    1. OCM_GOLD_FEATURES_PATH -- override absoluto via env var
    2. data_lake_root() / gold / features / ohlcv -- fallback derivado
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
        from ocm_platform.config.loader.yaml_loader import YamlLoader
        from ocm_platform.config.loader.env_resolver import resolve_env

        config_dir = _find_config_dir()
        if config_dir is None:
            return None

        env = resolve_env()

        # Orden de merge: base → env/{env} → storage/datalake (SSOT Hydra)
        # storage/datalake.yaml tiene @package _global_ — su storage.data_lake.path
        # es el mismo valor que Hydra resuelve en AppConfig.storage.data_lake.path.
        # settings.yaml NO se incluye — es solo para environment.default_env.
        data: dict = {}
        for fname in ("base.yaml", f"env/{env}.yaml", "storage/datalake.yaml"):
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
        from loguru import logger
        logger.debug(
            "paths._read_yaml_lake_path | fallback_to_structural exc_type={} exc={}",
            type(exc).__name__, exc,
        )
        return None


__all__ = [
    "data_lake_root",
    "bronze_ohlcv_root",
    "silver_ohlcv_root",
    "gold_features_root",
    "repo_root",
]
