from __future__ import annotations

"""
core/utils.py
=============

Núcleo mínimo de utilidades stdlib.

Contenido
---------
  repo_root()  — anchor del repositorio, sin dependencias, nunca parents[N]

Funciones deprecadas (wrappers de compatibilidad)
-------------------------------------------------
Las funciones de path y lineage han migrado a sus módulos canónicos:

  get_git_hash()      → core.config.lineage.get_git_hash
  silver_ohlcv_root() → core.config.paths.silver_ohlcv_root
  gold_features_root() → core.config.paths.gold_features_root

Los wrappers se mantienen para no romper código externo, pero emiten
DeprecationWarning. Se eliminarán en una versión futura.
"""

import subprocess
import warnings
from pathlib import Path


# ==========================================================
# Repo root — stdlib pura, anchor de toda la arquitectura de paths
# ==========================================================

def repo_root() -> Path:
    """
    Devuelve la raíz del repositorio de forma robusta.

    Busca el directorio .git subiendo desde este archivo.
    Falla explícitamente si no se encuentra — mejor que un
    parents[N] silenciosamente incorrecto al mover archivos.
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
# Wrappers de compatibilidad — DEPRECADOS
# Migrar a core.config.lineage / core.config.paths
# ==========================================================

def get_git_hash() -> str:
    """
    DEPRECADO. Usar: from core.config.lineage import get_git_hash
    """
    warnings.warn(
        "core.utils.get_git_hash está deprecado. "
        "Usa: from core.config.lineage import get_git_hash",
        DeprecationWarning,
        stacklevel=2,
    )
    from core.config.lineage import get_git_hash as _fn
    return _fn()


def silver_ohlcv_root() -> Path:
    """
    DEPRECADO. Usar: from core.config.paths import silver_ohlcv_root
    """
    warnings.warn(
        "core.utils.silver_ohlcv_root está deprecado. "
        "Usa: from core.config.paths import silver_ohlcv_root",
        DeprecationWarning,
        stacklevel=2,
    )
    from core.config.paths import silver_ohlcv_root as _fn
    return _fn()


def gold_features_root() -> Path:
    """
    DEPRECADO. Usar: from core.config.paths import gold_features_root
    """
    warnings.warn(
        "core.utils.gold_features_root está deprecado. "
        "Usa: from core.config.paths import gold_features_root",
        DeprecationWarning,
        stacklevel=2,
    )
    from core.config.paths import gold_features_root as _fn
    return _fn()


__all__ = [
    "repo_root",
    # Deprecados — mantener para compatibilidad
    "get_git_hash",
    "silver_ohlcv_root",
    "gold_features_root",
]
