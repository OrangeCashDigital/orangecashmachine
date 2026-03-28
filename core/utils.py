from __future__ import annotations

"""
core/utils.py
=============
Utilidades compartidas del proyecto OrangeCashMachine.

Reglas
------
- Solo stdlib — sin dependencias internas ni de terceros
- Importar desde aquí en lugar de redefinir en cada módulo
- Todo lo que se use en 2+ módulos vive aquí
"""

import subprocess
from pathlib import Path


# ==========================================================
# Repo root — anchor confiable, nunca parents[N] hardcodeado
# ==========================================================

def repo_root() -> Path:
    """
    Devuelve la raíz del repositorio de forma robusta.

    Busca el directorio .git subiendo desde este archivo.
    Falla explícitamente si no se encuentra — mejor que un
    parents[3] silenciosamente incorrecto al mover archivos.
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
# Rutas canónicas del Data Lakehouse
# ==========================================================

# Componentes relativos — se combinan con repo_root() en runtime
LAKE_ROOT:         tuple[str, ...] = ("data_platform", "data_lake")
SILVER_OHLCV_PATH: tuple[str, ...] = LAKE_ROOT + ("silver", "ohlcv")
GOLD_FEATURES_PATH: tuple[str, ...] = LAKE_ROOT + ("gold", "features", "ohlcv")


def silver_ohlcv_root() -> Path:
    """Path absoluto a silver/ohlcv/. Falla explícitamente si no existe."""
    return repo_root().joinpath(*SILVER_OHLCV_PATH)


def gold_features_root() -> Path:
    """Path absoluto a gold/features/ohlcv/. No requiere que exista."""
    return repo_root().joinpath(*GOLD_FEATURES_PATH)



# ==========================================================
# Git
# ==========================================================

def get_git_hash() -> str:
    """Hash corto del commit actual para trazabilidad de lineage."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=2,
        )
        return result.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


__all__ = [
    "repo_root",
    "LAKE_ROOT",
    "SILVER_OHLCV_PATH",
    "GOLD_FEATURES_PATH",
    "silver_ohlcv_root",
    "gold_features_root",
    "get_git_hash",
]
