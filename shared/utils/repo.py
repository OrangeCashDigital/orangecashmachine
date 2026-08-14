"""
shared/utils/repo.py
====================

SSOT para localización de la raíz del repositorio y utilidades git.

Responsabilidad única (SRP):
    Proveer repo_root() y _get_git_hash() — únicas funciones, única razón de cambio.

Por qué en shared/utils/:
    - stdlib-only: solo usa pathlib.Path y subprocess
    - Universalmente necesaria: cualquier BC puede necesitar anclar rutas u obtener hash git
    - Dependency-free: cumple BC-01 (shared no importa internos)
    - Evita duplicación: antes existía en ocm/config/paths.py Y en
      apps/app/cli/main.py como _find_repo_root() — violación DRY/SSOT

Principios: SRP · SSOT · DRY · Fail-Fast
"""

from __future__ import annotations

import subprocess
from pathlib import Path


def repo_root() -> Path:
    """Raíz del repositorio localizando el directorio .git.

    Sube desde este archivo hasta encontrar un directorio que contenga
    .git. Fail-Fast explícito: nunca usa parents[N] hardcodeado.

    Returns:
        Path absoluto a la raíz del repositorio.

    Raises:
        RuntimeError: Si no se encuentra .git en ningún ancestro.
            Indica ejecución desde fuera del repositorio o instalación
            como paquete sin el .git presente.
    """
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / ".git").exists():
            return parent
    raise RuntimeError(f"No se encontró .git subiendo desde {here}. ¿Estás ejecutando desde fuera del repositorio?")


def _get_git_hash() -> str:
    """Retorna el git hash corto del HEAD. Fail-soft: retorna 'unknown'."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=2,
        )
        return result.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


__all__ = ["repo_root", "_get_git_hash"]
