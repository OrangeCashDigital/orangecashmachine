from __future__ import annotations

"""
ocm_platform/utils.py
=====================

Utilidades de infraestructura base sin dependencias externas.

Contenido
---------
  repo_root() — anchor del repositorio basado en .git, Fail-Fast explícito.
                Nunca usa parents[N] hardcodeado — robusto ante reubicación
                de archivos.
"""

from pathlib import Path


def repo_root() -> Path:
    """Devuelve la raíz del repositorio localizando el directorio .git.

    Busca subiendo desde este archivo. Falla de forma explícita (Fail-Fast)
    si no encuentra .git — mejor que un parents[N] silenciosamente incorrecto.

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


__all__ = ["repo_root"]
