from __future__ import annotations

"""
core/utils.py
=============

Núcleo mínimo de utilidades stdlib.

Contenido
---------
  repo_root()  — anchor del repositorio, sin dependencias, nunca parents[N]
"""

from pathlib import Path


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


__all__ = ["repo_root"]
