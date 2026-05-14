from __future__ import annotations

"""
ocm_platform/observability/formats.py
======================================

Formatos de log centralizados para todos los sinks de Loguru.

Un cambio aquí se propaga a consola y archivos de texto.

Nota sobre ``run_id``
---------------------
``FILE`` y ``CONSOLE`` usan ``{extra[run_id]}``. El valor por defecto ``"-"``
se garantiza vía ``logger.configure(patcher=_make_patcher(...))`` en
``logger.py`` — loguru lanza ``KeyError`` si la clave no existe en extra.

Sinks JSON (``app_*.log``)
--------------------------
Los sinks que usan ``serialize=True`` serializan el record completo de
loguru como JSON estructurado. No usan un format string — loguru lo ignora
cuando serialize=True. El formato JSON es canónico y no necesita
representación aquí.
"""

CONSOLE: str = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level:<8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{line}</cyan> | "
    "<dim>{extra[run_id]}</dim> | "
    "{message}"
)

FILE: str = (
    "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {name}:{line} | "
    "{extra[run_id]} | {message}"
)
