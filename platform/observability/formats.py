from __future__ import annotations

"""
core/observability/formats.py
=======================

Formatos de log centralizados para todos los sinks de Loguru.

Un cambio aquí se propaga a consola, archivos y pipeline.

Nota sobre ``run_id``
---------------------
``FILE`` y ``CONSOLE`` usan ``{extra[run_id]}``. El valor por defecto ``"-"``
se garantiza vía ``logger.configure(patcher=...)`` en ``setup.py``, no en el
string de formato — loguru lanza ``KeyError`` si la clave no existe.

``PIPELINE`` no incluye ``run_id``: ese sink filtra por ``exchange``/``dataset``
y el contexto de ``logger.bind()`` en ese flujo no propaga ``run_id``.
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

PIPELINE: str = (
    "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {name}:{line} | "
    "{extra[exchange]}/{extra[dataset]} | {message}"
)
