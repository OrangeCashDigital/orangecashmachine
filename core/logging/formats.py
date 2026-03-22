"""
core/logging/formats.py
=======================
Formatos de log centralizados para todos los sinks de Loguru.
Cambia aquí, se propaga a consola, archivos y pipeline.
"""

CONSOLE: str = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level:<8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{line}</cyan> | "
    "{message}"
)

FILE: str = (
    "{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {name}:{line} | {message}"
)

PIPELINE: str = (
    "{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {name}:{line} | "
    "{extra[exchange]}/{extra[dataset]} | {message}"
)
