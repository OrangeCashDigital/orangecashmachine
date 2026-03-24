"""
core/logging/formats.py
=======================
Formatos de log centralizados para todos los sinks de Loguru.
Cambia aquí, se propaga a consola, archivos y pipeline.

request_id
----------
FILE y CONSOLE usan {extra[request_id]}.
El default "-" se garantiza via logger.configure(patcher=...) en setup.py,
no en el string de formato — loguru lanza KeyError si la key no existe.

PIPELINE no incluye request_id: ese sink filtra por exchange+dataset
y logger.bind() en ese contexto no incluye request_id.
"""

CONSOLE: str = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level:<8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{line}</cyan> | "
    "<dim>{extra[request_id]}</dim> | "
    "{message}"
)

FILE: str = (
    "{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {name}:{line} | "
    "{extra[request_id]} | {message}"
)

PIPELINE: str = (
    "{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {name}:{line} | "
    "{extra[exchange]}/{extra[dataset]} | {message}"
)
