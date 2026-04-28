"""
core/observability/__init__.py — SHIM DE MIGRACIÓN
====================================================
Re-exporta desde platform.observability.
TEMPORAL: eliminar cuando todos los imports apunten a platform.*
"""
from platform.observability import *  # noqa: F401, F403
