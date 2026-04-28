"""
core/config/__init__.py — SHIM DE MIGRACIÓN
=============================================
Re-exporta desde platform.config para mantener compatibilidad
mientras se actualizan los imports reales módulo a módulo.

TEMPORAL: eliminar cuando todos los imports apunten a platform.*
"""
from platform.config import *  # noqa: F401, F403
