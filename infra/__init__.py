"""Shim de compatibilidad: re-exporta desde ocm_platform.infra.

TEMPORAL: eliminar cuando todos los callers apunten a ocm_platform.infra.*
"""
from ocm_platform.infra import *  # noqa: F401, F403
