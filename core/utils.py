"""Shim de compatibilidad: re-exporta desde ocm_platform.utils.

TEMPORAL: eliminar cuando todos los callers apunten a ocm_platform.utils.
"""
from ocm_platform.utils import repo_root  # noqa: F401

__all__ = ["repo_root"]
