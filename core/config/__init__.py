from __future__ import annotations

"""
core/config/__init__.py
=======================

Exports públicos del paquete core.config.

Uso recomendado
---------------
    from core.config.schema import AppConfig
    from core.config.paths import data_lake_root
    from core.config.env_vars import OCM_DATA_LAKE_PATH

Nota sobre RunConfig
--------------------
RunConfig vive en core/runtime/run_config.py (ubicación canónica).
No se re-exporta aquí en nivel de módulo para evitar import circular:
  core/config/__init__ → core/runtime/run_config → core/config/env_vars
  → core/config/__init__ (paquete aún no inicializado) → CIRCULAR

Los callers que necesiten RunConfig deben importar desde:
    from core.runtime import RunConfig
    from core.runtime.run_config import RunConfig
    from core.config.runtime import RunConfig   ← shim de compatibilidad
"""

from core.config.credentials import resolve_exchange_credentials

__all__ = [
    "resolve_exchange_credentials",
]
