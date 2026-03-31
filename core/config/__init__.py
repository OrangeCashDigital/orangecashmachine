from __future__ import annotations

"""
core/config/__init__.py
=======================

Exports públicos del paquete core.config.

Uso recomendado
---------------
    from core.config import RunConfig
    from core.config.paths import data_lake_root, silver_ohlcv_root
    from core.config.env_vars import OCM_DATA_LAKE_PATH
"""

from core.config.runtime import RunConfig
from core.config.credentials import resolve_exchange_credentials

__all__ = [
    "RunConfig",
    "resolve_exchange_credentials",
]
