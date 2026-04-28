# -*- coding: utf-8 -*-
"""
market_data/storage/storage_protocol.py
========================================

SHIM DE COMPATIBILIDAD — no modificar, no agregar lógica aquí.

La definición canónica vive en:
    market_data.ports.storage.OHLCVStorage

Este módulo re-exporta para mantener compatibilidad hacia atrás
con los imports existentes mientras se migran progresivamente.

Migración
---------
Actualizar todos los imports a:
    from market_data.ports.storage import OHLCVStorage

Una vez completada la migración, este archivo puede eliminarse.
"""
from market_data.ports.storage import OHLCVStorage  # noqa: F401

__all__ = ["OHLCVStorage"]
