# -*- coding: utf-8 -*-
"""
data_platform/loaders/gold_loader.py
======================================

SHIM DE COMPATIBILIDAD — no contiene lógica propia.

La implementación real fue migrada a:
    market_data.adapters.outbound.storage.gold_reader.GoldReader

Este módulo existe únicamente para no romper imports externos durante
la transición. Será eliminado en el siguiente ciclo de limpieza.

Imports activos que aún usan GoldLoader:
    research/data/data_access.py  ← migrar a DataPlatform.features()

Principios: DRY · SSOT (implementación en market_data) · transición limpia
"""
from __future__ import annotations

from market_data.adapters.outbound.storage.gold_reader import GoldReader as GoldLoader

# Aliases semánticos para compatibilidad con imports existentes
from data_platform.ohlcv_utils import DataNotFoundError as GoldLoaderError
from data_platform.ohlcv_utils import VersionNotFoundError as GoldVersionNotFound

__all__ = ["GoldLoader", "GoldLoaderError", "GoldVersionNotFound"]
