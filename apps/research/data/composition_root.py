# -*- coding: utf-8 -*-
"""
research/data/composition_root.py
==================================

Composition Root del bounded context research (F-1).

Responsabilidad única
---------------------
Único punto dentro de research que conoce las implementaciones concretas de
los ports de market_data que research consume:

    research.data.data_access
            ↓  (depende solo de contracts)
        StorageFactoryPort / FeatureReaderPort
            ↓  (este módulo elige la implementación)
        IcebergStorageFactory / GoldReader

data_access importa los ports y este composition root — nunca los adapters.

Contratos enforced
------------------
F-1 (import-linter): "research imports market_data.adapters/infrastructure
only from research/data/composition_root" — ver architecture/importlinter.toml.

Regla de uso
------------
Los consumers de research.data NO importan IcebergStorageFactory/GoldReader.
Para obtener una implementación, llaman build_storage_factory() /
build_feature_reader() desde aquí (o reciben el port inyectado).

Principios: DIP · SRP · KISS · composition root (Seemann, «DI in .NET»)
"""

from __future__ import annotations

from typing import Optional, cast

from market_data.adapters.outbound.storage.gold_reader import GoldReader
from market_data.adapters.outbound.storage.iceberg_factory import IcebergStorageFactory
from market_data.ports.outbound.feature_reader import FeatureReaderPort
from market_data.ports.outbound.storage_factory import StorageFactoryPort

__all__ = ["build_storage_factory", "build_feature_reader"]


def build_storage_factory() -> StorageFactoryPort:
    """
    Implementación concreta de StorageFactoryPort usada por research.

    Returns
    -------
    StorageFactoryPort : fábrica cacheada de OHLCVStorage por
        (exchange, market_type) — hoy IcebergStorageFactory.
    """
    return IcebergStorageFactory()


def build_feature_reader(exchange: Optional[str] = None) -> FeatureReaderPort:
    """
    Implementación concreta de FeatureReaderPort usada por research.

    Parameters
    ----------
    exchange : exchange canónico ("bybit", "kucoin", ...) o None para "any".

    Returns
    -------
    FeatureReaderPort : lector de features Gold (hoy GoldReader).

    GoldReader devuelve pl.DataFrame nativo — coincide con el port.
    """
    return cast(FeatureReaderPort, GoldReader(exchange=exchange))
