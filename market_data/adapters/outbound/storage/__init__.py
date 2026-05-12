# -*- coding: utf-8 -*-
"""
market_data/adapters/outbound/storage/
========================================

Adapters de storage outbound — implementaciones concretas de OHLCVStorage.

Contenido
---------
IcebergStorageFactory — implementación activa de StorageFactoryPort.
                        Crea y cachea instancias de IcebergStorage por exchange.

Importar desde ports/, no desde aquí (DIP):
    from market_data.ports.outbound.storage import OHLCVStorage
    from market_data.ports.outbound.storage_factory import StorageFactoryPort
"""
