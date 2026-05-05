# -*- coding: utf-8 -*-
"""
market_data/ports/storage_factory.py
=====================================

Puerto estructural para la fábrica de backends OHLCVStorage.

Por qué existe este port
------------------------
OHLCVStorage es un contrato de instancia (una instancia por exchange/market_type).
El endpoint HTTP recibe exchange como parámetro de ruta — distintas requests
necesitan distintas instancias. Un puerto de instancia no resuelve esto; se
necesita un puerto de fábrica (Factory Method — GoF).

Sin StorageFactoryPort:
  - El handler conoce IcebergStorage (adaptador concreto)          → viola DIP
  - Se crean instancias por request sin cache                       → ineficiente
  - Imposible inyectar mock en tests                               → no testeable

Con StorageFactoryPort:
  - El handler pide get_storage(exchange) al port                   → DIP correcto
  - La implementación concreta decide cachear o no                  → SRP
  - Tests inyectan InMemoryStorageFactory                           → testeable

Implementación activa
---------------------
market_data.adapters.outbound.storage.iceberg_factory.IcebergStorageFactory

Principios: DIP · OCP · SRP · SSOT · Factory Method (GoF)
"""
from __future__ import annotations

from typing import Protocol, runtime_checkable

from market_data.ports.storage import OHLCVStorage


@runtime_checkable
class StorageFactoryPort(Protocol):
    """
    Contrato de fábrica de backends OHLCVStorage.

    Contrato semántico
    ------------------
    get_storage(exchange, market_type) retorna una instancia de OHLCVStorage
    lista para usar. La implementación decide la política de cache.

    SafeOps
    -------
    Nunca retorna None — lanza si no puede construir el backend.
    El caller no necesita verificar None.

    OCP
    ---
    Añadir un nuevo backend (e.g. ParquetStorage) no modifica este contrato.
    """

    def get_storage(
        self,
        exchange:    str,
        market_type: str  = "spot",
        dry_run:     bool = False,
    ) -> OHLCVStorage:
        """
        Retorna (o crea) una instancia de OHLCVStorage para el par dado.

        Parameters
        ----------
        exchange    : nombre canónico del exchange ("bybit", "kucoin", ...)
        market_type : tipo de mercado ("spot" | "futures" | ...)
        dry_run     : True = no persiste datos (tests / safety mode).
                      Las instancias dry_run y no-dry_run no comparten cache.

        Returns
        -------
        OHLCVStorage listo para usar.

        Raises
        ------
        RuntimeError si el backend no puede inicializarse.
        """
        ...
