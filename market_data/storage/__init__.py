# -*- coding: utf-8 -*-
"""
market_data/storage/
=====================

Capa de storage del bounded context market_data.

Módulos
-------
  silver/trades_storage      — append-only tick data      (silver.trades)
  silver/derivatives_storage — snapshots de derivados     (silver.funding_rate,
                               silver.open_interest)

Nota sobre OHLCVStorage
-----------------------
El backend OHLCV activo (IcebergStorage) vive en:
  market_data/adapters/outbound/storage/iceberg/

El contrato se importa desde:
  market_data/ports/storage.py::OHLCVStorage

No se re-exporta aquí para respetar el límite de bounded context:
los consumidores dependen del puerto (DIP), no de la implementación.

Principios: SOLID · KISS · DRY · SafeOps
"""

from market_data.storage.silver.trades_storage import TradesStorage
from market_data.storage.silver.derivatives_storage import (
    DerivativesStorage,
    SUPPORTED_DATASETS,
)

__all__ = [
    "TradesStorage",
    "DerivativesStorage",
    "SUPPORTED_DATASETS",
]
