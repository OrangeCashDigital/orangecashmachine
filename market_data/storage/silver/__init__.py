# -*- coding: utf-8 -*-
"""
market_data/storage/silver/
============================

Capa Silver — datos limpios y normalizados listos para análisis.

Módulos
-------
trades_storage      : append-only tick data  (silver.trades)
derivatives_storage : snapshots de derivados (silver.funding_rate,
                      silver.open_interest)

Convención de nombres de tabla
-------------------------------
  silver.<dataset>  →  e.g. silver.trades, silver.funding_rate

Principios: SOLID · KISS · DRY · SafeOps
"""
from market_data.storage.silver.trades_storage import TradesStorage
from market_data.storage.silver.derivatives_storage import DerivativesStorage

__all__ = ["TradesStorage", "DerivativesStorage"]
