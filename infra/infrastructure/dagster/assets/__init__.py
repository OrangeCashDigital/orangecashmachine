# -*- coding: utf-8 -*-
"""
infrastructure/dagster/assets/
================================

Assets Dagster de OCM — uno por bounded context y operación.

Assets activos
--------------
  bronze_ohlcv    — ingesta OHLCV exchange → Bronze → Silver
  repair_ohlcv    — reparación de gaps en Silver
  resample_ohlcv  — resampleo Silver 1m → timeframes superiores
  asset_checks    — checks de calidad sobre assets materializados

Exports (SSOT para Definitions)
--------------------------------
  BRONZE_OHLCV_ASSETS
  REPAIR_OHLCV_ASSETS
  RESAMPLE_OHLCV_ASSETS
  ALL_ASSET_CHECKS
"""
from infrastructure.dagster.assets.bronze_ohlcv   import BRONZE_OHLCV_ASSETS
from infrastructure.dagster.assets.repair_ohlcv   import REPAIR_OHLCV_ASSETS
from infrastructure.dagster.assets.resample_ohlcv import RESAMPLE_OHLCV_ASSETS
from infrastructure.dagster.assets.asset_checks   import ALL_ASSET_CHECKS

__all__ = [
    "BRONZE_OHLCV_ASSETS",
    "REPAIR_OHLCV_ASSETS",
    "RESAMPLE_OHLCV_ASSETS",
    "ALL_ASSET_CHECKS",
]
