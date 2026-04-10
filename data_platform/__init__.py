from __future__ import annotations

"""
data_platform/__init__.py
=========================

Exports públicos del paquete data_platform.

Capas
-----
  protocols    — contratos estructurales (OHLCVStorageProtocol, FeatureStorageProtocol)
  platform     — facade DataPlatform (punto de entrada para research/backtesting)
  ohlcv_utils  — utilidades de dominio OHLCV (safe_symbol, normalize_ohlcv_df)
  loaders      — acceso directo al Gold layer (GoldLoader)

Uso recomendado
---------------
    from data_platform.platform import DataPlatform

    data = DataPlatform(exchange="bybit", market_type="spot")
    df   = data.ohlcv("BTC/USDT", "1h")
    df   = data.features("BTC/USDT", "1h")
"""

from data_platform.ohlcv_utils import safe_symbol, normalize_ohlcv_df
from data_platform.platform import DataPlatform

__all__ = [
    "DataPlatform",
    "safe_symbol",
    "normalize_ohlcv_df",
]
