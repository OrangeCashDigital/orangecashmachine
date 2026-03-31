from __future__ import annotations

"""
data_platform/__init__.py
=========================

Exports públicos del paquete data_platform.

Capas
-----
  ohlcv_utils  — utilidades de dominio OHLCV (safe_symbol, normalize_ohlcv_df)
  loaders      — acceso al Data Lake (MarketDataLoader, GoldLoader)

Uso
---
    from data_platform.ohlcv_utils import safe_symbol, normalize_ohlcv_df
    from data_platform.loaders.market_data_loader import MarketDataLoader
    from data_platform.loaders.gold_loader import GoldLoader
"""

# Exports de ohlcv_utils son los más usados — re-exportar para conveniencia
from data_platform.ohlcv_utils import safe_symbol, normalize_ohlcv_df

__all__ = [
    "safe_symbol",
    "normalize_ohlcv_df",
]
