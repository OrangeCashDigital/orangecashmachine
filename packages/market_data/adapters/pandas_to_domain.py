# -*- coding: utf-8 -*-
"""
market_data/adapters/pandas_to_domain.py — RE-EXPORT BRIDGE
SSOT movido a adapters/inbound/pandas_to_domain.py
"""
from market_data.adapters.inbound.pandas_to_domain import (  # noqa: F401
    ohlcv_df_to_chunk,
    DataFrameMappingError,
)
__all__ = ["ohlcv_df_to_chunk", "DataFrameMappingError"]
