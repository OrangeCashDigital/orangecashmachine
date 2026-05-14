# -*- coding: utf-8 -*-
"""
data_platform/ohlcv_utils.py
==============================

SHIM DE COMPATIBILIDAD — re-exporta desde market_data.domain.exceptions
y provee utilidades OHLCV usadas por research/ y tests/.

SSOT: las excepciones viven en market_data.domain.exceptions.
      Este módulo existe para no romper imports externos (OCP).

Principios: DRY · SSOT · transición limpia
"""
from __future__ import annotations

import re
import pandas as pd

from market_data.domain.exceptions import (  # noqa: F401
    DataNotFoundError,
    DataReadError,
    VersionNotFoundError,
)


def safe_symbol(symbol: str) -> str:
    """
    Normaliza un símbolo a formato seguro para uso como clave/path.

    BTC/USDT → BTC_USDT
    BTC-USDT → BTC_USDT
    btc/usdt → BTC_USDT
    """
    return re.sub(r"[^A-Z0-9]", "_", symbol.upper())


def normalize_ohlcv_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza un DataFrame OHLCV a tipos y orden canónicos.

    - timestamp: datetime64[ns, UTC]
    - open/high/low/close/volume: float64
    - Ordena por timestamp ascendente
    - Resetea el índice
    """
    if df is None or df.empty:
        return df

    df = df.copy()

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    for col in ("open", "high", "low", "close", "volume"):
        if col in df.columns:
            df[col] = df[col].astype("float64")

    if "timestamp" in df.columns:
        df = df.sort_values("timestamp").reset_index(drop=True)

    return df


__all__ = [
    "DataNotFoundError",
    "DataReadError",
    "VersionNotFoundError",
    "safe_symbol",
    "normalize_ohlcv_df",
]
