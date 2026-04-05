from __future__ import annotations

"""
data_platform/ohlcv_utils.py
============================

Utilidades de dominio OHLCV compartidas entre silver, gold y loader.

Separado de core/utils.py porque depende de pandas — core/utils.py
es solo stdlib para no contaminar entornos ligeros.

Importar desde aquí en lugar de redefinir en cada módulo:

    from data_platform.ohlcv_utils import safe_symbol, normalize_ohlcv_df
"""

import pandas as pd


def safe_symbol(symbol: str) -> str:
    """
    Normaliza un símbolo para uso en nombres de archivo y paths.

    "BTC/USDT" → "BTC_USDT"

    Fuente única — no redefinir en silver, gold ni loader.
    """
    return symbol.replace("/", "_")


def normalize_ohlcv_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalización canónica de un DataFrame OHLCV.

    Operaciones (en orden correcto):
    1. sort_values por timestamp  — orden cronológico garantizado
    2. drop_duplicates keep="last" — última escritura gana (last-write-wins)
    3. reset_index                 — índice limpio 0..N

    Comportamiento canónico: keep="last" en todos los consumidores.
    No redefinir en silver, gold ni loader — llamar siempre a esta función.
    """
    return (
        df
        .sort_values("timestamp")
        .drop_duplicates(subset="timestamp", keep="last")
        .reset_index(drop=True)
    )


__all__ = ["safe_symbol", "normalize_ohlcv_df"]


# ==========================================================
# Timeframe constants — fuente de verdad para todo el sistema
# ==========================================================

TIMEFRAME_SECONDS: dict[str, int] = {
    "1m":  60,
    "3m":  180,
    "5m":  300,
    "15m": 900,
    "30m": 1_800,
    "1h":  3_600,
    "2h":  7_200,
    "4h":  14_400,
    "6h":  21_600,
    "8h":  28_800,
    "12h": 43_200,
    "1d":  86_400,
    "1w":  604_800,
}


# ==========================================================
# Data Platform exceptions — compartidas por loaders
# ==========================================================

class DataPlatformError(Exception):
    """Base exception para errores de lectura del Data Lake."""

class DataNotFoundError(DataPlatformError):
    """No existen datos para el símbolo/timeframe/versión solicitado."""

class DataReadError(DataPlatformError):
    """Error al leer o deserializar datos del Data Lake."""

class VersionNotFoundError(DataPlatformError):
    """La versión solicitada no existe en el dataset."""

# Alias para compatibilidad — market_data_loader usaba estos nombres
MarketDataLoaderError = DataPlatformError
