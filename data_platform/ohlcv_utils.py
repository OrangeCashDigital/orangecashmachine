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
