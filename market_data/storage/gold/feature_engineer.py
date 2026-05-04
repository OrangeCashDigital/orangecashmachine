"""
market_data/storage/gold/feature_engineer.py
=============================================
DEPRECATED desde v2.0.0 — tombstone.

Por qué fue deprecado
---------------------
1. Capa incorrecta: storage/ no debe computar features (viola SRP).
   Feature engineering pertenece a la capa Gold, no al storage.

2. DRY: duplicaba lógica de gold/transformer.py con VWAP incorrecto
   (acumulado vs rolling) — dos SSOT son cero SSOT.

Migración
---------
Antes:
    from market_data.storage.gold.feature_engineer import FeatureEngineer
    result = FeatureEngineer().compute(df, symbol="BTC/USDT", timeframe="1h")

Ahora:
    from market_data.gold.transformer import GoldTransformer
    result = GoldTransformer.transform(df, symbol="BTC/USDT",
                                       timeframe="1h", exchange="binance")

Eliminación programada: v3.0.0
"""
from __future__ import annotations

import warnings

# Columnas que el caller podría referenciar — re-exportadas para no romper
# código que hace `from feature_engineer import FEATURE_COLUMNS`.
from market_data.gold.transformer import FEATURE_COLUMNS, VERSION as _GT_VERSION  # noqa: F401

_DEPRECATED_MSG = (
    "FeatureEngineer está DEPRECATED desde v2.0.0. "
    "Usar market_data.gold.transformer.GoldTransformer. "
    "Eliminado en v3.0.0."
)


class FeatureEngineer:
    """
    DEPRECATED. Ver docstring del módulo.

    Esta clase es un shim de compatibilidad — delega a GoldTransformer.
    Emite DeprecationWarning en cada llamada a compute().
    """

    VERSION = f"1.1.0-deprecated (→ GoldTransformer {_GT_VERSION})"

    def compute(
        self,
        df:        "pd.DataFrame",
        symbol:    str = "",
        timeframe: str = "",
    ) -> "pd.DataFrame":
        warnings.warn(_DEPRECATED_MSG, DeprecationWarning, stacklevel=2)

        from market_data.gold.transformer import GoldTransformer
        return GoldTransformer.transform(
            df,
            symbol    = symbol,
            timeframe = timeframe,
            exchange  = "unknown",  # FeatureEngineer no tenía exchange — degradación segura
        )
