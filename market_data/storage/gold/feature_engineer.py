from __future__ import annotations

import numpy as np
import pandas as pd
from loguru import logger

FEATURE_COLUMNS = [
    "return_1",
    "log_return",
    "volatility_20",
    "high_low_spread",
    "vwap",
]


class FeatureEngineer:
    # Versión semántica del set de features.
    # Incrementar cuando cambien los indicadores o su cálculo — permite
    # detectar en el manifest de Gold qué versión de features produjo cada dataset.
    VERSION = "1.1.0"  # 1.1.0: vwap rolling-20 (tp*vol) en lugar de acumulado desde t=0

    def compute(
        self,
        df:        pd.DataFrame,
        symbol:    str = "",
        timeframe: str = "",
    ) -> pd.DataFrame:
        if df is None or df.empty or len(df) < 2:
            logger.warning(
                "FeatureEngineer: DataFrame vacio o insuficiente | {}/{}",
                symbol, timeframe,
            )
            return df

        df = df.copy().sort_values("timestamp").reset_index(drop=True)

        df["return_1"]        = df["close"].pct_change()
        _safe_close            = df["close"].replace(0, float("nan"))
    df["log_return"]      = np.log(_safe_close / _safe_close.shift(1))
        df["volatility_20"]   = df["log_return"].rolling(20, min_periods=5).std()
        df["high_low_spread"] = (df["high"] - df["low"]) / df["close"].replace(0, np.nan)

        # VWAP rolling 20 periodos — VWAP acumulado desde t=0 no tiene
        # significado financiero en series multi-sesion (produce señal falsa
        # en backtesting con timeframes diarios/semanales).
        # Typical-price weighted rolling es la convención estándar.
        _tp  = (df["high"] + df["low"] + df["close"]) / 3
        _vol = df["volume"].replace(0, np.nan)
        df["vwap"] = (
            (_tp * _vol).rolling(20, min_periods=5).sum()
            / _vol.rolling(20, min_periods=5).sum()
        )

        logger.debug(
            "Features calculados | {}/{} rows={} features={}",
            symbol, timeframe, len(df), FEATURE_COLUMNS,
        )
        return df
