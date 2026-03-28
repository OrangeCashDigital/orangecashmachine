from __future__ import annotations
import numpy as np
import pandas as pd
from loguru import logger

FEATURE_COLUMNS = ["return_1","log_return","volatility_20","high_low_spread","vwap"]

class FeatureEngineer:
    # Versión semántica del set de features.
    # Incrementar cuando cambien los indicadores o su cálculo — permite
    # detectar en el manifest de Gold qué versión de features produjo cada dataset.
    VERSION = "1.0.0"  # return_1, log_return, volatility_20, high_low_spread, vwap

    def compute(self, df: pd.DataFrame, symbol: str = "", timeframe: str = "") -> pd.DataFrame:
        if df is None or df.empty or len(df) < 2:
            logger.warning("FeatureEngineer: DataFrame vacio o insuficiente | {}/{}", symbol, timeframe)
            return df
        df = df.copy().sort_values("timestamp").reset_index(drop=True)
        df["return_1"]       = df["close"].pct_change()
        df["log_return"]     = np.log(df["close"] / df["close"].shift(1))
        df["volatility_20"]  = df["log_return"].rolling(20, min_periods=5).std()
        df["high_low_spread"]= (df["high"] - df["low"]) / df["close"].replace(0, np.nan)
        df["vwap"]           = (df["close"] * df["volume"]).cumsum() / df["volume"].cumsum().replace(0, np.nan)
        logger.debug("Features calculados | {}/{} rows={} features={}", symbol, timeframe, len(df), FEATURE_COLUMNS)
        return df
