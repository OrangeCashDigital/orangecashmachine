"""
gold_storage.py
===============

Capa Gold del Data Lakehouse.

Responsabilidad
---------------
Leer datos Silver limpios, calcular features técnicos via FeatureEngineer,
y persistir el dataset enriquecido listo para estrategias/backtesting.

Estructura de partición
-----------------------
gold/features/ohlcv/
  exchange={exchange}/
    symbol={symbol}/
      market_type={market_type}/
        timeframe={timeframe}/
          {symbol}_{tf}_features.parquet
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, List

import pandas as pd
from loguru import logger

from core.utils import silver_ohlcv_root, gold_features_root
from data_platform.ohlcv_utils import safe_symbol
from market_data.batch.storage.feature_engineer import FeatureEngineer
from market_data.batch.storage.silver_storage import SilverStorage



# ==========================================================
# GoldStorage
# ==========================================================

class GoldStorage:
    """
    Construye datasets Gold (Silver + features) para trading/backtesting.

    Uso
    ---
    gold = GoldStorage()
    df   = gold.build(exchange="bybit", symbol="BTC/USDT", market_type="spot", timeframe="1h")
    """

    def __init__(
        self,
        silver_path: Optional[Path] = None,
        gold_path:   Optional[Path] = None,
    ) -> None:
        self._silver   = Path(silver_path) if silver_path else silver_ohlcv_root()
        self._gold     = Path(gold_path)   if gold_path   else gold_features_root()
        self._gold.mkdir(parents=True, exist_ok=True)
        self._engineer = FeatureEngineer()
        logger.info("GoldStorage ready | silver={} gold={}", self._silver, self._gold)

    def build(
        self,
        exchange:    str,
        symbol:      str,
        market_type: str,
        timeframe:   str,
        start:       Optional[pd.Timestamp] = None,
        end:         Optional[pd.Timestamp] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Lee Silver, calcula features y persiste en Gold.

        Parameters
        ----------
        exchange    : e.g. "bybit"
        symbol      : e.g. "BTC/USDT"
        market_type : "spot" | "swap"
        timeframe   : e.g. "1h"
        start / end : filtro de tiempo opcional
        """
        symbol_safe = safe_symbol(symbol)
        silver = SilverStorage(
            base_path=self._silver,
            exchange=exchange,
            market_type=market_type,
        )
        try:
            df = silver.load_ohlcv(
                symbol=symbol,
                timeframe=timeframe,
                start=start,
                end=end,
            )
        except Exception as exc:
            logger.warning(
                "Gold build: Silver load failed | {}/{}/{}/{} err={}",
                exchange, symbol, market_type, timeframe, exc,
            )
            return None

        if df is None or df.empty:
            logger.warning(
                "Gold build: sin datos en Silver | {}/{}/{}/{}",
                exchange, symbol, market_type, timeframe,
            )
            return None


        df = self._engineer.compute(df, symbol=symbol, timeframe=timeframe)

        out_dir = (
            self._gold
            / f"exchange={exchange}"
            / f"symbol={symbol_safe}"
            / f"market_type={market_type}"
            / f"timeframe={timeframe}"
        )
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"{symbol_safe}_{timeframe}_features.parquet"
        df.to_parquet(out_file, index=False)

        logger.info(
            "Gold saved | {}/{}/{}/{} rows={} features={} file={}",
            exchange, symbol, market_type, timeframe,
            len(df), len(df.columns), out_file,
        )
        return df

    def build_all(
        self,
        exchange:    str,
        symbols:     List[str],
        market_type: str,
        timeframes:  List[str],
    ) -> None:
        """Construye Gold para todos los pares/timeframes de un exchange."""
        for symbol in symbols:
            for tf in timeframes:
                try:
                    self.build(
                        exchange=exchange,
                        symbol=symbol,
                        market_type=market_type,
                        timeframe=tf,
                    )
                except Exception as exc:
                    logger.error(
                        "Gold build_all error | {}/{}/{}/{} err={}",
                        exchange, symbol, market_type, tf, exc,
                    )
